# ENTSO-E Data Engineering Pipeline

End-to-end data pipeline that ingests electricity data from the [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/), processes it with Apache Spark, and transforms it into analytics-ready tables in BigQuery.

**Data collected:** hourly generation by technology, electricity demand (load), Day-Ahead Market prices, cross-border physical flows, and annual installed capacity — for all European countries.

---

## Architecture

```
ENTSO-E REST API
      │
      ▼
Docker containers (Python ingestion)
      │  raw XML files
      ▼
Google Cloud Storage  ──────────────────────────────────────┐
      │  raw/entsoe/{daily,historical}/.../*.xml            │
      ▼                                                     │
Dataproc Serverless (PySpark)                               │
      │  parses XML → Parquet, partitioned by country/year  │
      ▼                                                     │
Google Cloud Storage                                        │
      │  processed/{load,generation,prices,...}/*.parquet   │
      ▼                                             (BigQuery External Tables)
BigQuery ◄──────────────────────────────────────────────────┘
      │
      ▼
dbt (BigQuery adapter)
      │  staging → intermediate → marts
      ▼
Analytics tables (fct_*, mart_*)
```

**Orchestration:** Kestra (self-hosted via Docker Compose)  
**Infrastructure provisioning:** Terraform

---

## Data Flow: from XML to Parquet

ENTSO-E delivers data as XML files following the IEC CIM standard. Each file contains a set of `TimeSeries` elements with `Period` blocks and `Point` entries (one per time interval).

The PySpark scripts in `ScriptForSpark/` parse this structure and produce flat Parquet files:

| Source XML field | Parquet output |
|---|---|
| `country` (from file path) | `country` partition column |
| `timeInterval.start` + `resolution` + `Point.position` | computed `timestamp` |
| `Point.quantity` | `mw` or `installed_capacity_mw` |
| `MktPSRType.psrType` | `psrType` (technology code, e.g. `B16` for Solar) |
| `price.amount` | `price_eur_mwh` |

A deterministic SHA-256 `id` is computed per row (e.g. `SHA256(country|timestamp|psrType)`) to enable idempotent re-runs.

---

## dbt Transformation Layers

| Layer | Materialization | Purpose |
|---|---|---|
| `staging` | view | Type casting, deduplication, PSR code → name join |
| `intermediate` | view | Time dimensions, quality flags, capacity utilisation |
| `marts` | table | Daily aggregations (facts, dimensions, final marts) |

Key seeds: `psr_type_mapping.csv` (PSR code → technology name), `country_names.csv` (ISO-2 → full name + region).

Final mart tables `mart_country_energy_balance_daily` and `mart_country_price_load_daily` are the primary entry points for dashboards.

---

## Project Structure

```
.
├── Dockerfile               # Ingestion image (Python + uv)
├── Dockerfile.dbt           # dbt runner image (planned for automated post-daily runs)
├── pyproject.toml
├── src/                     # Python ingestion scripts
│   ├── daily_ingestion.py
│   ├── historical_ingestion.py
│   ├── HistoricalPrice.py
│   ├── countries.json
│   └── borders.json
├── ScriptForSpark/          # PySpark jobs (submitted to Dataproc Serverless)
│   ├── entsoe_master_daily.py
│   ├── entsoe_master_historical.py
│   ├── entsoe_installed_capacity.py
│   └── entsoe_compact_load.py
├── dbt/                     # dbt project (BigQuery adapter)
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── macros/
│   ├── seeds/
│   └── dbt_project.yml
├── kestra/                  # Kestra flow definitions and Docker Compose
│   ├── daily.yml
│   ├── historical.yml
│   ├── installed_capacity.yml
│   ├── monthly_compaction.yml
│   └── docker-compose.yml
└── terraform/               # GCP infrastructure (GCS, BigQuery, Artifact Registry, VPC)
```

### Two Dockerfiles

- **`Dockerfile`** — builds the Python ingestion image. Used by Kestra's `daily` and `historical` flows to download XML files from the ENTSO-E API and upload them to GCS. The image is pushed to **Google Artifact Registry** and pulled at runtime by Kestra.

- **`Dockerfile.dbt`** — builds a self-contained dbt runner image. Planned for automated dbt execution as a Kestra task immediately after the daily Spark job completes, keeping the transformation layer in sync without manual intervention. Also pushed to Artifact Registry.

---

## Reproduction

### Prerequisites

- Docker Desktop
- Terraform
- A Google Cloud project with billing enabled
- An ENTSO-E API token ([request here](https://transparency.entsoe.eu/))

---

### 1 — GCP Service Account

Create a service account with the following roles and download its JSON key to `terraform/credentials.json` (gitignored):

- `Storage Admin`
- `BigQuery Admin`
- `Artifact Registry Admin`
- `Service Usage Admin`
- `Compute Network Admin`
- `Dataproc Editor`

---

### 2 — Provision Infrastructure

```bash
cd terraform
```

Create `terraform.tfvars` (gitignored):

```hcl
project_id    = "your-gcp-project-id"
region        = "europe-west4"
zone          = "europe-west4-a"
bucket_name   = "your-unique-bucket-name"
bq_dataset_id = "entsoe_analytics_dev"
```

```bash
terraform init
terraform apply
```

Creates: GCS bucket, BigQuery dataset, Artifact Registry repository, VPC subnet.

---

### 3 — Build and Push Docker Images

```bash
gcloud auth configure-docker europe-west4-docker.pkg.dev

# Ingestion image
docker build -t europe-west4-docker.pkg.dev/<PROJECT_ID>/entsoe/ingest:latest .
docker push europe-west4-docker.pkg.dev/<PROJECT_ID>/entsoe/ingest:latest

# dbt image (optional, for automated runs)
docker build -f Dockerfile.dbt -t europe-west4-docker.pkg.dev/<PROJECT_ID>/entsoe/dbt:latest .
docker push europe-west4-docker.pkg.dev/<PROJECT_ID>/entsoe/dbt:latest
```

---

### 4 — Upload Spark Scripts to GCS

```bash
gsutil cp ScriptForSpark/*.py gs://<BUCKET_NAME>/scripts/
```

---

### 5 — Configure Kestra Secrets

Kestra reads secrets from base64-encoded environment variables prefixed with `SECRET_`.

```powershell
# PowerShell
$token = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes("your-entsoe-token"))
"SECRET_ENTSOE_TOKEN=$token" | Add-Content kestra/.env_encoded

$json = Get-Content -Raw terraform/credentials.json
$encoded = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($json))
"SECRET_GCP_CREDS_JSON=$encoded" | Add-Content kestra/.env_encoded
```

---

### 6 — Start Kestra

```bash
cd kestra
docker compose up -d
```

Kestra UI: [http://localhost:8080](http://localhost:8080)

Upload the flows from `kestra/*.yml` via **Flows → Create**.

The daily pipeline runs automatically at **03:00 Europe/Rome**. For historical backfill, trigger `entsoe-historical-bootstrap` manually with a start/end date range.

---

### 7 — Run dbt

```bash
cd dbt

# Load seed reference tables
uv run dbt seed

# Run all models (first time or after schema changes)
uv run dbt run --full-refresh

# Run tests
uv run dbt test

# Generate documentation
uv run dbt docs generate
```

---

## Known Issue: Kestra + Dataproc LRO Polling

On long Spark jobs (typically historical backfills), Kestra's LRO (Long Running Operation) poller may time out and report the task as `FAILED`, while Dataproc continues and completes successfully. The processed Parquet files on GCS are the source of truth — if they exist and are complete, the pipeline succeeded regardless of the Kestra task state. This is a known plugin limitation unrelated to data correctness.
