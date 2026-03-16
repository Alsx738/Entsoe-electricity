# ENTSO-E Data Engineering Pipeline

A daily data ingestion pipeline that fetches electricity data (Load, Generation, Cross-Border Physical Flows) from the [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/) for all European countries and stores raw XML files in Google Cloud Storage.

Orchestrated by **Kestra**, containerized with **Docker**, and provisioned with **Terraform**.

---

## Architecture

```
ENTSO-E API → Python script (Docker) → Google Cloud Storage (raw XML)
                       ↑
                   Kestra (scheduler & orchestrator)
```

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.3.0
- A [Google Cloud](https://console.cloud.google.com/) project
- An [ENTSO-E API token](https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html#_authentication_and_authorisation)

---

## Setup & Reproduction

### Step 1 — Clone the repository

```bash
git clone https://github.com/Alsx738/Entsoe-electricity.git
cd Entsoe-electricity
```

---

### Step 2 — Create a GCP Service Account

In the Google Cloud Console:

1. Go to **IAM & Admin → Service Accounts** and create a new service account (e.g. `terraform-runner`).
2. Assign the following roles:
   - `Storage Admin` — to create and write to GCS buckets
   - `BigQuery Admin` — to create BigQuery datasets
   - `Artifact Registry Admin` — to push and pull Docker images
   - `Service Usage Admin` — to enable APIs via Terraform
3. Create and download a **JSON key** for the service account.
4. Place the JSON key at `terraform/credentials.json` (this file is gitignored).

---

### Step 3 — Provision GCP Infrastructure with Terraform

```bash
cd terraform
```

Create a `terraform.tfvars` file (also gitignored):

```hcl
project_id    = "your-gcp-project-id"
region        = "europe-west1"
zone          = "europe-west1-d"
bucket_name   = "your-unique-bucket-name"
bq_dataset_id = "entsoe_data"
```

Then apply:

```bash
terraform init
terraform plan
terraform apply
```

This creates:
- A **GCS bucket** (raw data lake)
- A **BigQuery dataset** (data warehouse, for future use)
- An **Artifact Registry** Docker repository

---

### Step 4 — Build and Push the Docker Image

Authenticate Docker with Google Artifact Registry first:

```bash
gcloud auth configure-docker europe-west1-docker.pkg.dev
```

Then build and push:

```bash
docker build -t europe-west1-docker.pkg.dev/<PROJECT_ID>/entsoe/ingest:latest .
docker push europe-west1-docker.pkg.dev/<PROJECT_ID>/entsoe/ingest:latest
```

---

### Step 5 — Configure Kestra Secrets

Kestra reads secrets from environment variables prefixed with `SECRET_` and base64-encoded.

Create `kestra/.env_encoded` (gitignored) with your secrets:

**On Linux/macOS:**
```bash
echo "SECRET_ENTSOE_TOKEN=$(echo -n 'your-entsoe-token' | base64)" >> kestra/.env_encoded
echo "SECRET_GCP_CREDS_JSON=$(base64 -w 0 terraform/credentials.json)" >> kestra/.env_encoded
```

**On Windows (PowerShell):**
```powershell
# ENTSO-E token
$token = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes("your-entsoe-token"))
"SECRET_ENTSOE_TOKEN=$token" | Add-Content kestra/.env_encoded

# GCP credentials JSON
$json = Get-Content -Raw terraform/credentials.json
$encoded = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($json))
"SECRET_GCP_CREDS_JSON=$encoded" | Add-Content kestra/.env_encoded
```

---

### Step 6 — Start Kestra

1. Create a local `.env` file for database credentials (gitignored):
    An example is provided in the `kestra` directory, you can edit and rename it to `.env`.

2. Start the containers:
   ```bash
   cd kestra
   docker compose up -d
   ```

Kestra UI will be available at [http://localhost:8080](http://localhost:8080).

---

### Step 7 — Deploy the Flow

1. Open [http://localhost:8080](http://localhost:8080)
2. Go to **Flows** and click **Create**
3. Paste the contents of `kestra/entsoe_ingestion_flow.yml`
4. Save the flow

The pipeline will run automatically every night at **03:00 (Europe/Rome)**. You can also trigger it manually by clicking **Execute** and optionally specifying a custom date.

---

## Project Structure

```
.
├── Dockerfile                          # Container definition for the ingestion script
├── pyproject.toml                      # Python dependencies (managed by uv)
├── src/
│   ├── ingest_entsoe.py                # Main ingestion script
│   ├── countries.json                  # European country codes
│   └── borders.json                    # Cross-border definitions
├── kestra/
│   ├── docker-compose.yml              # Kestra + PostgreSQL local setup
│   ├── entsoe_ingestion_flow.yml       # Kestra flow definition
│   ├── .env.example                    # Template for Postgres credentials
│   ├── .env_encoded.example            # Template for Kestra secrets
│   ├── .env                            # (gitignored) Local Postgres credentials
│   └── .env_encoded                    # (gitignored) Base64-encoded Kestra secrets
└── terraform/
    ├── main.tf                         # GCP resources (GCS, BigQuery, Artifact Registry)
    ├── variables.tf                    # Input variable definitions
    ├── terraform.tfvars.example        # (gitignored) Your variable values example
    ├── terraform.tfvars                # (gitignored) Your variable values
    └── credentials.json                # (gitignored) GCP service account key
```
