# kestra — Orchestration Flows

Kestra orchestrates the entire pipeline. All flows are defined as YAML files in this directory.

## Starting Kestra

```bash
docker compose up -d
```

Kestra UI will be available at [http://localhost:8080](http://localhost:8080).

## Deploying Flows

Flows are not automatically loaded — you need to upload each YAML file manually:

1. Open the Kestra UI at [http://localhost:8080](http://localhost:8080)
2. Go to **Flows** → **Create**
3. Paste the contents of each `.yml` file and save

Repeat for each flow you want to activate.

## Configuration Files

| File | Purpose |
|---|---|
| `.env.example` | Template for Kestra database and basic auth credentials — copy to `.env` |
| `.env_encoded.example` | Template for base64-encoded API secrets — copy to `.env_encoded` |

See the main README for instructions on how to base64-encode secrets.

---

## Flows

### `daily.yml` — `entsoe-daily-ingestion`
**Trigger:** scheduled at 03:00 Europe/Rome every day.

**Steps:**
1. `ingest_daily_data` — Docker task: runs `daily_ingestion.py` to download Load, Generation, and Flow XML for yesterday
2. `ingest_daily_prices` — Docker task: runs `dailyPrice.py` to download Day-Ahead prices
3. `process_with_spark` — Dataproc Serverless: runs `entsoe_master_daily.py` to parse XML → Parquet
4. `notify_on_failure` — Slack alert if ingestion Docker task fails

**Dynamic Date Logic:**
Kestra automatically calculates the target ingestion date as the previous day (`Trigger Date - 1 day`). If you run the flow manually supplying an optional `custom_date`, Kestra will target `custom_date - 1 day`.

---

### `historical.yml` — `entsoe-historical-bootstrap`
**Trigger:** manual only (used for initial backfill or gap-filling).

**Inputs:** `start_date`, `end_date`, `country` (default: ALL)

**Steps:**
1. `ingest_historical_data` — Docker task: runs `historical_ingestion.py` for the date range
2. `ingest_historical_prices` — Docker task: runs `HistoricalPrice.py`
3. `process_with_spark` — Dataproc Serverless: runs `entsoe_master_historical.py`
4. `notify_on_failure` — Slack alert on ingestion failure

> **Note:** On long Spark jobs (typically > 15 min), Kestra's LRO poller may time out and mark the Spark task as `FAILED` even though Dataproc completes successfully. Check GCS output files as the source of truth.

---

### `installed_capacity.yml` — `entsoe-installed-capacity`
**Trigger:** scheduled twice a year — **1 January and 1 July at 04:00 Europe/Rome**. Can also be triggered manually.

Runs `installed_capacity_ingestion.py` (Docker) to download capacity XML, then `entsoe_installed_capacity.py` (Dataproc) to parse it into Parquet.

---

### `monthly_compaction.yml` — `entsoe-monthly-compaction`
**Trigger:** scheduled on the 1st of each month.

Runs `entsoe_compact_load.py` on Dataproc to merge small Parquet files into larger ones, reducing file count and improving BigQuery scan performance.

---

### `dbt_daily.yml` — `entsoe-dbt-daily`
**Trigger:** scheduled at 03:30 Europe/Rome every day (30 minutes after the `daily.yml` ingestion flow).

**Steps:**
1. `run_dbt_marts` — Docker task: pulls the latest `dbt:latest` image, safely injects GCP credentials bypassing Docker entrypoint caveats, runs `dbt seed` to populate lookup tables, and executes `dbt run` for all mart models (`+path:models/marts`).

**Dynamic Date Logic:**
This flow parameterizes dbt's incremental models to strictly align with the `daily.yml` ingestion schedule. 
It calculates a target `execution_date` (Trigger Date - 1 day, or Custom Date - 1 day) and passes it to dbt via `--vars`. A customized dbt macro then forces the incremental models to process exactly `execution_date - 1` and `execution_date` (a 2-day rolling window). This ensures dbt only processes the newly downloaded data and gracefully heals any late-arriving data from the day before, without requiring full dataset scans.
