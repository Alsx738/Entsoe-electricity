# ScriptForSpark — PySpark Processing Jobs

These scripts parse raw ENTSO-E XML files from GCS and produce flat Parquet datasets, also on GCS. They are submitted to **Dataproc Serverless** by Kestra.

## Common Pattern

All scripts follow the same structure:
1. Read XML files from `gs://<bucket>/raw/entsoe/...`
2. Parse `TimeSeries` → `Period` → `Point` hierarchy
3. Compute a **deterministic SHA-256 id** per record
4. Append to a Parquet dataset, skipping already-processed ids

### Timestamp Calculation
Each XML `Point` represents one time interval. The timestamp is:
```
timestamp = period_start + (point.position - 1) × resolution_minutes
```

### Deduplication
Before writing, only partition subdirectories that the new batch targets are scanned for existing ids (not the entire dataset root), keeping the operation proportional to the new data rather than total history:
```python
targeted_paths = [f"{output_path}/{col}={val}" for each partition value]
existing_ids = spark.read.parquet(*targeted_paths).select("id").distinct()
df = df.join(existing_ids, on="id", how="left_anti")
```

### Output Partitioning
All datasets are partitioned by `country` and `year`:
```
gs://<bucket>/processed/<type>/country=IT/year=2024/*.parquet
```

---

## Scripts

### `entsoe_master_daily.py`
Processes one day of data submitted by Kestra's daily flow. Runs four document types concurrently using `ThreadPoolExecutor`:

| Data type | Source path | Output path |
|---|---|---|
| Actual Load | `raw/.../actual_load/` | `processed/load/` |
| Generation | `raw/.../generation/` | `processed/generation/` |
| Physical Flows | `raw/.../physical_flows/` | `processed/physical_flows/` |
| Prices | `raw/.../prices/` | `processed/prices/` |

---

### `entsoe_master_historical.py`
Same as the daily script but designed for date ranges (backfill). Also processes **installed capacity** data, which the daily script does not handle.

Accepts `gs://<bucket>`, `start_date`, `end_date` as positional arguments.

---

### `entsoe_installed_capacity.py`
Dedicated script for annual installed capacity data. Capacity is reported once per year per country per technology — not hourly — so it has a separate simpler parsing path.

Output: `processed/installed_capacity/country=<code>/year=<YYYY>/`

---

### `entsoe_compact_load.py`
Spark's incremental append mode creates many small Parquet files over time. This script compacts them into a smaller number of larger files per `country/year` partition, improving downstream read performance.

It overwrites in-place — no data is created or deleted, only file layout changes.

Run periodically (e.g. once a month via the Kestra `monthly_compaction` flow).
