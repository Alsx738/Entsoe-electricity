# src — Python Ingestion Scripts

These scripts download raw XML data from the ENTSO-E REST API and upload it to Google Cloud Storage. They run inside the Docker ingestion container, orchestrated by Kestra, and are managed with **[uv](https://docs.astral.sh/uv/)** for dependency management.

To run locally:
```bash
uv run python src/daily_ingestion.py --start 2024-01-15
```

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `ENTSOE_TOKEN` | ✅ | ENTSO-E API security token |
| `GCS_BUCKET_NAME` | ✅ | Target GCS bucket |
| `GCP_SA_CREDENTIALS_JSON` | optional | Service account JSON (falls back to ADC) |
| `MAX_WORKERS` | optional | Thread pool size (default: 10) |

---

## Files

### `daily_ingestion.py`
Downloads data for **exactly one day** (default: yesterday UTC). Designed for the scheduled daily Kestra flow.

Fetches three document types per country in parallel (`ThreadPoolExecutor`):
- **A65** — Actual Load
- **A75** — Aggregated Generation per type
- **A11** — Physical cross-border flows (import + export per border pair)

Output GCS path:
```
raw/entsoe/daily/<type>/country=<code>/period=<YYYYMMDD>/
```

Arguments: `--start <YYYY-MM-DD>`, `--country <code|ALL>`

---

### `historical_ingestion.py`
Downloads Load, Generation, and Flow data across a **multi-year date range**. Because the ENTSO-E API has a maximum window per request (~1 year for some endpoints), the script automatically splits the range into yearly chunks and iterates through them, one year at a time. Used for the initial backfill and for gap-filling.

Arguments: `--start <YYYY-MM-DD>`, `--end <YYYY-MM-DD>`, `--country <code|ALL>`

---

### `dailyPrice.py`
Downloads Day-Ahead Market prices (document type **A44**) for the current day. Prices are split by bidding zone and stored under:
```
raw/entsoe/daily/prices/country=<code>/period=<YYYYMMDD>/
```

---

### `HistoricalPrice.py`
Same as `dailyPrice.py` but iterates over a configurable date range with year-by-year splitting. Reads bidding zone mappings from `European Price Zones Mapping (2017-2026).json` to handle zones that changed over time.

---

### `installed_capacity_ingestion.py`
Downloads annual installed generation capacity (document type **A68**) for each country and year. Triggered automatically **twice a year** (1 January and 1 July) by the Kestra `installed_capacity` flow, or manually on demand.

---

## Configuration Files

| File | Purpose |
|---|---|
| `countries.json` | ENTSO-E country codes and their bidding zone EIC domains |
| `borders.json` | Border pairs per country for cross-border flow requests |
| `European Price Zones Mapping (2017-2026).json` | Historical bidding zone EIC codes by year |

## GCS Upload Fallback

If a GCS upload fails, the script saves the XML file locally under `data_output/` rather than silently dropping the data.
