import os
import json
import argparse
import pendulum
import requests
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURATION ---
load_dotenv()

ENTSOE_TOKEN = os.getenv("ENTSOE_TOKEN")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "entsoe-data-lake")
API_URL = "https://web-api.tp.entsoe.eu/api"
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))

if not ENTSOE_TOKEN:
    raise ValueError("ENTSOE_TOKEN environment variable is not set.")

# --- CLOUD STORAGE UTILITIES ---

def get_gcs_client() -> storage.Client:
    """Initializes GCS client with Service Account or Default credentials."""
    gcp_json = os.getenv("GCP_SA_CREDENTIALS_JSON")
    if gcp_json:
        creds_info = json.loads(gcp_json)
        credentials = service_account.Credentials.from_service_account_info(creds_info)
        return storage.Client(credentials=credentials)
    return storage.Client()

def upload_to_storage(content: str, blob_path: str):
    """Uploads XML content to GCS with local fallback on failure."""
    try:
        client = get_gcs_client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(blob_path)
        blob.upload_from_string(content, content_type="application/xml")
        print(f"[INFO] Successfully uploaded: {blob_path}")
    except Exception as e:
        print(f"[ERROR] GCS upload failed for {blob_path}: {e}")
        local_path = f"data_output/{blob_path}"
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"[FALLBACK] Data saved locally: {local_path}")

# --- HELPER FUNCTIONS ---

def load_config(filename: str):
    """Loads JSON configuration from the same directory as the script."""
    path = os.path.join(os.path.dirname(__file__), filename)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def split_into_yearly_ranges(start_date: str, end_date: str):
    """Splits a UTC date range into year chunks using [start, end) semantics."""
    start = pendulum.parse(start_date).in_tz("UTC").start_of("day")
    end = pendulum.parse(end_date).in_tz("UTC").start_of("day")
    if end < start:
        raise ValueError("Invalid date range: end date must be on or after start date.")

    end_exclusive = end.add(days=1)
    ranges = []
    current = start
    while current < end_exclusive:
        next_year_start = current.start_of("year").add(years=1)
        chunk_end_exclusive = min(next_year_start, end_exclusive)
        ranges.append((current, chunk_end_exclusive))
        current = chunk_end_exclusive
    return ranges


def build_period_key(start_date: str, end_date: str) -> str:
    """Builds a stable key used in raw storage paths."""
    start = pendulum.parse(start_date).in_tz("UTC").format("YYYYMMDD")
    end = pendulum.parse(end_date).in_tz("UTC").format("YYYYMMDD")
    return f"{start}-{end}"

# --- EXTRACTION ENGINE ---

def fetch_and_process(params: dict, blob_path: str, label: str):
    """Performs the API call and triggers storage upload."""
    params["securityToken"] = ENTSOE_TOKEN
    try:
        # Increased timeout for large yearly XML files
        response = requests.get(API_URL, params=params, timeout=120)
        if response.status_code == 200:
            upload_to_storage(response.text, blob_path)
        else:
            print(f"[WARNING] {label} - API returned status {response.status_code}")
    except Exception as e:
        print(f"[ERROR] {label} - Request failed: {e}")

def process_historical_country(country: dict, start: pendulum.DateTime, end_exclusive: pendulum.DateTime, period_key: str):
    """Handles Load and Generation extraction for a specific country and year."""
    code, domain = country["code"], country["domain"]
    year = start.year
    p_start = start.format("YYYYMMDDHHmm")
    p_end = end_exclusive.format("YYYYMMDDHHmm")

    # Actual Load (A65)
    fetch_and_process(
        {"documentType": "A65", "processType": "A16", "outBiddingZone_Domain": domain, "periodStart": p_start, "periodEnd": p_end},
        f"raw/entsoe/historical/actual_load/country={code}/period={period_key}/chunk_year={year}/load.xml",
        f"Load-{code}-{year}"
    )

    # Aggregated Generation (A75)
    fetch_and_process(
        {"documentType": "A75", "processType": "A16", "in_Domain": domain, "periodStart": p_start, "periodEnd": p_end},
        f"raw/entsoe/historical/generation/country={code}/period={period_key}/chunk_year={year}/generation.xml",
        f"Generation-{code}-{year}"
    )

def process_historical_border(base_code: str, base_domain: str, neighbor: dict, start: pendulum.DateTime, end_exclusive: pendulum.DateTime, period_key: str):
    """Handles Physical Flow extraction (Import/Export) for a specific border and year."""
    n_code, n_domain = neighbor["code"], neighbor["domain"]
    year = start.year
    p_start = start.format("YYYYMMDDHHmm")
    p_end = end_exclusive.format("YYYYMMDDHHmm")

    # Flow: Import
    fetch_and_process(
        {"documentType": "A11", "in_Domain": base_domain, "out_Domain": n_domain, "periodStart": p_start, "periodEnd": p_end},
        f"raw/entsoe/historical/physical_flows/country={base_code}/direction=import/border={n_code}/period={period_key}/chunk_year={year}/flow.xml",
        f"Flow-Import-{base_code}-{n_code}-{year}"
    )

    # Flow: Export
    fetch_and_process(
        {"documentType": "A11", "in_Domain": n_domain, "out_Domain": base_domain, "periodStart": p_start, "periodEnd": p_end},
        f"raw/entsoe/historical/physical_flows/country={base_code}/direction=export/border={n_code}/period={period_key}/chunk_year={year}/flow.xml",
        f"Flow-Export-{base_code}-{n_code}-{year}"
    )

# --- MAIN EXECUTION ---

def main():
    parser = argparse.ArgumentParser(description="ENTSO-E Historical Backfill - Yearly Chunking")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--country", default="ALL", help="Country ISO code or 'ALL'")
    args = parser.parse_args()

    yearly_ranges = split_into_yearly_ranges(args.start, args.end)
    period_key = build_period_key(args.start, args.end)
    target_country = args.country.upper()

    countries = load_config("countries.json")
    borders_config = load_config("borders.json")

    if target_country != "ALL":
        countries = [c for c in countries if c["code"] == target_country]
        borders_config = {target_country: borders_config[target_country]} if target_country in borders_config else {}

    print(f"[START] Historical Backfill from {args.start} to {args.end}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        
        for start_dt, end_dt in yearly_ranges:
            # 1. Schedule Country tasks (Load & Generation)
            for country in countries:
                futures.append(executor.submit(process_historical_country, country, start_dt, end_dt, period_key))
            
            # 2. Schedule Border tasks (Physical Flows)
            for base_code, info in borders_config.items():
                for neighbor in info.get("borders", []):
                    futures.append(executor.submit(process_historical_border, base_code, info["domain"], neighbor, start_dt, end_dt, period_key))

        # Wait for completion
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"[CRITICAL] Worker exception: {e}")

    print("[COMPLETED] Historical backfill job finished.")

if __name__ == "__main__":
    main()