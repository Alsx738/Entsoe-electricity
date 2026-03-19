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
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))

if not ENTSOE_TOKEN:
    raise ValueError("ENTSOE_TOKEN environment variable is not set.")

# --- CLOUD STORAGE UTILITIES ---

def get_gcs_client() -> storage.Client:
    """
    Initializes a GCS client using Service Account JSON from env 
    or falls back to Application Default Credentials.
    """
    gcp_json = os.getenv("GCP_SA_CREDENTIALS_JSON")
    if gcp_json:
        creds_info = json.loads(gcp_json)
        credentials = service_account.Credentials.from_service_account_info(creds_info)
        return storage.Client(credentials=credentials)
    return storage.Client()

def upload_to_storage(content: str, blob_path: str):
    """
    Uploads content to GCS. In case of failure, saves the file 
    to a local directory as a fallback mechanism.
    """
    try:
        client = get_gcs_client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(blob_path)
        blob.upload_from_string(content, content_type="application/xml")
        print(f"[INFO] Successfully uploaded to GCS: {blob_path}")
    except Exception as e:
        print(f"[ERROR] GCS upload failed for {blob_path}: {e}")
        local_path = f"data_output/{blob_path}"
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"[FALLBACK] Data saved locally at: {local_path}")

# --- HELPER FUNCTIONS ---

def load_config(filename: str):
    """Loads a JSON configuration file from the script's directory."""
    path = os.path.join(os.path.dirname(__file__), filename)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def resolve_daily_window(start_date: str | None, end_date: str | None):
    """
    Calculates the time range for API requests and storage period key.
    Daily runs should pass start=end for a single target day.
    """
    if start_date and end_date:
        start_day = pendulum.parse(start_date).start_of("day")
        end_day = pendulum.parse(end_date).start_of("day")
    else:
        # Backward-compatible default: use previous UTC day.
        default_day = pendulum.now("UTC").subtract(days=1).start_of("day")
        start_day = default_day
        end_day = default_day

    period_start = start_day.format("YYYYMMDDHHmm")
    period_end = end_day.end_of("day").format("YYYYMMDDHHmm")
    period_key = f"{start_day.format('YYYYMMDD')}-{end_day.format('YYYYMMDD')}"
    return period_start, period_end, period_key

# --- ENTSO-E EXTRACTION LOGIC ---

def fetch_and_save(params: dict, path: str, label: str):
    """
    Executes a single GET request to the ENTSO-E API and 
    triggers the storage upload if the response is valid.
    """
    params["securityToken"] = ENTSOE_TOKEN
    try:
        response = requests.get(API_URL, params=params, timeout=60)
        if response.status_code == 200:
            upload_to_storage(response.text, path)
        elif response.status_code != 404:
            print(f"[WARNING] {label} returned HTTP {response.status_code}")
    except Exception as e:
        print(f"[ERROR] Request failed for {label}: {e}")

def process_country_data(country: dict, p_start: str, p_end: str, period_key: str):
    """Handles Actual Load and Generation data extraction for a country."""
    code, domain = country["code"], country["domain"]
    base_path = f"country={code}/period={period_key}"
    
    # Task: Actual Load (A65)
    fetch_and_save(
        {"documentType": "A65", "processType": "A16", "outBiddingZone_Domain": domain, "periodStart": p_start, "periodEnd": p_end},
        f"raw/entsoe/daily/actual_load/{base_path}/load.xml",
        f"Load-{code}"
    )
    
    # Task: Aggregated Generation (A75)
    fetch_and_save(
        {"documentType": "A75", "processType": "A16", "in_Domain": domain, "periodStart": p_start, "periodEnd": p_end},
        f"raw/entsoe/daily/generation/{base_path}/generation.xml",
        f"Generation-{code}"
    )

def process_border_flow(base_code: str, base_domain: str, neighbor: dict, p_start: str, p_end: str, period_key: str):
    """Handles Physical Flow (Import/Export) extraction for a specific border."""
    n_code, n_domain = neighbor["code"], neighbor["domain"]
    base_path = f"country={base_code}/direction={{direction}}/border={n_code}/period={period_key}"

    # Direction: Import (Neighbor -> Base)
    fetch_and_save(
        {"documentType": "A11", "in_Domain": base_domain, "out_Domain": n_domain, "periodStart": p_start, "periodEnd": p_end},
        f"raw/entsoe/daily/physical_flows/{base_path.format(direction='import')}/flow.xml",
        f"Flow-Import-{base_code}-{n_code}"
    )
    
    # Direction: Export (Base -> Neighbor)
    fetch_and_save(
        {"documentType": "A11", "in_Domain": n_domain, "out_Domain": base_domain, "periodStart": p_start, "periodEnd": p_end},
        f"raw/entsoe/daily/physical_flows/{base_path.format(direction='export')}/flow.xml",
        f"Flow-Export-{base_code}-{n_code}"
    )

# --- MAIN ---

def main():
    """Main entry point for the Kestra task."""
    parser = argparse.ArgumentParser(description="ENTSO-E High-Concurrency Ingestion Script")
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--date", type=str, help="Deprecated: execution date (YYYY-MM-DD)")
    parser.add_argument("--country", type=str, default="ALL", help="ISO Country code or 'ALL'")
    args = parser.parse_args()

    # Compatibility bridge: if only --date is provided, treat it as start=end of previous day logic.
    start_arg = args.start
    end_arg = args.end
    if (not start_arg or not end_arg) and args.date:
        previous_day = pendulum.parse(args.date).subtract(days=1).to_date_string()
        start_arg = previous_day
        end_arg = previous_day

    period_start, period_end, period_key = resolve_daily_window(start_arg, end_arg)
    target_country = args.country.upper()

    print(f"[START] Ingestion started for period: {period_key}")

    countries = load_config("countries.json")
    borders = load_config("borders.json")

    # Apply country filtering
    if target_country != "ALL":
        countries = [c for c in countries if c["code"] == target_country]
        borders = {target_country: borders[target_country]} if target_country in borders else {}

    # Multi-threaded execution for API requests and I/O
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []

        # Schedule Load and Generation tasks
        for country in countries:
            futures.append(executor.submit(process_country_data, country, period_start, period_end, period_key))

        # Schedule Cross-Border Flow tasks
        for base_code, info in borders.items():
            base_domain = info["domain"]
            for neighbor in info.get("borders", []):
                futures.append(executor.submit(process_border_flow, base_code, base_domain, neighbor, period_start, period_end, period_key))

        # Block until all threads complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"[CRITICAL] Worker thread raised an exception: {e}")

    print("[COMPLETED] Ingestion job finished successfully.")

if __name__ == "__main__":
    main()