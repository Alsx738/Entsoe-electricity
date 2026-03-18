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

def get_execution_window(date_str: str | None):
    """
    Calculates the time range for the API request.
    Defaults to the full day preceding the execution date.
    """
    base_date = pendulum.parse(date_str) if date_str else pendulum.now("UTC")
    target_day = base_date.subtract(days=1).start_of("day")
    
    start = target_day.format("YYYYMMDDHHmm")
    end = target_day.end_of("day").format("YYYYMMDDHHmm")
    
    return start, end, target_day

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

def process_country_data(country: dict, p_start: str, p_end: str, day: pendulum.DateTime):
    """Handles Actual Load and Generation data extraction for a country."""
    code, domain = country["code"], country["domain"]
    date_path = f"year={day.year}/month={day.month:02}/day={day.day:02}"
    
    # Task: Actual Load (A65)
    fetch_and_save(
        {"documentType": "A65", "processType": "A16", "outBiddingZone_Domain": domain, "periodStart": p_start, "periodEnd": p_end},
        f"raw/entsoe/actual_load/country={code}/{date_path}/load.xml",
        f"Load-{code}"
    )
    
    # Task: Aggregated Generation (A75)
    fetch_and_save(
        {"documentType": "A75", "processType": "A16", "in_Domain": domain, "periodStart": p_start, "periodEnd": p_end},
        f"raw/entsoe/generation/country={code}/{date_path}/generation.xml",
        f"Generation-{code}"
    )

def process_border_flow(base_code: str, base_domain: str, neighbor: dict, p_start: str, p_end: str, day: pendulum.DateTime):
    """Handles Physical Flow (Import/Export) extraction for a specific border."""
    n_code, n_domain = neighbor["code"], neighbor["domain"]
    date_path = f"year={day.year}/month={day.month:02}/day={day.day:02}"

    # Direction: Import (Neighbor -> Base)
    fetch_and_save(
        {"documentType": "A11", "in_Domain": base_domain, "out_Domain": n_domain, "periodStart": p_start, "periodEnd": p_end},
        f"raw/entsoe/physical_flows/country={base_code}/direction=import/border={n_code}/{date_path}/flow.xml",
        f"Flow-Import-{base_code}-{n_code}"
    )
    
    # Direction: Export (Base -> Neighbor)
    fetch_and_save(
        {"documentType": "A11", "in_Domain": n_domain, "out_Domain": base_domain, "periodStart": p_start, "periodEnd": p_end},
        f"raw/entsoe/physical_flows/country={base_code}/direction=export/border={n_code}/{date_path}/flow.xml",
        f"Flow-Export-{base_code}-{n_code}"
    )

# --- MAIN ---

def main():
    """Main entry point for the Kestra task."""
    parser = argparse.ArgumentParser(description="ENTSO-E High-Concurrency Ingestion Script")
    parser.add_argument("--date", type=str, help="Execution date (YYYY-MM-DD)")
    parser.add_argument("--country", type=str, default="ALL", help="ISO Country code or 'ALL'")
    args = parser.parse_args()

    period_start, period_end, target_day = get_execution_window(args.date)
    target_country = args.country.upper()

    print(f"[START] Ingestion started for data date: {target_day.to_date_string()}")

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
            futures.append(executor.submit(process_country_data, country, period_start, period_end, target_day))

        # Schedule Cross-Border Flow tasks
        for base_code, info in borders.items():
            base_domain = info["domain"]
            for neighbor in info.get("borders", []):
                futures.append(executor.submit(process_border_flow, base_code, base_domain, neighbor, period_start, period_end, target_day))

        # Block until all threads complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"[CRITICAL] Worker thread raised an exception: {e}")

    print("[COMPLETED] Ingestion job finished successfully.")

if __name__ == "__main__":
    main()