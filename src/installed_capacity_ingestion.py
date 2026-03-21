import os
import json
import argparse
import pendulum
import requests
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account
from concurrent.futures import ThreadPoolExecutor, as_completed


load_dotenv()

ENTSOE_TOKEN = os.getenv("ENTSOE_TOKEN")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "entsoe-data-lake")
API_URL = "https://web-api.tp.entsoe.eu/api"
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))

if not ENTSOE_TOKEN:
    raise ValueError("ENTSOE_TOKEN environment variable is not set.")


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


def load_config(filename: str):
    """Loads JSON configuration from the same directory as the script."""
    path = os.path.join(os.path.dirname(__file__), filename)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_period_key(start_date: str, end_date: str) -> str:
    """Builds a stable key used in raw storage paths."""
    start = pendulum.parse(start_date).in_tz("UTC").format("YYYYMMDD")
    end = pendulum.parse(end_date).in_tz("UTC").format("YYYYMMDD")
    return f"{start}-{end}"


def years_in_range(start_date: str, end_date: str):
    """Returns all years touched by the [start, end] date range."""
    start = pendulum.parse(start_date).in_tz("UTC").start_of("day")
    end = pendulum.parse(end_date).in_tz("UTC").start_of("day")
    if end < start:
        raise ValueError("Invalid date range: end date must be on or after start date.")
    return list(range(start.year, end.year + 1))


def yearly_capacity_window(year: int):
    """Returns ENTSO-E annual window boundaries in UTC-like ENTSO-E format."""
    period_start = f"{year - 1}12312300"
    period_end = f"{year}12312300"
    return period_start, period_end


def fetch_and_process(params: dict, blob_path: str, label: str):
    """Performs the API call and triggers storage upload."""
    req_params = dict(params)
    req_params["securityToken"] = ENTSOE_TOKEN
    try:
        response = requests.get(API_URL, params=req_params, timeout=120)
        if response.status_code == 200:
            upload_to_storage(response.text, blob_path)
        else:
            print(f"[WARNING] {label} - API returned status {response.status_code}")
    except Exception as e:
        print(f"[ERROR] {label} - Request failed: {e}")


def process_installed_capacity_country(
    country: dict,
    year: int,
    period_key: str,
    raw_prefix: str,
):
    """Downloads annual installed generation capacity (A68/A33) for one country/year."""
    code, domain = country["code"], country["domain"]
    p_start, p_end = yearly_capacity_window(year)

    fetch_and_process(
        {
            "documentType": "A68",
            "processType": "A33",
            "in_Domain": domain,
            "periodStart": p_start,
            "periodEnd": p_end,
        },
        f"{raw_prefix}/country={code}/period={period_key}/year={year}/installed_capacity.xml",
        f"InstalledCapacity-{code}-{year}",
    )


def ingest_installed_capacity(
    start_date: str,
    end_date: str,
    country_code: str = "ALL",
    raw_prefix: str = "raw/entsoe/installed_capacity",
):
    """Ingests installed capacity by expanding any range to full annual windows."""
    period_key = build_period_key(start_date, end_date)
    years = years_in_range(start_date, end_date)
    target_country = country_code.upper()

    countries = load_config("countries.json")
    if target_country != "ALL":
        countries = [c for c in countries if c["code"] == target_country]

    print(
        f"[START] Installed Capacity ingestion. Input period={start_date}..{end_date}, "
        f"expanded years={years}, country={target_country}"
    )

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for year in years:
            for country in countries:
                futures.append(
                    executor.submit(
                        process_installed_capacity_country,
                        country,
                        year,
                        period_key,
                        raw_prefix,
                    )
                )

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"[CRITICAL] Worker exception: {e}")

    print("[COMPLETED] Installed Capacity ingestion finished.")


def main():
    parser = argparse.ArgumentParser(description="ENTSO-E Installed Capacity Ingestion")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--year", type=int, help="Single year of interest (e.g. 2024)")
    parser.add_argument("--country", default="ALL", help="Country ISO code or 'ALL'")
    args = parser.parse_args()

    if args.year:
        start_date = f"{args.year}-01-01"
        end_date = f"{args.year}-12-31"
    else:
        if args.start and args.end:
            start_date = args.start
            end_date = args.end
        elif args.start:
            start_date = args.start
            end_date = args.start
        elif args.end:
            start_date = args.end
            end_date = args.end
        else:
            year = pendulum.now("UTC").year
            start_date = f"{year}-01-01"
            end_date = f"{year}-12-31"

    ingest_installed_capacity(start_date, end_date, args.country)


if __name__ == "__main__":
    main()
