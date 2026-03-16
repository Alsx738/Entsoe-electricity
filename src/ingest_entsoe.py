import os
import argparse
import pendulum
import requests
import json
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account

# Load environment variables (for local development)
load_dotenv()

ENTSOE_TOKEN = os.getenv("ENTSOE_TOKEN")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "entsoe-data-lake")
API_URL = "https://web-api.tp.entsoe.eu/api"

if not ENTSOE_TOKEN:
    raise ValueError("ENTSOE_TOKEN environment variable is not set.")


def get_gcs_client() -> storage.Client:
    """Build a GCS client using a JSON credentials string or Application Default Credentials."""
    gcp_json = os.getenv("GCP_SA_CREDENTIALS_JSON")
    if gcp_json:
        creds_info = json.loads(gcp_json)
        credentials = service_account.Credentials.from_service_account_info(creds_info)
        return storage.Client(credentials=credentials)
    return storage.Client()


def upload_to_gcs(content: str, blob_path: str) -> None:
    """Upload a string to GCS. Falls back to a local file if the upload fails."""
    try:
        client = get_gcs_client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(blob_path)
        blob.upload_from_string(content, content_type="application/xml")
        print(f"Uploaded: {blob_path}")
    except Exception as e:
        print(f"GCS upload failed: {e}")
        local_path = f"data_output/{blob_path}"
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"Saved locally: {local_path}")


def load_json_file(filename: str):
    """Load a JSON file located in the same directory as this script."""
    file_path = os.path.join(os.path.dirname(__file__), filename)
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"File not found: '{filename}'")
        return []


def get_time_params(date_str: str | None = None) -> tuple[str, str, pendulum.DateTime]:
    """
    Compute ENTSO-E API time parameters for the day prior to `date_str`.
    If `date_str` is None, uses today's date.
    Returns (period_start, period_end, target_day) in UTC.
    """
    if date_str:
        try:
            execution_date = pendulum.parse(date_str)
        except Exception:
            print(f"Invalid date format: '{date_str}'. Defaulting to today.")
            execution_date = pendulum.now("UTC")
    else:
        execution_date = pendulum.now("UTC")

    target_day = execution_date.subtract(days=1).in_tz("UTC")
    start_utc = target_day.start_of("day")
    end_utc = target_day.end_of("day")

    return start_utc.format("YYYYMMDDHHmm"), end_utc.format("YYYYMMDDHHmm"), target_day


def fetch_country_data(country: dict, period_start: str, period_end: str, day: pendulum.DateTime) -> None:
    """Fetch actual load and generation data for a single country and upload to GCS."""
    code = country["code"]
    domain = country["domain"]
    year = day.year
    month = f"{day.month:02}"
    day_str = f"{day.day:02}"

    # --- Actual Load ---
    params = {
        "securityToken": ENTSOE_TOKEN,
        "documentType": "A65",
        "processType": "A16",
        "outBiddingZone_Domain": domain,
        "periodStart": period_start,
        "periodEnd": period_end,
    }
    response = requests.get(API_URL, params=params)
    if response.status_code == 200:
        upload_to_gcs(response.text, f"raw/entsoe/actual_load/country={code}/year={year}/month={month}/day={day_str}/load.xml")
    elif response.status_code != 404:
        print(f"[{code}] Load error: HTTP {response.status_code}")

    # --- Generation by Type ---
    params = {
        "securityToken": ENTSOE_TOKEN,
        "documentType": "A75",
        "processType": "A16",
        "in_Domain": domain,
        "periodStart": period_start,
        "periodEnd": period_end,
    }
    response = requests.get(API_URL, params=params)
    if response.status_code == 200:
        upload_to_gcs(response.text, f"raw/entsoe/generation/country={code}/year={year}/month={month}/day={day_str}/generation.xml")
    elif response.status_code != 404:
        print(f"[{code}] Generation error: HTTP {response.status_code}")


def fetch_cross_border_flows(borders, target_country: str, period_start: str, period_end: str, day: pendulum.DateTime) -> None:
    """Fetch cross-border physical flows (import + export) and upload to GCS."""
    year = day.year
    month = f"{day.month:02}"
    day_str = f"{day.day:02}"

    if target_country != "ALL":
        if target_country not in borders:
            print(f"No border config found for country: {target_country}")
            return
        items = [(target_country, borders[target_country])]
    else:
        items = list(borders.items())

    for base_code, country_info in items:
        base_domain = country_info["domain"]
        for neighbor in country_info.get("borders", []):
            neighbor_code = neighbor["code"]
            neighbor_domain = neighbor["domain"]

            # Import: energy flows INTO the base country FROM the neighbor
            params = {
                "securityToken": ENTSOE_TOKEN,
                "documentType": "A11",
                "in_Domain": base_domain,
                "out_Domain": neighbor_domain,
                "periodStart": period_start,
                "periodEnd": period_end,
            }
            response = requests.get(API_URL, params=params)
            if response.status_code == 200:
                upload_to_gcs(response.text, f"raw/entsoe/physical_flows/country={base_code}/direction=import/border={neighbor_code}/year={year}/month={month}/day={day_str}/flow.xml")

            # Export: energy flows OUT of the base country INTO the neighbor
            params = {
                "securityToken": ENTSOE_TOKEN,
                "documentType": "A11",
                "in_Domain": neighbor_domain,
                "out_Domain": base_domain,
                "periodStart": period_start,
                "periodEnd": period_end,
            }
            response = requests.get(API_URL, params=params)
            if response.status_code == 200:
                upload_to_gcs(response.text, f"raw/entsoe/physical_flows/country={base_code}/direction=export/border={neighbor_code}/year={year}/month={month}/day={day_str}/flow.xml")


def main() -> None:
    parser = argparse.ArgumentParser(description="ENTSO-E daily data ingestion script")
    parser.add_argument("--date", type=str, default=None, help="Execution date (e.g. 2026-03-16). Data for the previous day will be fetched.")
    parser.add_argument("--country", type=str, default="ALL", help="ISO country code (e.g. IT, FR) or 'ALL' for all European countries.")
    args = parser.parse_args()

    period_start, period_end, target_day = get_time_params(args.date)
    target_country = args.country.upper()

    print("--- ENTSO-E INGESTION JOB START ---")
    print(f"Target data date : {target_day.to_date_string()} (trigger date: {args.date or 'today'})")
    print(f"Country filter   : {target_country}")

    # 1. Actual Load & Generation
    countries = load_json_file("countries.json")
    if target_country != "ALL":
        countries = [c for c in countries if c["code"] == target_country]
        if not countries:
            print(f"Country '{target_country}' not found in countries.json.")
            return

    print(f"\nProcessing Load & Generation ({len(countries)} countries)...")
    for country in countries:
        fetch_country_data(country, period_start, period_end, target_day)

    # 2. Cross-Border Physical Flows
    borders = load_json_file("borders.json")
    print("\n Processing Cross-Border Physical Flows...")
    fetch_cross_border_flows(borders, target_country, period_start, period_end, target_day)

    print("\n Job completed successfully!")


if __name__ == "__main__":
    main()
