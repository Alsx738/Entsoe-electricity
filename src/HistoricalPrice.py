import os
import json
import argparse
import time
from pathlib import Path
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
MAPPING_FILE = Path(__file__).resolve().parent / "European Price Zones Mapping (2017-2026).json"

if not ENTSOE_TOKEN:
    raise ValueError("ENTSOE_TOKEN non impostato nel file .env")

def log(message: str):
    ts = pendulum.now('Europe/Rome').format("HH:mm:ss")
    print(f"[{ts}] {message}", flush=True)

# --- GCS UTILITIES ---

def get_gcs_client():
    gcp_json = os.getenv("GCP_SA_CREDENTIALS_JSON")
    if gcp_json:
        creds_info = json.loads(gcp_json)
        credentials = service_account.Credentials.from_service_account_info(creds_info)
        return storage.Client(credentials=credentials)
    return storage.Client()

def upload_to_storage(content: str, blob_path: str):
    try:
        client = get_gcs_client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(blob_path)
        blob.upload_from_string(content, content_type="application/xml")
        log(f"✅ Caricato su GCS: {blob_path}")
    except Exception as e:
        log(f"❌ Errore GCS: {e}")
        local_path = f"data_output/{blob_path}"
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "w", encoding="utf-8") as f:
            f.write(content)
        log(f"💾 Fallback locale: {local_path}")

# --- LOGICA DI SPLIT (FIXED) ---

def split_ranges_for_country(mapping, country_code, start, end):
    """Spezza il periodo UTC in chunk annuali con semantica [start, end)."""
    ranges = []
    for entry in mapping:
        if entry["country"] != country_code:
            continue

        m_start = pendulum.parse(entry["valid_from"]).in_tz('UTC').start_of('day')
        # valid_to nel mapping e' data-only e va trattata come inclusiva sul giorno.
        m_end_exclusive = pendulum.parse(entry["valid_to"]).in_tz('UTC').add(days=1).start_of('day')

        overlap_start = max(start, m_start)
        overlap_end_exclusive = min(end, m_end_exclusive)

        if overlap_start < overlap_end_exclusive:
            current = overlap_start
            while current < overlap_end_exclusive:
                next_year_start = current.start_of('year').add(years=1)
                chunk_end_exclusive = min(next_year_start, overlap_end_exclusive)
                ranges.append((current, chunk_end_exclusive, entry["bzn_eic"]))
                current = chunk_end_exclusive
    return ranges

# --- EXTRACTION ---

def fetch_prices(country_code, eic, start, end):
    # Formattazione pulita per ENTSO-E (solo Ore)
    p_start = start.format("YYYYMMDDHH00")
    p_end = end.format("YYYYMMDDHH00")
    
    params = {
        "securityToken": ENTSOE_TOKEN,
        "documentType": "A44",
        "in_Domain": eic,
        "out_Domain": eic,
        "periodStart": p_start,
        "periodEnd": p_end
    }

    year = start.year
    blob_path = f"raw/entsoe/historical/prices/country={country_code}/year={year}/prices_{p_start}.xml"

    try:
        res = requests.get(API_URL, params=params, timeout=60)
        if res.status_code == 200:
            if "999" in res.text and "No matching data" in res.text:
                log(f"⚠️  Nessun dato per {country_code} ({year})")
            else:
                upload_to_storage(res.text, blob_path)
        else:
            log(f"🚫 Errore API {res.status_code} per {country_code}")
    except Exception as e:
        log(f"💥 Fallimento totale {country_code}: {e}")

# --- MAIN ---

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--country", default="ALL")
    args = parser.parse_args()

    # Caricamento Mapping
    try:
        with open(MAPPING_FILE, "r", encoding="utf-8") as f:
            mapping = json.load(f)
    except FileNotFoundError:
        log(f"🛑 ERRORE: File {MAPPING_FILE} non trovato!")
        return

    req_start = max(
        pendulum.parse(args.start).in_tz('UTC').start_of('day'),
        pendulum.datetime(2017, 1, 1, tz='UTC')
    )
    req_end_day = pendulum.parse(args.end).in_tz('UTC').start_of('day')
    if req_end_day < req_start:
        raise ValueError("Intervallo non valido: end deve essere uguale o successivo a start")
    req_end_exclusive = req_end_day.add(days=1)

    countries = sorted(list(set(m["country"] for m in mapping)))
    if args.country != "ALL":
        countries = [args.country.upper()]

    log(f"🚀 Avvio per {len(countries)} paesi da {req_start.to_date_string()}...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for code in countries:
            chunks = split_ranges_for_country(mapping, code, req_start, req_end_exclusive)
            for s, e, eic in chunks:
                futures.append(executor.submit(fetch_prices, code, eic, s, e))

        if not futures:
            log("🤔 Nessun task generato. Controlla le date e il JSON.")
            return

        for f in as_completed(futures):
            f.result()

    log("🏁 Lavoro completato!")

if __name__ == "__main__":
    main()