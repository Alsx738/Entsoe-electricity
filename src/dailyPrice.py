import os
import json
import argparse
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
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10")) # Più veloce per il daily
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

# --- MAPPING HELPER ---

def get_eic_for_date(mapping, country_code, date):
    """Trova l'EIC corretto per la data specifica."""
    for entry in mapping:
        if entry["country"] == country_code:
            v_from = pendulum.parse(entry["valid_from"]).in_tz('UTC')
            # valid_to nel mapping e' data-only e va trattata come inclusiva sul giorno.
            v_to_exclusive = pendulum.parse(entry["valid_to"]).in_tz('UTC').add(days=1)
            if v_from <= date < v_to_exclusive:
                return entry["bzn_eic"]
    return None


def resolve_day_from_date_arg(date_arg: str) -> pendulum.DateTime:
    """
    Converte --date in un giorno UTC:
    - YYYY-MM-DD: usa quel giorno esatto.
    - timestamp/datetime: considera l'istante di esecuzione e usa il giorno UTC precedente.
    """
    parsed = pendulum.parse(date_arg).in_tz('UTC')
    if len(date_arg.strip()) == 10:
        return parsed.start_of('day')
    return parsed.subtract(days=1).start_of('day')

# --- EXTRACTION ---

def fetch_daily_prices(country_code, eic, target_date):
    """Scarica i prezzi per le 24 ore della data target."""
    # Intervallo giornaliero UTC con fine esclusiva [start, end)
    start = target_date.start_of('day')
    end_exclusive = start.add(days=1)

    p_start = start.format("YYYYMMDDHHmm")
    p_end = end_exclusive.format("YYYYMMDDHHmm")
    
    params = {
        "securityToken": ENTSOE_TOKEN,
        "documentType": "A44",
        "in_Domain": eic,
        "out_Domain": eic,
        "periodStart": p_start,
        "periodEnd": p_end
    }

    # Path ottimizzato per partizionamento giornaliero: year/month/day
    blob_path = (
        f"raw/entsoe/daily/prices/"
        f"country={country_code}/year={start.year}/month={start.format('MM')}/day={start.format('DD')}/"
        f"prices_{country_code}_{p_start}.xml"
    )

    try:
        res = requests.get(API_URL, params=params, timeout=30)
        if res.status_code == 200:
            if "999" in res.text and "No matching data" in res.text:
                log(f"⚠️  Dati non ancora disponibili per {country_code} il {start.to_date_string()}")
            else:
                upload_to_storage(res.text, blob_path)
        else:
            log(f"🚫 Errore API {res.status_code} per {country_code}")
    except Exception as e:
        log(f"💥 Fallimento {country_code}: {e}")

# --- MAIN ---

def main():
    parser = argparse.ArgumentParser()
    # Se non passata, usa la data di ieri
    parser.add_argument("--date", help="Data specifica (YYYY-MM-DD). Default: ieri")
    parser.add_argument("--country", default="ALL", help="ALL o codice ISO (es. IT)")
    args = parser.parse_args()

    # Logica data: default ieri (UTC)
    if args.date:
        target_date = resolve_day_from_date_arg(args.date)
    else:
        target_date = pendulum.yesterday('UTC')

    # Caricamento Mapping
    try:
        with open(MAPPING_FILE, "r", encoding="utf-8") as f:
            mapping = json.load(f)
    except FileNotFoundError:
        log(f"🛑 ERRORE: File {MAPPING_FILE} non trovato!")
        return

    countries = sorted(list(set(m["country"] for m in mapping)))
    if args.country != "ALL":
        countries = [args.country.upper()]

    log(f"📅 Daily Import per il giorno: {target_date.to_date_string()}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for code in countries:
            eic = get_eic_for_date(mapping, code, target_date)
            if eic:
                futures.append(executor.submit(fetch_daily_prices, code, eic, target_date))
            else:
                log(f"❓ Nessun mapping EIC trovato per {code} in data {target_date.to_date_string()}")

        for f in as_completed(futures):
            f.result()

    log("🏁 Daily import completato!")

if __name__ == "__main__":
    main()