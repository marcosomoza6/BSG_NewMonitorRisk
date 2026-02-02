import os
import re
import subprocess
from flask        import Flask, request
from google.cloud import storage

##########################################################################################################
# Archivo     : main.py                                                                                  #
# Nombre      : Marco Somoza                                                                             #
# Descripción : Orquestador (Cloud Run). Se activa por Eventarc (GCS finalized) en bucket Landing,       #
#               filtra solo events, encuentra reference más reciente y lanza Dataproc Serverless batch.  #
#                                                                                                        #
##########################################################################################################
app = Flask(__name__)

PROJECT_ID                 = os.environ["GCP_PROJECT_ID"]
REGION                     = os.environ.get("GCP_REGION", "us-east1")
SERVICE_ACCOUNT            = os.environ["SERVICE_ACCOUNT_EMAIL"]
BUCKET_LANDING             = os.environ["GCS_BUCKET_LANDING"]
BUCKET_PROCESS             = os.environ["GCS_BUCKET_PROCESS"]
BUCKET_DATABASE            = os.environ["GCS_BUCKET_DATABASE"]
LANDING_EVENTS_PREFIX      = os.environ["LANDING_EVENTS_PREFIX"]      # new-risk-monitor/gdelt/landing/events
LANDING_REF_PREFIX         = os.environ["LANDING_REF_PREFIX"]         # new-risk-monitor/gdelt/reference/country_risk
BRONZE_EVENTS_PREFIX       = os.environ["BRONZE_EVENTS_PREFIX"]       # new-risk-monitor/gdelt/bronze/events
BRONZE_COUNTRY_RISK_PREFIX = os.environ["BRONZE_COUNTRY_RISK_PREFIX"] # new-risk-monitor/gdelt/bronze/country_risk
SILVER_EVENTS_PREFIX       = os.environ["SILVER_EVENTS_PREFIX"]       # new-risk-monitor/gdelt/silver/events
PYSPARK_URI                = os.environ["PYSPARK_URI"]
BQ_DATASET                 = os.environ["BQ_DATASET"]
BQ_TABLE                   = os.environ["BQ_TABLE_GOLD"]

def _extract_ingestion_date_and_file(obj_name: str):
    pattern = rf"^{re.escape(LANDING_EVENTS_PREFIX)}/ingestion_date=(\d{{4}}-\d{{2}}-\d{{2}})/(.+)$"  # {LANDING_EVENTS_PREFIX}/ingestion_date=YYYY-MM-DD/<file>
    m       = re.match(pattern, obj_name)

    if not m:
        return None, None
    
    return m.group(1), m.group(2)

def _latest_reference_uri(client: storage.Client):
    bucket           = client.bucket(BUCKET_LANDING)
    blobs            = bucket.list_blobs(prefix=LANDING_REF_PREFIX + "/ingestion_date=")
    latest_date      = None
    latest_blob_name = None

    for b in blobs:
        pattern = rf"^{re.escape(LANDING_REF_PREFIX)}/ingestion_date=(\d{{4}}-\d{{2}}-\d{{2}})/(.+)$" # {LANDING_REF_PREFIX}/ingestion_date=YYYY-MM-DD/<file>
        m       = re.match(pattern, b.name)

        if not m:
            continue

        date = m.group(1)

        if (latest_date is None) or (date > latest_date):
            latest_date      = date
            latest_blob_name = b.name

    if not latest_blob_name:
        raise RuntimeError("No reference file found under landing reference ingestion_date=...")

    ref_uri  = f"gs://{BUCKET_LANDING}/{latest_blob_name}"
    ref_file = latest_blob_name.split("/")[-1]

    return ref_uri, ref_file, latest_date

@app.post("/")
def ingest():
    event       = request.get_json(force=True)
    bucket_name = event.get("bucket")
    obj_name    = event.get("name")

    print(f"[EVENT] bucket={bucket_name} name={obj_name}", flush=True)

    if bucket_name != BUCKET_LANDING:
        print("[IGNORED] not landing bucket", flush=True)
        return ("Ignored: not landing bucket", 200)

    if not obj_name:
        print("[ERROR] missing name", flush=True)
        return ("Missing name", 400)

    if obj_name.endswith("/"):
        print("[IGNORED] folder placeholder", flush=True)
        return ("Ignored: folder placeholder", 200)

    ingestion_date, events_file = _extract_ingestion_date_and_file(obj_name)
    if not ingestion_date:
        print("[IGNORED] not events ingestion_date path", flush=True)
        return ("Ignored: not events ingestion_date path", 200)

    if not (events_file.endswith(".csv") or events_file.endswith(".tsv")):
        print("[IGNORED] not csv/tsv", flush=True)
        return ("Ignored: not csv/tsv", 200)

    print(f"[OK] triggering batch ingestion_date={ingestion_date} file={events_file}", flush=True)

    client = storage.Client()
    ref_uri, ref_file, ref_date = _latest_reference_uri(client)

    events_uri              = f"gs://{BUCKET_LANDING}/{obj_name}"
    bronze_events_out       = f"gs://{BUCKET_PROCESS}/{BRONZE_EVENTS_PREFIX}/ingestion_date={ingestion_date}/"
    bronze_country_risk_out = f"gs://{BUCKET_PROCESS}/{BRONZE_COUNTRY_RISK_PREFIX}/ingestion_date={ingestion_date}/"
    silver_out              = f"gs://{BUCKET_PROCESS}/{SILVER_EVENTS_PREFIX}/"

    cmd = ["gcloud", "dataproc", "batches", "submit", "pyspark", PYSPARK_URI,
           "--project"                , PROJECT_ID             ,
           "--region"                 , REGION                 ,
           "--service-account"        , SERVICE_ACCOUNT        ,
           "--"                       ,
           "--events_input"           , events_uri             ,
           "--reference_input"        , ref_uri                ,
           "--bronze_events_out"      , bronze_events_out      ,
           "--bronze_country_risk_out", bronze_country_risk_out,
           "--silver_out"             , silver_out             ,
           "--bq_project"             , PROJECT_ID             ,
           "--bq_dataset"             , BQ_DATASET             ,
           "--bq_table"               , BQ_TABLE               ,
           "--bq_gcs_bucket"          , BUCKET_DATABASE        ,
           "--ingestion_date"         , ingestion_date]

    subprocess.check_call(cmd)
    
    return (f"OK: launched batch for {events_file}, ref_date={ref_date}", 200)
