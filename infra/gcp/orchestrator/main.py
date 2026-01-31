import os
import re
import subprocess
from google.cloud import storage

PROJECT = os.environ["GCP_PROJECT_ID"]
REGION  = os.environ.get("GCP_REGION", "us-east1")
BUCKET  = os.environ["GCS_BUCKET"]
SA      = os.environ["SERVICE_ACCOUNT_EMAIL"]

BQ_DATASET = os.environ["BQ_DATASET"]
BQ_TABLE   = os.environ["BQ_TABLE_GOLD"]
BQ_GCS_BUCKET = os.environ["BQ_GCS_BUCKET"]  # staging bucket for connector
PYSPARK_URI = os.environ["PYSPARK_URI"]

EVENTS_PREFIX = "landing-zone/gdelt/events/"
REF_PREFIX    = "landing-zone/gdelt/reference/"

def _extract_ingestion_date_and_file(obj_name: str):
    # landing-zone/gdelt/events/ingestion_date=YYYY-MM-DD/<file>
    m = re.match(r"^landing-zone/gdelt/events/ingestion_date=(\d{4}-\d{2}-\d{2})/(.+)$", obj_name)
    if not m:
        return None, None
    return m.group(1), m.group(2)

def _latest_reference_uri(client: storage.Client, bucket_name: str):
    # Find latest ingestion_date folder under reference/
    # We assume folders like: landing-zone/gdelt/reference/ingestion_date=YYYY-MM-DD/<file>
    bucket = client.bucket(bucket_name)

    # List objects under reference prefix; we’ll infer latest date from paths.
    blobs = bucket.list_blobs(prefix=REF_PREFIX + "ingestion_date=")

    latest_date = None
    latest_blob_name = None

    for b in blobs:
        # b.name example: landing-zone/gdelt/reference/ingestion_date=2026-01-24/gdelt_country_risk_20260124.csv
        m = re.match(r"^landing-zone/gdelt/reference/ingestion_date=(\d{4}-\d{2}-\d{2})/(.+)$", b.name)
        if not m:
            continue
        date = m.group(1)
        # pick latest date, and within that we assume single file; first match is ok
        if (latest_date is None) or (date > latest_date):
            latest_date = date
            latest_blob_name = b.name

    if not latest_blob_name:
        raise RuntimeError("No reference file found under landing-zone/gdelt/reference/ingestion_date=.../")

    return f"gs://{bucket_name}/{latest_blob_name}", latest_blob_name.split("/")[-1], latest_date

def handler(event, context):
    # Eventarc -> Cloud Run Functions passes Cloud Storage event payload.
    bucket_name = event.get("bucket")
    obj_name = event.get("name")

    if not bucket_name or not obj_name:
        raise ValueError("Missing bucket or name in event")

    # Only react to events files
    if not obj_name.startswith(EVENTS_PREFIX):
        return "Ignored (not events prefix)"

    ingestion_date, events_file = _extract_ingestion_date_and_file(obj_name)
    if not ingestion_date:
        return "Ignored (path not matching ingestion_date pattern)"

    client = storage.Client()
    ref_uri, ref_file, ref_date = _latest_reference_uri(client, bucket_name)

    events_uri = f"gs://{bucket_name}/{obj_name}"

    bronze_events_out = f"gs://{bucket_name}/bronze/gdelt/events/ingestion_date={ingestion_date}/"
    bronze_ref_out    = f"gs://{bucket_name}/bronze/gdelt/reference/ingestion_date={ingestion_date}/"
    silver_out        = f"gs://{bucket_name}/silver/gdelt/"

    cmd = [
        "gcloud", "dataproc", "batches", "submit", "pyspark", PYSPARK_URI,
        "--project", PROJECT,
        "--region", REGION,
        "--service-account", SA,
        "--",
        "--events_input", events_uri,
        "--country_risk_input", ref_uri,
        "--bronze_events_out", bronze_events_out,
        "--bronze_ref_out", bronze_ref_out,
        "--silver_out", silver_out,
        "--bq_project", PROJECT,
        "--bq_dataset", BQ_DATASET,
        "--bq_table", BQ_TABLE,
        "--bq_gcs_bucket", BQ_GCS_BUCKET,
        "--ingestion_date", ingestion_date,
        "--events_file_name", events_file,
        "--ref_file_name", ref_file
    ]

    subprocess.check_call(cmd)
    return f"OK: launched batch for {events_file} (ingestion_date={ingestion_date}), reference_date={ref_date}"
