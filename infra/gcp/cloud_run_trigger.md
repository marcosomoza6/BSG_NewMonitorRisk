# FLOWER BOX
# Cloud Storage Trigger -> Cloud Run (Eventarc)

## Objetivo
Cuando se suba un archivo de events en:
landing-zone/gdelt/events/ingestion_date=YYYY-MM-DD/gdelt_event_YYYYMMDD.csv

...se dispara un servicio Cloud Run que:
1) obtiene ingestion_date y file name del path
2) encuentra el reference latest en landing-zone/gdelt/reference/...
3) lanza Dataproc Serverless batch con esos parámetros

## Crear Cloud Run service
- Runtime: Python
- Service account: bsg-sa-newriskmonitor@new-risk-monitor.iam.gserviceaccount.com
- Env vars:
  - GCP_PROJECT_ID=new-risk-monitor
  - GCP_REGION=us-east1
  - GCS_BUCKET=bsg-gcs-newriskmonitor
  - BQ_DATASET=BSG_DS_NMR
  - BQ_TABLE_GOLD=T_DW_BSG_GDELT_RISK_EVENTS
  - BQ_GCS_BUCKET=bsg-gcs-newriskmonitor
  - PYSPARK_URI=gs://bsg-gcs-newriskmonitor/src/pipeline/jobs/NewRiskMonitor-ETL.py

## Crear trigger (Storage finalize)
En Cloud Run -> Triggers -> Add trigger:
- Provider: Cloud Storage
- Event: Object finalized
- Bucket: bsg-gcs-newriskmonitor
- Path filter: landing-zone/gdelt/events/
- Destination: bsg-cr-newriskmonitor-etl-function

Docs: Cloud Run storage triggers (Eventarc)
