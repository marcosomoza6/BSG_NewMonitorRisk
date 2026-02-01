##########################################################################################################
# Archivo     : dataproc_serverless.md                                                                   #
# Nombre      : Marco Somoza                                                                             #
# Descripción : Cómo ejecutar manualmente el ETL en Dataproc Serverless (Spark) con parámetros.          #
#                                                                                                        #
##########################################################################################################

# Dataproc Serverless - Submit PySpark ETL Script (New Risk Monitor)
## Variables (ejemplo)
- PROJECT_ID=new-risk-monitor
- REGION=us-east1
- SERVICE_ACCOUNT=bsg-sa-newriskmonitor@new-risk-monitor.iam.gserviceaccount.com
- BUCKET_LANDING=bsg-gcs-landingzone
- BUCKET_PROCESS=bsg-gcs-processzone
- BUCKET_DATABASE=bsg-gcs-databasezone

## Script PySpark en GCS
Sube el script a (recomendado en processzone):
gs://bsg-gcs-processzone/new-risk-monitor/gdelt/src/pipeline/jobs/NewRiskMonitor-ETL.py

Ejemplo de upload:
gsutil cp src/pipeline/jobs/NewRiskMonitor-ETL.py \
  gs://bsg-gcs-processzone/new-risk-monitor/gdelt/src/pipeline/jobs/NewRiskMonitor-ETL.py

## Submit batch (ejemplo con parámetros)
export INGESTION_DATE="2026-01-24"
export EVENTS_FILE="gdelt_event_20260124.csv"

# Reference: puedes pasar la ruta exacta si la conoces o la más reciente (recommended: el orquestador la resuelve).
export REF_DATE="2026-01-24"
export REF_FILE="gdelt_country_risk_20260124.csv"

gcloud dataproc batches submit pyspark \
  gs://bsg-gcs-processzone/new-risk-monitor/gdelt/src/pipeline/jobs/NewRiskMonitor-ETL.py \
  --project=new-risk-monitor \
  --region=us-east1 \
  --service-account=bsg-sa-newriskmonitor@new-risk-monitor.iam.gserviceaccount.com \
  -- \
  --events_input "gs://bsg-gcs-landingzone/new-risk-monitor/gdelt/landing/events/ingestion_date=${INGESTION_DATE}/${EVENTS_FILE}" \
  --reference_input "gs://bsg-gcs-landingzone/new-risk-monitor/gdelt/reference/country_risk/ingestion_date=${REF_DATE}/${REF_FILE}" \
  --bronze_events_out "gs://bsg-gcs-processzone/new-risk-monitor/gdelt/bronze/events/ingestion_date=${INGESTION_DATE}/" \
  --bronze_country_risk_out "gs://bsg-gcs-processzone/new-risk-monitor/gdelt/bronze/country_risk/ingestion_date=${INGESTION_DATE}/" \
  --silver_out "gs://bsg-gcs-processzone/new-risk-monitor/gdelt/silver/events/" \
  --bq_project "new-risk-monitor" \
  --bq_dataset "BSG_DS_NMR" \
  --bq_table "T_DW_BSG_GDELT_RISK_EVENTS" \
  --bq_gcs_bucket "bsg-gcs-databasezone" \
  --ingestion_date "${INGESTION_DATE}"
