# FLOWER BOX
# Dataproc Serverless - Submit PySpark ETL Script (New Risk Monitor)

## Variables (ejemplo)
- PROJECT_ID=new-risk-monitor
- REGION=us-east1
- SERVICE_ACCOUNT=bsg-sa-newriskmonitor@new-risk-monitor.iam.gserviceaccount.com
- BUCKET=bsg-gcs-newriskmonitor

## Script PySpark en GCS
Sube el script a: gs://bsg-gcs-newriskmonitor/src/pipeline/jobs/NewRiskMonitor-ETL.py

## Submit batch (ejemplo con parámetros)
gcloud dataproc batches submit pyspark gs://bsg-gcs-newriskmonitor/src/pipeline/jobs/NewRiskMonitor-ETL.py \
  --project=new-risk-monitor \
  --region=us-east1 \
  --service-account=bsg-sa-newriskmonitor@new-risk-monitor.iam.gserviceaccount.com \
  -- \
  --events_input "gs://bsg-gcs-newriskmonitor/landing-zone/gdelt/events/ingestion_date=${INGESTION_DATE}/${EVENTS_FILE}" \
  --country_risk_input "${COUNTRY_RISK_URI}" \
  --bronze_events_out "gs://bsg-gcs-newriskmonitor/bronze/gdelt/events/ingestion_date=${INGESTION_DATE}/" \
  --bronze_ref_out "gs://bsg-gcs-newriskmonitor/bronze/gdelt/reference/ingestion_date=${INGESTION_DATE}/" \
  --silver_out "gs://bsg-gcs-newriskmonitor/silver/gdelt/" \
  --bq_project "new-risk-monitor" \
  --bq_dataset "BSG_DS_NMR" \
  --bq_table "T_DW_BSG_GDELT_RISK_EVENTS" \
  --bq_gcs_bucket "bsg-gcs-newriskmonitor" \
  --ingestion_date "${INGESTION_DATE}"
