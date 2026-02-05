##########################################################################################################
# Archivo     : cloud_run_trigger.md                                                                     #
# Nombre      : Marco Somoza                                                                             #
# Descripción : Guía operativa para el despliegue del orquestador en Cloud Run y la creación del trigger #
#               Eventarc basado en Cloud Storage (object finalized).                                     #
#                                                                                                        #
##########################################################################################################
## Recursos
- Cloud Run service: bsg-cr-newriskmonitor-etl-function
- Eventarc trigger: bsg-trigger-newriskmonitor-events-ingest
- Landing bucket: gs://bsg-gcs-landingzone
- Process bucket: gs://bsg-gcs-processzone
- Database bucket (BQ staging): gs://bsg-gcs-databasezone
- Region: us-east1
- Service Account: bsg-sa-newriskmonitor@new-risk-monitor.iam.gserviceaccount.com

## Rutas (GCS)
### Landing (inputs)
- Events:
  gs://bsg-gcs-landingzone/new-risk-monitor/gdelt/landing/events/ingestion_date=YYYY-MM-DD/gdelt_event_YYYYMMDD.csv

- Country risk reference:
  gs://bsg-gcs-landingzone/new-risk-monitor/gdelt/reference/country_risk/ingestion_date=YYYY-MM-DD/gdelt_country_risk_YYYYMMDD.csv

### Process (outputs)
- Bronze:
  gs://bsg-gcs-processzone/new-risk-monitor/gdelt/bronze/events/ingestion_date=YYYY-MM-DD/
  gs://bsg-gcs-processzone/new-risk-monitor/gdelt/bronze/country_risk/ingestion_date=YYYY-MM-DD/

- Silver:
  gs://bsg-gcs-processzone/new-risk-monitor/gdelt/silver/events/

### BigQuery staging (databasezone)
- gs://bsg-gcs-databasezone/new-risk-monitor/gdelt/big-query/staging/

## Variables de entorno del Cloud Run service
Configura estas env vars en el servicio bsg-cr-newriskmonitor-etl-function:

GCP_PROJECT_ID=new-risk-monitor
GCP_REGION=us-east1
SERVICE_ACCOUNT_EMAIL=bsg-sa-newriskmonitor@new-risk-monitor.iam.gserviceaccount.com

GCS_BUCKET_LANDING=bsg-gcs-landingzone
GCS_BUCKET_PROCESS=bsg-gcs-processzone
GCS_BUCKET_DATABASE=bsg-gcs-databasezone

GCS_ROOT_PREFIX=new-risk-monitor/gdelt

LANDING_EVENTS_PREFIX=new-risk-monitor/gdelt/landing/events
LANDING_REF_PREFIX=new-risk-monitor/gdelt/reference/country_risk

BRONZE_EVENTS_PREFIX=new-risk-monitor/gdelt/bronze/events
BRONZE_COUNTRY_RISK_PREFIX=new-risk-monitor/gdelt/bronze/country_risk
SILVER_EVENTS_PREFIX=new-risk-monitor/gdelt/silver/events

PYSPARK_URI=gs://bsg-gcs-processzone/new-risk-monitor/gdelt/src/pipeline/jobs/NewRiskMonitor-ETL.py

BQ_DATASET=BSG_DS_NMR
BQ_TABLE_GOLD=T_DW_BSG_GDELT_RISK_EVENTS

## Deploy Manual
ENV_VARS="GCP_PROJECT_ID=new-risk-monitor,GCP_REGION=us-east1,SERVICE_ACCOUNT_EMAIL=bsg-sa-newriskmonitor@new-risk-monitor.iam.gserviceaccount.com,GCS_BUCKET_LANDING=bsg-gcs-landingzone,GCS_BUCKET_PROCESS=bsg-gcs-processzone,GCS_BUCKET_DATABASE=bsg-gcs-databasezone,GCS_ROOT_PREFIX=new-risk-monitor/gdelt,LANDING_EVENTS_PREFIX=new-risk-monitor/gdelt/landing/events,LANDING_REF_PREFIX=new-risk-monitor/gdelt/reference/country_risk,BRONZE_EVENTS_PREFIX=new-risk-monitor/gdelt/bronze/events,BRONZE_COUNTRY_RISK_PREFIX=new-risk-monitor/gdelt/bronze/country_risk,SILVER_EVENTS_PREFIX=new-risk-monitor/gdelt/silver/events,PYSPARK_URI=gs://bsg-gcs-processzone/new-risk-monitor/gdelt/src/pipeline/jobs/NewRiskMonitor-ETL.py,BQ_DATASET=BSG_DS_NMR,BQ_TABLE_GOLD=T_DW_BSG_GDELT_RISK_EVENTS,BQ_STAGING_PREFIX=new-risk-monitor/gdelt/big-query/staging"

gcloud run deploy "bsg-cr-newriskmonitor-etl-function" \\
  --source "infra/gcp/orchestrator" \\
  --project "new-risk-monitor" \\
  --region "us-east1" \\
  --service-account "bsg-sa-newriskmonitor@new-risk-monitor.iam.gserviceaccount.com" \\
  --allow-unauthenticated \\
  --set-build-env-vars "GOOGLE_RUNTIME_VERSION=3.12.3" \\
  --set-env-vars "$ENV_VARS"

## Crear Trigger (Eventarc)
Crear un trigger de Cloud Storage (Object Finalized) en el bucket landing:
- Event provider: Cloud Storage
- Event type: google.cloud.storage.object.v1.finalized
- Bucket: bsg-gcs-landingzone
- Destination: Cloud Run service bsg-cr-newriskmonitor-etl-function
- Service URL path: /

Nota:
El trigger se dispara por cualquier objeto finalizado en el bucket landing. El código del orquestador filtra para ejecutar SOLO cuando el objeto cae bajo:
new-risk-monitor/gdelt/landing/events/
