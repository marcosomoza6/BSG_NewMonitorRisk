# FLOWER BOX
# IAM - New Risk Monitor (GCP)

Service Account:
- bsg-sa-newriskmonitor@new-risk-monitor.iam.gserviceaccount.com

Project:
- new-risk-monitor

### Storage (leer/escribir en GCS)
- roles/storage.objectAdmin

### BigQuery (crear/escribir tablas y correr jobs)
- roles/bigquery.dataEditor
- roles/bigquery.jobUser

### Dataproc Serverless (batches)
- roles/dataproc.worker

### Logging
- roles/logging.logWriter

## Comandos gcloud (ejemplo)
# Ejecutar estos comandos como un usuario con permisos de IAM Admin en el proyecto.
export PROJECT_ID="new-risk-monitor"
export SA="bsg-sa-newriskmonitor@new-risk-monitor.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SA}" \
  --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SA}" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SA}" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SA}" \
  --role="roles/dataproc.worker"

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SA}" \
  --role="roles/logging.logWriter"
