##########################################################################################################
# Archivo     : iam.md                                                                                   #
# Nombre      : Marco Somoza                                                                             #
# Descripción : Documento de referencia para la configuración de permisos IAM necesarios para el         #
#               proyecto New Risk Monitor en Google Cloud Platform.                                      #
#                                                                                                        #
#               Define:                                                                                  #
#               - El Service Account utilizado por Cloud Run y Dataproc Serverless.                      #
#               - Los roles mínimos requeridos para operar el pipeline end-to-end.                       #
#                                                                                                        #
#               El principio aplicado es "least privilege", otorgando únicamente los permisos necesarios #
#               para:                                                                                    #
#               - Leer y escribir objetos en Cloud Storage (Landing, Process y Database zones).          #
#               - Ejecutar jobs y escribir resultados en BigQuery.                                       #
#               - Crear y ejecutar batches en Dataproc Serverless.                                       #
#               - Escribir logs en Cloud Logging para observabilidad y troubleshooting.                  #
#                                                                                                        #
#               Este documento sirve como runbook de seguridad y auditoría para el proyecto, facilitando #
#               revisiones y replicación en otros entornos (dev / prod).                                 #
#                                                                                                        #
##########################################################################################################
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
