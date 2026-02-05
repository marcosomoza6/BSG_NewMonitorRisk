##########################################################################################################
# Archivo     : iam.md                                                                                   #
# Nombre      : Marco Somoza                                                                             #
# Descripción : Documento de referencia para la configuración de permisos IAM necesarios para el         #
#               proyecto New Risk Monitor en Google Cloud Platform.                                      #
#                                                                                                        #
##########################################################################################################
## Service Account:
bsg-sa-newriskmonitor@new-risk-monitor.iam.gserviceaccount.com

## Project:
new-risk-monitor

## Roles asignados (proyecto)
Artifact Registry Reader: roles/artifactregistry.reader
Artifact Registry Writer: roles/artifactregistry.writer
BigQuery Data Editor    : roles/bigquery.dataEditor
BigQuery Job User       : roles/bigquery.jobUser
Cloud Build Editor      : roles/cloudbuild.builds.editor
Cloud Run Admin         : roles/run.admin
Dataproc Editor         : roles/dataproc.editor
Dataproc Worker         : roles/dataproc.worker
Eventarc Event Receiver : roles/eventarc.eventReceiver
Logs Writer             : roles/logging.logWriter
Pub/Sub Publisher       : roles/pubsub.publisher
Service Usage Consumer  : roles/serviceusage.serviceUsageConsumer
Storage Object Admin    : roles/storage.objectAdmin
Viewer                  : roles/viewer

## Comando (ejemplo) para asignar roles
### Ejecutar como usuario con permisos IAM Admin:
export PROJECT_ID="new-risk-monitor"
export SA="bsg-sa-newriskmonitor@new-risk-monitor.iam.gserviceaccount.com"

for ROLE in \\
  roles/artifactregistry.reader \\
  roles/artifactregistry.writer \\
  roles/bigquery.dataEditor \\
  roles/bigquery.jobUser \\
  roles/cloudbuild.builds.editor \\
  roles/run.admin \\
  roles/dataproc.editor \\
  roles/dataproc.worker \\
  roles/eventarc.eventReceiver \\
  roles/logging.logWriter \\
  roles/pubsub.publisher \\
  roles/serviceusage.serviceUsageConsumer \\
  roles/storage.objectAdmin \\
  roles/viewer
do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \\
    --member="serviceAccount:${SA}" \\
    --role="${ROLE}"
done
