##########################################################################################################
# Archivo     : RUNBOOK.md                                                                               #
# Nombre      : Marco Somoza                                                                             #
# Descripción : Runbook operativo del pipeline New Risk Monitor (GDELT) en GCP.                          #
#                                                                                                        #
##########################################################################################################
# RUNBOOK – New Risk Monitor (GDELT)

## 1) Arquitectura (end-to-end)
Landing (GCS) -> Cloud Run Orchestrator (Eventarc trigger) -> Dataproc Serverless (Spark ETL) -> Bronze (GCS) -> Silver (Parquet en GCS) -> Gold (BigQuery)

Notas:
- El trigger se dispara cuando se sube un archivo de EVENTS al landing.
- El orquestador busca el COUNTRY_RISK “más reciente” por ingestion_date.
- El ETL escribe Bronze/Silver en bucket processzone y carga Gold a BigQuery usando el connector y utilizando bucket databasezone para los archivos que genera.

Servicios: :contentReference[oaicite:1]{index=1}, :contentReference[oaicite:2]{index=2}, :contentReference[oaicite:3]{index=3}, :contentReference[oaicite:4]{index=4}

## 2) Rutas oficiales (GCS) — 3 buckets por zona
### 2.1 Landing Zone (inputs)
Bucket: gs://bsg-gcs-landingzone
- Events (GDELT TSV con extensión .csv):
  gs://bsg-gcs-landingzone/new-risk-monitor/gdelt/landing/events/ingestion_date=YYYY-MM-DD/gdelt_event_YYYYMMDD.csv

- Country Risk Reference (CSV con header):
  gs://bsg-gcs-landingzone/new-risk-monitor/gdelt/reference/country_risk/ingestion_date=YYYY-MM-DD/gdelt_country_risk_YYYYMMDD.csv

### 2.2 Process Zone (outputs)
Bucket: gs://bsg-gcs-processzone
- Bronze (copias “raw” del ETL):
  gs://bsg-gcs-processzone/new-risk-monitor/gdelt/bronze/events/ingestion_date=YYYY-MM-DD/
  gs://bsg-gcs-processzone/new-risk-monitor/gdelt/bronze/country_risk/ingestion_date=YYYY-MM-DD/

- Silver (Parquet curado):
  gs://bsg-gcs-processzone/new-risk-monitor/gdelt/silver/events/ingestion_date=YYYY-MM-DD/
  (escribe particiones por event_date, por ejemplo: event_date=2025-01-24/part-*.parquet)

### 2.3 Database Zone (BQ staging)
Bucket: gs://bsg-gcs-databasezone
- BigQuery connector staging:
  gs://bsg-gcs-databasezone/new-risk-monitor/gdelt/big-query/staging/

## 3) BigQuery (Gold)
Project: new-risk-monitor
Dataset: BSG_DS_NMR
Table  : T_DW_BSG_GDELT_RISK_EVENTS
Full   : `new-risk-monitor.BSG_DS_NMR.T_DW_BSG_GDELT_RISK_EVENTS`

Región:
- El dataset y el staging bucket deben estar creados en us-east1.

## 4) Cómo correr (end-to-end)
### 4.1 Subir reference (esto dispara el orquestador pero no el pipeline)
gsutil cp gdelt_country_risk_YYYYMMDD.csv \
  gs://bsg-gcs-landingzone/new-risk-monitor/gdelt/reference/country_risk/ingestion_date=YYYY-MM-DD/gdelt_country_risk_YYYYMMDD.csv

### 4.2 Subir events (esto dispara el orquestador pero tambien el pipeline)
gsutil cp gdelt_event_YYYYMMDD.csv \
  gs://bsg-gcs-landingzone/new-risk-monitor/gdelt/landing/events/ingestion_date=YYYY-MM-DD/gdelt_event_YYYYMMDD.csv

- NOTA: Sube un solo archivo dentro del ingestion_date correspondiente.

## 5) Qué hace el orquestador (Cloud Run)
1) Recibe evento de Cloud Storage (via Eventarc + Audit Logs)
2) Filtra para procesar SOLO si el objeto está bajo: new-risk-monitor/gdelt/landing/events/ingestion_date=YYYY-MM-DD/<file>
3) Extrae ingestion_date del path.
4) Busca el reference **más reciente** bajo: new-risk-monitor/gdelt/reference/country_risk/ingestion_date=YYYY-MM-DD/<file>
5) Lanza Dataproc Serverless (batch) con args:
   - events_input
   - reference_input
   - bronze_events_out / bronze_country_risk_out
   - silver_out
   - bq_project/dataset/table
   - bq_gcs_bucket (bucket databasezone para staging)
   - ingestion_date

## 6) Validación
### 6.1 Cloud Run logs
- Cloud Run -> Services -> bsg-cr-newriskmonitor-etl-function -> Logs
Debes ver:
- **Ignored …** si el objeto no coincide con el prefijo.
- **OK: launched batch …** cuando lanza el batch.

### 6.2 Dataproc Serverless: ver batches
Lista batches (más recientes primero):
gcloud dataproc batches list --region=us-east1 --project=new-risk-monitor

Describe el batch (reemplaza BATCH_ID con el batch mas reciente):
gcloud dataproc batches describe BATCH_ID --region=us-east1 --project=new-risk-monitor

Ver logs del driver (reemplaza BATCH_ID con el batch mas reciente):
gcloud dataproc batches get-iam-policy BATCH_ID --region=us-east1 --project=new-risk-monitor
(En UI suele ser más fácil: Dataproc -> Batches -> Driver logs)

### 6.3 Silver (GCS)
Listar particiones:
gsutil ls gs://bsg-gcs-processzone/new-risk-monitor/gdelt/silver/events/

Ver un día:
gsutil ls gs://bsg-gcs-processzone/new-risk-monitor/gdelt/silver/events/event_date=YYYY-MM-DD/

### 6.4 Gold (BigQuery)
- Conteo por día:
SELECT DTE_EVENT, COUNT(*) AS rows
FROM `new-risk-monitor.BSG_DS_NMR.T_DW_BSG_GDELT_RISK_EVENTS`
GROUP BY DTE_EVENT
ORDER BY DTE_EVENT DESC;

- Top países por score:
SELECT NAM_COUNTRY, SUM(NUM_RISK_SCORE) AS total_score
FROM `new-risk-monitor.BSG_DS_NMR.T_DW_BSG_GDELT_RISK_EVENTS`
WHERE DTE_EVENT = DATE('YYYY-MM-DD')
GROUP BY NAM_COUNTRY
ORDER BY total_score DESC
LIMIT 20;

## 7) Re-ejecución del pipeline:
- Subir nuevamente archivo **events** a Landing:
  gsutil cp gdelt_events_<YYYMMDD>.csv gs://landing/events/ingestion_date=YYYY-MM-DD/

- Cada upload dispara un run.
El reference usado será el “más reciente” disponible por ingestion_date.

## 8) Backfill (correr históricos)
- Sube archivos históricos de EVENTS al landing con su ingestion_date correcto:
  gs://bsg-gcs-landingzone/new-risk-monitor/gdelt/landing/events/ingestion_date=YYYY-MM-DD/...

## 9) Troubleshooting rápido
- Trigger se dispara "de más":
  - Con provider directo de Storage no hay filtro por prefijo; por eso se usa Audit Logs + path pattern.
  - Aun así, el código del orquestador filtra por prefijo (defensa extra).

- No encuentra reference:
  - Falta archivo en reference/country_risk/ingestion_date=.../

- Falla write a BigQuery:
  - Revisa permisos del service account (BQ + GCS staging)
  - Revisa que `temporaryGcsBucket` sea el bucket databasezone (nombre sin gs://)
  
- Spark no lee el TSV:
  - Asegúrate de sep="\t", header=false, y que sea el formato de 58 columnas.

- Logs a revisar:
 - Cloud Run logs
 - Dataproc batch status
 - BigQuery job history