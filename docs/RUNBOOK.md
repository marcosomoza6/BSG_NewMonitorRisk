# FLOWER BOX
# RUNBOOK – New Risk Monitor (GDELT)

## Arquitectura
Landing (GCS) -> Cloud Run Orchestrator (Eventarc trigger) -> Dataproc Serverless (Spark ETL) -> Silver (Parquet GCS) -> Gold (BigQuery) ---------------------> faltan looker y pub/sub

## Rutas oficiales (GCS)
### Landing
- gs://bsg-gcs-newriskmonitor/landing-zone/gdelt/events/ingestion_date=YYYY-MM-DD/gdelt_event_YYYYMMDD.csv
- gs://bsg-gcs-newriskmonitor/landing-zone/gdelt/reference/ingestion_date=YYYY-MM-DD/gdelt_country_risk_YYYYMMDD.csv

### Bronze
- gs://bsg-gcs-newriskmonitor/bronze/gdelt/events/ingestion_date=YYYY-MM-DD/...
- gs://bsg-gcs-newriskmonitor/bronze/gdelt/reference/ingestion_date=YYYY-MM-DD/...

### Silver
- gs://bsg-gcs-newriskmonitor/silver/gdelt/ (parquet partitioned by event_date)

### BigQuery staging
- gs://bsg-gcs-newriskmonitor/big-query/staging/ (used by BigQuery connector)

## Cómo correr (end-to-end)
1) Subir reference (si no hay uno reciente)
gsutil cp gdelt_country_risk_YYYYMMDD.csv \
  gs://bsg-gcs-newriskmonitor/landing-zone/gdelt/reference/ingestion_date=YYYY-MM-DD/gdelt_country_risk_YYYYMMDD.csv

2) Subir events (esto dispara el pipeline)
gsutil cp gdelt_event_YYYYMMDD.csv \
  gs://bsg-gcs-newriskmonitor/landing-zone/gdelt/events/ingestion_date=YYYY-MM-DD/gdelt_event_YYYYMMDD.csv

## Qué hace el orquestador
- Se activa por Cloud Storage "object finalized" sobre el bucket.
- Solo procesa si el objeto está bajo landing-zone/gdelt/events/
- Extrae ingestion_date del path
- Encuentra el reference más reciente bajo landing-zone/gdelt/reference/ingestion_date=...
- Lanza Dataproc Serverless batch pasando argumentos al ETL Spark

## Validación

### A) Cloud Run logs
- Verificar que detectó ingestion_date y el archivo
- Verificar reference_date seleccionado

### B) Dataproc Serverless
- Ver que existe un batch nuevo
- Revisar logs del driver si falla

### C) Silver (GCS)
gsutil ls gs://bsg-gcs-newriskmonitor/silver/gdelt/

### D) Gold (BigQuery)
Tabla:
new-risk-monitor.BSG_DS_NMR.T_DW_BSG_GDELT_RISK_EVENTS

Query rápida:
SELECT *
FROM `new-risk-monitor.BSG_DS_NMR.T_DW_BSG_GDELT_RISK_EVENTS`
WHERE DTE_EVENT = DATE('YYYY-MM-DD')
LIMIT 100;

## Backfill
Sube archivos históricos a landing-zone con su ingestion_date correspondiente.
Cada upload de events dispara un run.

## Troubleshooting
- Si Cloud Run se dispara con objetos fuera de landing-zone/gdelt/events/ -> revisa "Ignored" en logs
- Si no encuentra reference -> sube un archivo a reference/ingestion_date=...
- Si falla write a BigQuery -> revisar permisos SA + temporaryGcsBucket
