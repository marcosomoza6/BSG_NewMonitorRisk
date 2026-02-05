##########################################################################################################
# Archivo     : README.md                                                                                #
# Nombre      : Marco Somoza                                                                             #
# Proyecto    : New Risk Monitor                                                                         #
# Descripción : Este documento describe cómo ejecutar el pipeline New Risk Monitor en entorno local y    #
#               cómo desplegarlo completamente en Google Cloud Platform. Incluye la arquitectura del     #
#               proyecto, la estructura de datos, los pasos de operación y las decisiones técnicas clave #
#               necesarias para reproducir y evaluar la solución end-to-end.                             #
#                                                                                                        #
##########################################################################################################
# New Risk Monitor (GDELT) – End-to-End Data Engineering Pipeline (GCP)
El proyecto New Risk Monitor implementa un pipeline de datos end-to-end que ingiere eventos globales del dataset GDELT, los transforma y genera métricas analíticas para detectar eventos de riesgo geopolítico.  El objetivo es automatizar la ingesta, procesamiento y publicación de indicadores que permitan identificar países y ciudades con mayor riesgo en tiempo casi real, habilitando dashboards y alertas analíticas en Looker Studio.

Este repositorio implementa un pipeline de datos para ingerir eventos de **GDELT**, enriquecerlos con un **country risk reference**, producir capas **Bronze/Silver/Gold**, cargar **Gold** en **BigQuery**, y habilitar visualización en **Looker Studio**.

## 1) Arquitectura (resumen)
El pipeline sigue una arquitectura event-driven + medallion architecture:
- Diseño lógico del pipeline (Mermaid)
  flowchart LR
    A[GCS Landing] --> B[Eventarc]
    B --> C[Cloud Run Orchestrator]
    C --> D[Dataproc Serverless]
    D --> E[GCS Bronze]
    D --> F[GCS Silver]
    D --> G[BigQuery Gold]
    G --> H[Looker Studio]

Puedes consultar la imagen en: **architecture/architecture_overview.png**

## 2) Estructura de datos (Medallion):
**Buckets / zonas:**
- Landing: `bsg-gcs-landingzone`
- Process: `bsg-gcs-processzone`
- Database (staging BQ connector): `bsg-gcs-databasezone`

**BigQuery (Gold):**
- Dataset: `BSG_DS_NMR`
- Tabla: `T_DW_BSG_GDELT_RISK_EVENTS`  
  Full name: `new-risk-monitor.BSG_DS_NMR.T_DW_BSG_GDELT_RISK_EVENTS`

### Bronze (Raw): Datos sin transformar en formato CSV.
- GCS  : Copia fiel del archivo origen desde el bucket landingzone `bsg-gcs-landingzone` a processzone `bsg-gcs-processzone` de Bronze.
- LOCAL: Copia fiel del archivo origen desde el folder `data/input` a `data/output` de Bronze.

Solo realiza una copia fiel del archivo CSV Raw al bucket de processzone.

### Silver (Curated): Datos limpios y normalizados en formato Parquet.
- GCS  : Realiza un output del Dataframe ya procesado al bucket processzone `bsg-gcs-processzone` de Silver en formato Parquet.
- LOCAL: Realiza un output del Dataframe ya procesado al folder `data/output` de Silver en formato Parquet.

Realiza transformacion de Deduplicaciones, parseo de fechas, selección de columnas relevantes y enriquecimiento con country risk.

### Gold (analytics): Datos agregados listos para Dashboard.
- GCS  : Reliza un load a la tabla de Big-Query `new-risk-monitor.BSG_DS_NMR.T_DW_BSG_GDELT_RISK_EVENTS`.
- LOCAL: Realiza un output del Dataframe final al folder `data/output` en formato CSV, listo para su análisis.

Realiza la exportacion de la data final para que puedan ser analizados sus resultados e identificar riesgos.

### Decisiones clave de arquitectura
- Uso de arquitectura event-driven para evitar ejecuciones programadas innecesarias.
- Uso de Dataproc Serverless para procesamiento batch escalable sin cluster permanente.
- Separación en capas Bronze/Silver/Gold para reproducibilidad y trazabilidad.
- Ejecución local equivalente al cloud para debugging y testing.
- BigQuery como capa Gold para integración directa con BI.

### Costos y seguridad (resumen)
- Costos
El diseño minimiza costos usando:
Dataproc Serverless (pago por ejecución).
Cloud Run (escala a cero).
GCS (storage económico).
BigQuery particionado.
El pipeline no mantiene infraestructura activa.

- Seguridad
Se usa Service Account dedicada con:
Principio de mínimo privilegio.
Acceso controlado a GCS / BQ / Dataproc.
Autenticación vía Workload Identity en CI/CD.

### Mapeo de Servicios
| Componente          | Servicio            |
| ------------------- | ------------------- |
| Ingesta eventos     | Cloud Storage       |
| Trigger             | Eventarc            |
| Orquestación        | Cloud Run           |
| Procesamiento       | Dataproc Serverless |
| Storage raw/curated | Cloud Storage       |
| Analytics           | BigQuery            |
| Visualización       | Looker Studio       |

### Riesgos y Mitigaciones
| Riesgo                   | Mitigación                               |
| ------------------------ | ---------------------------------------- |
| Cambios de esquema GDELT | Validación Silver + data contracts       |
| Fallos API / ingestión   | Reintentos automáticos por event trigger |
| Fallos Spark             | Logs Dataproc + re-run manual            |
| Datos duplicados         | Deduplicación Silver                     |
| Corrupción datos         | Separación Bronze                        |

### Observavilidad propuesta
| Servicio  | Logs                  |
| --------- | --------------------- |
| Cloud Run | ejecución orquestador |
| Dataproc  | logs Spark driver     |
| BigQuery  | jobs history          |

- Métricas futuras:
    Tiempo de ejecución ETL
    Volumen de eventos procesados
    Número de ejecuciones fallidas

### Data Contract
- Ver archivos en: data_contract/schema/gdelt_bronze_country_risk.json
                                       /gdelt_bronze_events.json
                                       /gdelt_silver_events_curated.json
                                       /gdelt_gold_risk_metrics.json

## 2) Estructura del repo (alto nivel)
- `src/pipeline/` – pipeline local y utilidades.
  - `pipeline/main.py` – **entrypoint** (CLI)
  - `pipeline/config.py` – configuración (lee `.env` para ejecución local)
  - `pipeline/jobs/NewRiskMonitor-ETL.py` – job Spark (Dataproc Serverless)
- `infra/gcp/orchestrator/` – orquestador para Cloud Run (recibe evento, resuelve reference y lanza Dataproc)
- `docs/` – documentación operativa (deploy, IAM, dataproc, diccionario)
- `architecture/` – diagramas (PNG)
- `notebooks/` – exploración / explicación analítica (EDA y diseño de score)
- `tests/` – tests mínimos para CI

## 3) Requisitos locales
### 3.1 Python
- Python **3.12** recomendado (alineado con CI).

### 3.2 Instalar dependencias
- Desde el path **src** del repo:
  python -m pip install -r requirements.txt

## 4) Ejecución local
### 4.1 Entry point (pipeline/main.py)
- Desde el path **src** del repo utilizando el parametro **--local**:
  python -m pipeline.main `
    --local `
    --ingestion_date 2026-01-24 `
    --events_file gdelt_event_20260124.csv `
    --reference_file gdelt_country_risk_20260124.csv `
    --mode_silver overwrite `
    --mode_bq overwrite

### 4.2 Procesamiento de archivos
-----------------------
- Archivos de entrada:
-----------------------
Events (TSV con extensión .csv sin header):
  data/input/landing/events/ingestion_date=YYYY-MM-DD/gdelt_event_YYYYMMDD.csv

- Country risk reference (CSV con header):
  data/input/reference/country_risk/ingestion_date=YYYY-MM-DD/gdelt_country_risk_YYYYMMDD.csv

-----------------------
- Archivos de salida:
-----------------------
- Bronze Event (CSV sin header)
  data/output/bronze/events/ingestion_date=YYYY-MM-DD/part-*.csv

- Bronze Country Risk (CSV con header)
  data/output/bronze/country_risk/ingestion_date=YYYY-MM-DD/part-*.csv

- Silver Events Risk (Parquet)
  data/output/silver/events/ingestion_date=YYYY-MM-DD/part-*.parquet

- Gold Events Risk (CSV con header)
  data/output/gold/events/ingestion_date=YYYY-MM-DD/gdelt_gold_YYYY-MM-DD.csv

## 5) Crear y configurar en proyecto en GCP
### 5.1 Seleccionar proyecto y región:
  gcloud config set project new-risk-monitor
  gcloud config set run/region us-east1

### 5.2 Habilitar APIs requeridas
  gcloud services enable \\
    run.googleapis.com \\
    eventarc.googleapis.com \\
    pubsub.googleapis.com \\
    storage.googleapis.com \\
    dataproc.googleapis.com \\
    bigquery.googleapis.com \\
    cloudbuild.googleapis.com \\
    artifactregistry.googleapis.com \\
    logging.googleapis.com

### 5.3 Crear buckets (3 zonas)
  gsutil mb -p new-risk-monitor -l us-east1 gs://bsg-gcs-landingzone
  gsutil mb -p new-risk-monitor -l us-east1 gs://bsg-gcs-processzone
  gsutil mb -p new-risk-monitor -l us-east1 gs://bsg-gcs-databasezone

- Nota: ajustar --location si requiere una región específica.

### 5.4 Crear Service Account
  gcloud iam service-accounts create bsg-sa-newriskmonitor \\
    --display-name="bsg-sa-newriskmonitor"

- Service Account email: bsg-sa-newriskmonitor@new-risk-monitor.iam.gserviceaccount.com

### 5.5 Asginar los roles requeridos:
  PROJECT_ID="new-risk-monitor"
  SA="bsg-sa-newriskmonitor@new-risk-monitor.iam.gserviceaccount.com"

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
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \\
      --member="serviceAccount:$SA" \\
      --role="$ROLE"
  done

### 5.6 Crear dataset en Big-Query:
bq --location=us-east1 mk -d \\
  --description "New Risk Monitor dataset" \\
  new-risk-monitor:BSG_DS_NMR

### 5.7 Crear tabla Gold:
CREATE TABLE IF NOT EXISTS `new-risk-monitor.BSG_DS_NMR.T_DW_BSG_GDELT_RISK_EVENTS` (DTE_EVENT         DATE,
                                                                                     NAM_COUNTRY       STRING,
                                                                                     NAM_CITY          STRING,
                                                                                     CNT_EVENTS        INT64 NOT NULL,
                                                                                     NUM_GOLDSTEIN_AVG FLOAT64,
                                                                                     NUM_RISK_SCORE    FLOAT64,
                                                                                     DTE_INGESTION      DATE)
PARTITION BY DTE_EVENT
CLUSTER BY NAM_COUNTRY, NAM_CITY;

- Nota: este query se ejecuta en un editor dentro del servicio de Big-Query

### 5.8 Dataproc Serverless (ejecución manual)
Ver guía completa en: docs/dataproc_serverless.md

### 5.9 Deploy Cloud Run (orchestrator)
ENV_VARS="GCP_PROJECT_ID=new-risk-monitor,GCP_REGION=us-east1,SERVICE_ACCOUNT_EMAIL=bsg-sa-newriskmonitor@new-risk-monitor.iam.gserviceaccount.com,GCS_BUCKET_LANDING=bsg-gcs-landingzone,GCS_BUCKET_PROCESS=bsg-gcs-processzone,GCS_BUCKET_DATABASE=bsg-gcs-databasezone,GCS_ROOT_PREFIX=new-risk-monitor/gdelt,LANDING_EVENTS_PREFIX=new-risk-monitor/gdelt/landing/events,LANDING_REF_PREFIX=new-risk-monitor/gdelt/reference/country_risk,BRONZE_EVENTS_PREFIX=new-risk-monitor/gdelt/bronze/events,BRONZE_COUNTRY_RISK_PREFIX=new-risk-monitor/gdelt/bronze/country_risk,SILVER_EVENTS_PREFIX=new-risk-monitor/gdelt/silver/events,PYSPARK_URI=gs://bsg-gcs-processzone/new-risk-monitor/gdelt/src/pipeline/jobs/NewRiskMonitor-ETL.py,BQ_DATASET=BSG_DS_NMR,BQ_TABLE_GOLD=T_DW_BSG_GDELT_RISK_EVENTS,BQ_STAGING_PREFIX=new-risk-monitor/gdelt/big-query/staging"

gcloud run deploy "bsg-cr-newriskmonitor-etl-function" \\
  --source "infra/gcp/orchestrator" \\
  --project "new-risk-monitor" \\
  --region "us-east1" \\
  --service-account "bsg-sa-newriskmonitor@new-risk-monitor.iam.gserviceaccount.com" \\
  --allow-unauthenticated \\
  --set-build-env-vars "GOOGLE_RUNTIME_VERSION=3.12.3" \\
  --set-env-vars "$ENV_VARS"

### 5.10 Crear trigger Eventarc
- Ver guía completa en: docs/cloud_run_trigger.md

### 5.11 Looker Studio (conectar + alerta visual)
Abrir Looker Studio: https://lookerstudio.google.com/
  1.- Create ---> Data source ---> BigQuery
  2.- Seleccionar ---> Project: new-risk-monitor
                       Dataset: BSG_DS_NMR
                      Table: T_DW_BSG_GDELT_RISK_EVENTS
  3.- Campo calculado recomendado (nivel de riesgo)
  4.- Create field ---> Calculated Field:
    CASE
      WHEN NUM_RISK_SCORE >= 75 THEN "CRÍTICO"
      WHEN NUM_RISK_SCORE >= 60 THEN "ALTO"
      WHEN NUM_RISK_SCORE >= 40 THEN "MEDIO"
      ELSE "BAJO"
    END

  5a) Agregar tabla:
    Add chart ---> Table ---> Dimension: DTE_EVENT
                                         NAM_COUNTRY
                                         NAM_CITY
                                         RISK_LEVEL
                              Metric   : NUM_RISK_SCORE
                                         CNT_EVENTS
                                         NUM_GOLDSTEIN_AVG

  5b) Alerta visual:
  - Google Maps (Heatmap) ---> Location: NAM_CITY
                               Weight  : NUM_RISK_SCORE

## 6) GitHub Actions: CI y Deploy Orchestrator (Totalmente opcional)
- Workflow: .github/workflows/ci.yml

### Secrets requeridos (GitHub ---> Settings ---> Secrets and variables ---> Actions)
GCP_SERVICE_ACCOUNT = bsg-sa-newriskmonitor@new-risk-monitor.iam.gserviceaccount.com
GCP_WORKLOAD_IDENTITY_PROVIDER = Obtenerlo en GCP IAM ---> Workload Identity Federation (o gcloud) ---> projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/POOL/providers/PROVIDER

Si no tienes configurado WIF aún, la guía recomendada es crear el Pool/Provider y luego setear estos secretos pero esto ya se mantiene fuera del scope del README.
