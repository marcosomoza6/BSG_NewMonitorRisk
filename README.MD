gcloud auth application-default login
gcloud config set project new-risk-monitor

$env:PYTHONPATH="src"
python -m pipeline.main `
  --local `
  --ingestion_date 2026-01-24 `
  --events_file gdelt_event_20260124.csv `
  --reference_file gdelt_country_risk_20260109.csv `
  --mode_silver append `
  --mode_bq append

CREATE TABLE IF NOT EXISTS `new-risk-monitor.BSG_DS_NMR.T_DW_BSG_GDELT_RISK_EVENTS` (
  DTE_EVENT        DATE,
  NAM_COUNTRY      STRING,
  NAM_CITY         STRING,
  CNT_EVENTS       INT64 NOT NULL,
  NUM_GOLDSTEIN_AVG FLOAT64,
  NUM_RISK_SCORE    FLOAT64,
  DTE_INGESTION     DATE
)
PARTITION BY DTE_EVENT
CLUSTER BY NAM_COUNTRY, NAM_CITY;