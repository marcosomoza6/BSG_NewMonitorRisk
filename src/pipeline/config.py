# FLOWER BOX
import os

def env(name: str, default: str = "") -> str:
    return os.getenv(name, default)

class Settings:
    GCP_PROJECT_ID = env("GCP_PROJECT_ID")
    GCS_BUCKET     = env("GCS_BUCKET")
    CLOUD_PROVIDER = env("CLOUD_PROVIDER", "gcp")
    GCP_REGION     = env("GCP_REGION"    , "us-east1")
    GCS_PREFIX     = env("GCS_PREFIX"    , "BSG_DS_NMR")
    BQ_DATASET     = env("BQ_DATASET"    , "BSG_DS_NMR")
    BQ_TABLE_GOLD  = env("BQ_TABLE_GOLD" , "T_DW_BSG_GDELT_RISK_EVENTS")
    BQ_GCS_BUCKET  = env("BQ_GCS_BUCKET" , env("GCS_BUCKET"))
    INGESTION_DATE = env("INGESTION_DATE")
