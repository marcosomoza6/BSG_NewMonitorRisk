import os
from dataclasses import dataclass

##########################################################################################################
# Archivo     : config.py                                                                                #
# Nombre      : Marco Somoza                                                                             #
# Descripción : Configuración central del pipeline. Lee variables de entorno (.env / Cloud Run env vars) #
#               y expone un objeto Settings tipado con los parámetros necesarios para:                   #
#               - Identificar el proyecto/región de GCP.                                                 #
#               - Referenciar buckets por zona (landing/process/database).                               #
#               - Definir prefixes oficiales para Landing/Bronze/Silver.                                 #
#               - Definir destino Gold en BigQuery (dataset/tabla) y prefix de staging.                  #
#                                                                                                        #
##########################################################################################################
@dataclass
class Settings:
    GCP_PROJECT_ID             : str = os.getenv("GCP_PROJECT_ID"             , ""        )
    GCP_REGION                 : str = os.getenv("GCP_REGION"                 , "us-east1")
    GCS_BUCKET_LANDING         : str = os.getenv("GCS_BUCKET_LANDING"         , ""        )
    GCS_BUCKET_PROCESS         : str = os.getenv("GCS_BUCKET_PROCESS"         , ""        )
    GCS_BUCKET_DATABASE        : str = os.getenv("GCS_BUCKET_DATABASE"        , ""        )
    LANDING_EVENTS_PREFIX      : str = os.getenv("LANDING_EVENTS_PREFIX"      , ""        )
    LANDING_REF_PREFIX         : str = os.getenv("LANDING_REF_PREFIX"         , ""        )
    BRONZE_EVENTS_PREFIX       : str = os.getenv("BRONZE_EVENTS_PREFIX"       , ""        )
    BRONZE_COUNTRY_RISK_PREFIX : str = os.getenv("BRONZE_COUNTRY_RISK_PREFIX" , ""        )
    SILVER_EVENTS_PREFIX       : str = os.getenv("SILVER_EVENTS_PREFIX"       , ""        )
    BQ_STAGING_PREFIX          : str = os.getenv("BQ_STAGING_PREFIX"          , ""        )
    BQ_DATASET                 : str = os.getenv("BQ_DATASET"                 , ""        )
    BQ_TABLE_GOLD              : str = os.getenv("BQ_TABLE_GOLD"              , ""        )
