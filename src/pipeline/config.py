import os
from dataclasses import dataclass, field

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
    GCP_PROJECT_ID             : str = field(default_factory = lambda: os.getenv("GCP_PROJECT_ID"            , "")        )
    GCP_REGION                 : str = field(default_factory = lambda: os.getenv("GCP_REGION"                , "us-east1"))
    GCS_BUCKET_LANDING         : str = field(default_factory = lambda: os.getenv("GCS_BUCKET_LANDING"        , "")        )
    GCS_BUCKET_PROCESS         : str = field(default_factory = lambda: os.getenv("GCS_BUCKET_PROCESS"        , "")        )
    GCS_BUCKET_DATABASE        : str = field(default_factory = lambda: os.getenv("GCS_BUCKET_DATABASE"       , "")        )
    LANDING_EVENTS_PREFIX      : str = field(default_factory = lambda: os.getenv("LANDING_EVENTS_PREFIX"     , "")        )
    LANDING_REF_PREFIX         : str = field(default_factory = lambda: os.getenv("LANDING_REF_PREFIX"        , "")        )
    BRONZE_EVENTS_PREFIX       : str = field(default_factory = lambda: os.getenv("BRONZE_EVENTS_PREFIX"      , "")        )
    BRONZE_COUNTRY_RISK_PREFIX : str = field(default_factory = lambda: os.getenv("BRONZE_COUNTRY_RISK_PREFIX", "")        )
    SILVER_EVENTS_PREFIX       : str = field(default_factory = lambda: os.getenv("SILVER_EVENTS_PREFIX"      , "")        )
    BQ_STAGING_PREFIX          : str = field(default_factory = lambda: os.getenv("BQ_STAGING_PREFIX"         , "")        )
    BQ_DATASET                 : str = field(default_factory = lambda: os.getenv("BQ_DATASET"                , "")        )
    BQ_TABLE_GOLD              : str = field(default_factory = lambda: os.getenv("BQ_TABLE_GOLD"             , "")        )
