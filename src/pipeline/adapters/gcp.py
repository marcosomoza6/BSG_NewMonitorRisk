from pipeline.adapters.base import CloudAdapter

##########################################################################################################
# Archivo     : gcp.py                                                                                   #
# Nombre      : Marco Somoza                                                                             #
# Descripción : Adapter GCP, construye rutas gs:// usando buckets separados por zona.                    #
#                                                                                                        #
##########################################################################################################
class GCPAdapter(CloudAdapter):
    def __init__(self, bucket_landing: str, bucket_process: str, bucket_database: str, landing_events_prefix: str, landing_country_risk_prefix: str, bronze_events_prefix: str, bronze_country_risk_prefix: str, silver_events_prefix: str, bq_staging_prefix: str):
        self.bucket_landing              = bucket_landing
        self.bucket_process              = bucket_process
        self.bucket_database             = bucket_database
        self.landing_events_prefix       = landing_events_prefix.rstrip("/")
        self.landing_country_risk_prefix = landing_country_risk_prefix.rstrip("/")
        self.bronze_events_prefix        = bronze_events_prefix.rstrip("/")
        self.bronze_country_risk_prefix  = bronze_country_risk_prefix.rstrip("/")
        self.silver_events_prefix        = silver_events_prefix.rstrip("/")
        self.bq_staging_prefix           = bq_staging_prefix.rstrip("/")
        
    def landing_events_path(self, ingestion_date: str, filename: str) -> str:
        return f"gs://{self.bucket_landing}/{self.landing_events_prefix}/ingestion_date={ingestion_date}/{filename}"
        
    def landing_country_risk_path(self, ingestion_date: str, filename: str) -> str:
        return f"gs://{self.bucket_landing}/{self.landing_country_risk_prefix}/ingestion_date={ingestion_date}/{filename}"
        
    def bronze_events_path(self, ingestion_date: str) -> str:
        return f"gs://{self.bucket_process}/{self.bronze_events_prefix}/ingestion_date={ingestion_date}/"
        
    def bronze_country_risk_path(self, ingestion_date: str) -> str:
        return f"gs://{self.bucket_process}/{self.bronze_country_risk_prefix}/ingestion_date={ingestion_date}/"
        
    def silver_events_path(self) -> str:
        return f"gs://{self.bucket_process}/{self.silver_events_prefix}/"
        
    def bq_staging_path(self) -> str:
        return f"gs://{self.bucket_database}/{self.bq_staging_prefix}/" # Nota: BigQuery connector pide bucket sin gs:// para temporaryGcsBucket.
        