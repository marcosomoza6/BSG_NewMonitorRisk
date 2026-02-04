from pathlib                import Path
from pipeline.adapters.base import CloudAdapter

##########################################################################################################
# Archivo     : local.py                                                                                 #
# Nombre      : Marco Somoza                                                                             #
# Descripción : Adapter Local, construye rutas file system usando data/input y data/output.              #
#               Mantiene la misma semántica de carpetas que en GCS.                                      #
#                                                                                                        #
##########################################################################################################
class LocalAdapter(CloudAdapter):
    def __init__(self, local_input_dir: str, local_output_dir: str):
        self.local_input_dir  = Path(local_input_dir).resolve()
        self.local_output_dir = Path(local_output_dir).resolve()
    
    #########################################################################
    # Landing (input)                                                       #
    #########################################################################
    def landing_events_path(self, ingestion_date: str, filename: str) -> str:
        return str(self.local_input_dir / "landing" / "events" / f"ingestion_date={ingestion_date}" / filename)
    
    def landing_country_risk_path(self, ingestion_date: str, filename: str) -> str:
        return str(self.local_input_dir / "reference" / "country_risk" / f"ingestion_date={ingestion_date}" / filename)
    
    #########################################################################
    # Bronze (output)                                                       #
    #########################################################################
    def bronze_events_path(self, ingestion_date: str) -> str:
        return str(self.local_output_dir / "bronze" / "events" / f"ingestion_date={ingestion_date}")
    
    def bronze_country_risk_path(self, ingestion_date: str) -> str:
        return str(self.local_output_dir / "bronze" / "country_risk" / f"ingestion_date={ingestion_date}")
    
    #########################################################################
    # Silver (output)                                                       #
    #########################################################################
    def silver_events_path(self) -> str:
        return str(self.local_output_dir / "silver" / "events")
    
    #########################################################################
    # Big-Query (output)                                                    #
    # bq_staging_path se ejecuta solo en modo GCP (futura implementación)   #
    # gold_events_path se ejecuta solo en modo local.                       #
    #########################################################################
    def bq_staging_path(self) -> str:
        return str(self.local_output_dir / "bq_staging")
    
    def gold_events_path(self) -> str:
        return str(self.local_output_dir / "gold" / "events")
    