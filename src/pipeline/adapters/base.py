##########################################################################################################
# Archivo     : base.py                                                                                  #
# Nombre      : Marco Somoza                                                                             #
# Descripción : Interface vendor-agnostic para construir rutas de datos por capa.                        #
#                                                                                                        #
##########################################################################################################
class CloudAdapter:
    def landing_events_path(self, ingestion_date: str, filename: str) -> str:
        raise NotImplementedError

    def landing_country_risk_path(self, ingestion_date: str, filename: str) -> str:
        raise NotImplementedError

    def bronze_events_path(self, ingestion_date: str) -> str:
        raise NotImplementedError

    def bronze_country_risk_path(self, ingestion_date: str) -> str:
        raise NotImplementedError

    def silver_events_path(self) -> str:
        raise NotImplementedError

    def bq_staging_path(self) -> str:
        raise NotImplementedError

    def gold_events_path(self) -> str:
        raise NotImplementedError
    