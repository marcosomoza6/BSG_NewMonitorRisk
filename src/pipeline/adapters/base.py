# FLOWER BOX
class CloudAdapter:
    def bronze_events_path(self, ingestion_date: str) -> str:
        raise NotImplementedError

    def bronze_ref_path(self, ingestion_date: str) -> str:
        raise NotImplementedError

    def silver_path(self) -> str:
        raise NotImplementedError
