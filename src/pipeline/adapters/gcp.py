# FLOWER BOX
from pipeline.adapters.base import CloudAdapter

class GCPAdapter(CloudAdapter):
    def __init__(self, bucket: str):
        self.bucket = bucket

    def bronze_events_path(self, ingestion_date: str) -> str:
        return f"gs://{self.bucket}/bronze/gdelt/events/ingestion_date={ingestion_date}/"

    def bronze_ref_path(self, ingestion_date: str) -> str:
        return f"gs://{self.bucket}/bronze/gdelt/reference/ingestion_date={ingestion_date}/"

    def silver_path(self) -> str:
        return f"gs://{self.bucket}/silver/gdelt/"
