# FLOWER BOX
from pipeline.config import Settings

def main():
    s = Settings()
    print("Pipeline entrypoint OK")
    print("CLOUD_PROVIDER:", s.CLOUD_PROVIDER)
    print("GCP_PROJECT_ID:", s.GCP_PROJECT_ID)
    # Aquí normalmente invocarías orquestación / adapters (Workflows/Composer),
    # pero para el proyecto basta con estandarizar el entrypoint.

if __name__ == "__main__":
    main()
