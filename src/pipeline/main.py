from pipeline.config import Settings

##########################################################################################################
# Archivo     : main.py                                                                                  #
# Nombre      : Marco Somoza                                                                             #
# Descripción : Entrypoint estándar del template. Para el proyecto, valida config y muestra paths base.  #
#                                                                                                        #
##########################################################################################################
def main():
    s = Settings()
    print("Corriendo el Pipeline de ETL ....")
    print("GCP_PROJECT_ID:", s.GCP_PROJECT_ID)
    print("GCP_REGION    :", s.GCP_REGION)
    print("LANDING BUCKET:", s.GCS_BUCKET_LANDING)
    print("PROCESS BUCKET:", s.GCS_BUCKET_PROCESS)
    print("DB BUCKET     :", s.GCS_BUCKET_DATABASE)
    print("BQ DATASET    :", s.BQ_DATASET)
    print("BQ TABLE      :", s.BQ_TABLE_GOLD)

if __name__ == "__main__":
    main()
