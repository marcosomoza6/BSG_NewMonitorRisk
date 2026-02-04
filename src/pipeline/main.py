import os
import sys
import subprocess
import argparse

from dotenv                  import load_dotenv
from pathlib                 import Path
from pipeline.config         import Settings
from pipeline.adapters.gcp   import GCPAdapter
from pipeline.adapters.local import LocalAdapter

##########################################################################################################
# Archivo     : main.py                                                                                  #
# Nombre      : Marco Somoza                                                                             #
# Descripción : Runner local (y opcional cloud manual). Construye rutas con adapters y llama al ETL job. #
#                                                                                                        #
##########################################################################################################
REPO_ROOT = Path(__file__).resolve().parents[2]

def _build_adapter(settings: Settings, local: bool):
    if local:
        local_input_dir = os.getenv("LOCAL_INPUT_DIR") or str(REPO_ROOT / "data" / "input")
        local_out_dir   = os.getenv("LOCAL_OUT_DIR")   or str(REPO_ROOT / "data" / "output")

        return LocalAdapter(local_input_dir = local_input_dir, local_output_dir = local_out_dir)
        
    return GCPAdapter(bucket_landing              = settings.GCS_BUCKET_LANDING,
                      bucket_process              = settings.GCS_BUCKET_PROCESS,
                      bucket_database             = settings.GCS_BUCKET_DATABASE,
                      landing_events_prefix       = settings.LANDING_EVENTS_PREFIX,
                      landing_country_risk_prefix = settings.LANDING_REF_PREFIX,
                      bronze_events_prefix        = settings.BRONZE_EVENTS_PREFIX,
                      bronze_country_risk_prefix  = settings.BRONZE_COUNTRY_RISK_PREFIX,
                      silver_events_prefix        = settings.SILVER_EVENTS_PREFIX,
                      bq_staging_prefix           = settings.BQ_STAGING_PREFIX)
        
def parse_args():
    p = argparse.ArgumentParser(description="Runner del pipeline New Risk Monitor (local/cloud manual)")
    p.add_argument("--local"         , action   = "store_true", help="Ejecuta con rutas locales (./data/input y ./data/output)")
    p.add_argument("--ingestion_date", required = True        , help="YYYY-MM-DD (para carpeta ingestion_date=...)")
    p.add_argument("--events_file"   , required = True        , help="Nombre del archivo events (ej: gdelt_event_20260124.tsv)")
    p.add_argument("--reference_file", required = True        , help="Nombre del archivo reference (si no se define, usa el del .env o el más reciente en cloud)")
    p.add_argument("--mode_silver"   , default  = "append"    , choices=["overwrite", "append"])
    p.add_argument("--mode_bq"       , default  = "append"    , choices=["overwrite", "append"])
    p.add_argument("--app_name"      , default  = "NewRiskMonitor-ETL")
    
    return p.parse_args()
    
def main():
    load_dotenv() # Carga .env localmente (en Cloud Run no lo usas)

    args = parse_args()
    s = Settings()
    
    adapter = _build_adapter(s, local=args.local)
    
    # Rutas por adapter
    events_input            = adapter.landing_events_path(args.ingestion_date, args.events_file)
    reference_input         = adapter.landing_country_risk_path(args.ingestion_date, args.reference_file)
    bronze_events_out       = adapter.bronze_events_path(args.ingestion_date)
    bronze_country_risk_out = adapter.bronze_country_risk_path(args.ingestion_date)
    silver_out              = adapter.silver_events_path()
    
    # BigQuery (futura implementación)
    bq_project     = s.GCP_PROJECT_ID
    bq_dataset     = s.BQ_DATASET
    bq_table       = s.BQ_TABLE_GOLD
    bq_temp_bucket = s.GCS_BUCKET_DATABASE
    
    etl_script = os.path.abspath(os.path.join(os.path.dirname(__file__), "jobs", "NewRiskMonitor-ETL.py"))
    
    cmd = [sys.executable, etl_script, "--events_input"           , events_input           ,
                                       "--reference_input"        , reference_input        ,
                                       "--bronze_events_out"      , bronze_events_out      ,
                                       "--bronze_country_risk_out", bronze_country_risk_out,
                                       "--silver_out"             , silver_out             ,
                                       "--bq_project"             , bq_project             ,
                                       "--bq_dataset"             , bq_dataset             ,
                                       "--bq_table"               , bq_table               ,
                                       "--bq_gcs_bucket"          , bq_temp_bucket         ,
                                       "--ingestion_date"         , args.ingestion_date    ,
                                       "--mode_silver"            , args.mode_silver       ,
                                       "--mode_bq"                , args.mode_bq           ,
                                       "--app_name"               , args.app_name]
    
    if args.local:
        gold_out = os.path.join(adapter.gold_events_path(), f"ingestion_date={args.ingestion_date}")
        cmd += ["--gold_out", gold_out]
    
    print("\n[RUN] Ejecutando ETL con comando:\n", " ".join(cmd), "\n", flush=True)
    
    subprocess.check_call(cmd)
    
if __name__ == "__main__":
    main()
    