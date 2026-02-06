#!/usr/bin/env python3
import argparse
import os
import shutil

from dataclasses  import dataclass
from datetime     import datetime, timezone
from pathlib      import Path
from pyspark.sql  import SparkSession, functions as F, types as T

##########################################################################################################
# Archivo     : NewRiskMonitor-ETL.py                                                                    #
# Nombre      : Marco Somoza                                                                             #
# Descripción : Este Script toma un archivo de entrada para ser procesado en un pipeline de datos ETL    #
#               para el proyecto final de BSG.                                                           #
#                                                                                                        #
##########################################################################################################
# Datos I/O:                                                                                             #
# - Landing Input : gdelt_event_YYYYMMDD.csv            (local o landing zone bucket)                    #
#                   gdelt_country_risk_YYYYMMDD.csv     (local o landing zone bucket)                    #
# - Bronze Output : bsg-gdelt-events-YYYYMMDD.csv       (local o process zone bucket)                    #
#                   bsg-gdelt-country_risk-YYYYMMDD.csv (local o process zone bucket)                    #
# - Silver Output : part-*.parquet                      (local o process zone bucket)                    #
# - Gold Output   : T_DW_BSG_GDELT_RISK_EVENTS          (requiere conector spark-bigquery)               #
#                                                                                                        #
##########################################################################################################
def build_spark(app_name: str) -> SparkSession:
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    
    if os.name == "nt":
        import sys
        os.environ.setdefault("PYSPARK_PYTHON"       , sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    return (SparkSession.builder.appName(app_name).getOrCreate())
    
@dataclass
class ETLConfig:
    events_input            : str
    reference_input         : str
    bronze_events_out       : str
    bronze_country_risk_out : str
    silver_out              : str
    bq_project              : str
    bq_dataset              : str
    bq_table                : str
    bq_gcs_bucket           : str
    ingestion_date          : str
    app_name                : str
    mode_silver             : str
    mode_bq                 : str
    gold_out                : str
    
def parse_args() -> ETLConfig:
    p = argparse.ArgumentParser(description="GDELT New Risk Monitor ETL (Landing/Bronze/Silver/Gold) - Spark/Dataproc")
    p.add_argument("--app_name"   , default = "NewRiskMonitor-ETL", help    = "Nombre de la app del ETL")
    p.add_argument("--mode_silver", default = "overwrite"         , choices = ["overwrite", "append"])
    p.add_argument("--mode_bq"    , default = "append"            , choices = ["overwrite", "append"])
    p.add_argument("--gold_out"   , default = ""                  , help    = "Si se especifica, exporta GOLD a CSV local y evita BigQuery.")

    p.add_argument("--bq_project"             , required = True, help = "GCP Project ID (New Risk Monitor)")
    p.add_argument("--events_input"           , required = True, help = "Archivo de entrada para eventos CSV")
    p.add_argument("--reference_input"        , required = True, help = "Archivo de entrada para reference CSV")
    p.add_argument("--bronze_events_out"      , required = True, help = "Archivo de salida para Bronze Events")
    p.add_argument("--bronze_country_risk_out", required = True, help = "Archivo de salida para Bronze Country" )
    p.add_argument("--silver_out"             , required = True, help = "Archivo de salida para Silver parquet")
    p.add_argument("--bq_dataset"             , required = True, help = "BigQuery dataset (BSG_DS_NMR)")
    p.add_argument("--bq_table"               , required = True, help = "BigQuery table name (T_DW_BSG_GDELT_RISK_EVENTS)")
    p.add_argument("--bq_gcs_bucket"          , required = True, help = "Bucket para staging del conector de BigQuery")
    p.add_argument("--ingestion_date"         , required = True, help = "Fecha de la ingesta (YYYY-MM-DD)")
    
    a = p.parse_args()
    
    return ETLConfig(events_input            = a.events_input           ,
                     reference_input         = a.reference_input        ,
                     bronze_events_out       = a.bronze_events_out      ,
                     bronze_country_risk_out = a.bronze_country_risk_out,
                     silver_out              = a.silver_out             ,
                     bq_project              = a.bq_project             ,
                     bq_dataset              = a.bq_dataset             ,
                     bq_table                = a.bq_table               ,
                     bq_gcs_bucket           = a.bq_gcs_bucket          ,
                     ingestion_date          = a.ingestion_date         ,
                     app_name                = a.app_name               ,
                     mode_silver             = a.mode_silver            ,
                     mode_bq                 = a.mode_bq                ,
                     gold_out                = a.gold_out)
                        
class IOAdapter:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def read_events_raw(self, path: str):
        return self.spark.read.option("sep", "\t").option("header", "false").csv(path)
        
    def read_reference_raw(self, path: str):
        return self.spark.read.option("header", "true").option("inferSchema", "true").csv(path)
        
    def write_bronze_events(self, df, out_path: str):
        (df.coalesce(1).write.mode("overwrite").option("header", "false").option("sep"   , "\t") .csv(out_path))
        
    def write_bronze_reference(self, df, out_path: str):
        (df.coalesce(1).write.mode("overwrite").option("header", "true").csv(out_path))
        
    def write_silver(self, df, out_path: str, mode: str):
        (df.write.mode(mode).partitionBy("event_date").parquet(out_path))
        
    def write_bigquery(self, df, bq_project: str, bq_dataset: str, bq_table: str, temp_bucket: str, mode: str):
        full_table = f"{bq_project}:{bq_dataset}.{bq_table}"
        
        (df.write.format("bigquery").option("table", full_table)
                                    .option("temporaryGcsBucket", temp_bucket).mode(mode).save())
        
        return full_table
        
###########################
#         Layers          #
###########################
class BronzeLayer:
    def __init__(self, io: IOAdapter):
        self.io = io
        
    def run(self, events_input: str, reference_input: str, bronze_events_out: str, bronze_ref_out: str):
        df_events_raw = self.io.read_events_raw(events_input)
        df_ref_raw    = self.io.read_reference_raw(reference_input)
        
        # Escribir outputs a Bronze
        self.io.write_bronze_events(df_events_raw, bronze_events_out)
        self.io.write_bronze_reference(df_ref_raw, bronze_ref_out)
        
        return df_events_raw, df_ref_raw
        
class SilverLayer:
    def __init__(self, io: IOAdapter):
        self.io = io
        
    def run(self, df_events_raw, df_ref_raw, silver_out: str, mode_silver: str, ingestion_date: str):
        ingestion_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # Normalizar columnas de Events
        df = df_events_raw.select(          F.col("_c0").cast(T.LongType()).alias("globaleventid")     ,
                                  F.to_date(F.col("_c1"), "yyyyMMdd").alias("event_date")              ,
                                            F.col("_c29").cast(T.IntegerType()).alias("quadclass")     ,
                                            F.col("_c30").cast(T.DoubleType()).alias("goldstein_scale"),
                                            F.col("_c50").alias("actiongeo_fullname")                  ,
                                            F.col("_c51").alias("actiongeo_countrycode")               ,
                                            F.col("_c52").alias("actiongeo_adm1code")                  ,
                                            F.col("_c53").cast(T.DoubleType()).alias("actiongeo_lat" ) ,
                                            F.col("_c54").cast(T.DoubleType()).alias("actiongeo_long"))
        
        # Derivadas y limpieza básica
        df = (df.withColumn("country", F.upper(F.col("actiongeo_countrycode")))
                .withColumn("city", F.when(F.col("actiongeo_fullname").isNull(), F.lit(None)).otherwise(F.trim(F.split(F.col("actiongeo_fullname"), ",").getItem(0))))
                .withColumn("ingestion_date", F.lit(ingestion_date))
                .withColumn("ingestion_ts", F.lit(ingestion_ts)))
        
        # DQ mínimo y dedup
        df = (df.filter((F.col("event_date"   ).isNotNull()) &
                        (F.col("globaleventid").isNotNull()) &
                        (F.col("country"      ).isNotNull()) &
                        (F.length(F.col("country")) == 2)).dropDuplicates(["globaleventid"]))
        
        # Utilizar numeros para risk base por quadclass
        df = df.withColumn("risk_weight", F.when(F.col("quadclass") == 1, F.lit(0.25))
                                           .when(F.col("quadclass") == 2, F.lit(0.50))
                                           .when(F.col("quadclass") == 3, F.lit(0.75))
                                           .when(F.col("quadclass") == 4, F.lit(1.00)).otherwise(F.lit(0.50)))
        
        # Normalizar coumnas de Reference y descartar duplicates por pais
        df_ref = (df_ref_raw.select(F.upper(F.col("country")).alias("country")                                    ,
                                            F.col("baseline_risk").cast(T.DoubleType()).alias("baseline_risk")    ,
                                            F.col("risk_multiplier").cast(T.DoubleType()).alias("risk_multiplier"),
                                            F.col("updated_at").cast(T.StringType()).alias("updated_at")).dropDuplicates(["country"]))
        
        # Join de ambos Dataframes por pais y ajustar valor de riesgo: events.risk_weight * country_risk.risk_multiplier
        df = (df.join(df_ref, on = "country", how = "left").withColumn("risk_weight_adj", F.col("risk_weight") * F.coalesce(F.col("risk_multiplier"), F.lit(1.0))))
        
        # Escribir outputs a Silver
        self.io.write_silver(df, silver_out, mode_silver)
        
        return df
        
class GoldLayer:
    def __init__(self, io: IOAdapter):
        self.io = io
        
    def build_mart(self, df, ingestion_date: str):
        # Hacer calculos con aggregations
        mart = (df.groupBy("event_date", "country", "city").agg(F.count("*").alias("events_count")              , 
                                                                F.sum("risk_weight_adj").alias("risk_score_raw"),
                                                                F.avg("goldstein_scale").alias("avg_goldstein")).withColumn("risk_score", F.round((F.col("risk_score_raw") / F.col("events_count")) * 100, 2)).drop("risk_score_raw"))
        # Agregar columna ingestion date
        mart = mart.withColumn("ingestion_date", F.to_date(F.lit(ingestion_date)))

        # Definir estructura final del mart
        mart_bq = (mart.withColumnRenamed("event_date"    , "DTE_EVENT"        )
                       .withColumnRenamed("country"       , "NAM_COUNTRY"      )
                       .withColumnRenamed("city"          , "NAM_CITY"         )
                       .withColumnRenamed("events_count"  , "CNT_EVENTS"       )
                       .withColumnRenamed("risk_score"    , "NUM_RISK_SCORE"   )
                       .withColumnRenamed("avg_goldstein" , "NUM_GOLDSTEIN_AVG")
                       .withColumnRenamed("ingestion_date", "DTE_INGESTION"    ))
        
        return mart_bq
    
    def run(self, df_silver, bq_project, bq_dataset, bq_table, bq_gcs_bucket, mode_bq, gold_out: str, ingestion_date: str):
        mart_bq  = self.build_mart(df_silver, ingestion_date)
        gold_out = (gold_out or "").strip()
        
        ###########################
        # MODO LOCAL: CSV + PRINT #
        ###########################
        if gold_out:
            print(f"Exportando salida GOLD a CSV en: {gold_out}")
            print( "GOLD LAYER LOCAL Schema:")
            mart_bq.printSchema()

            print("----------------------------------------------------")
            print("GOLD LAYER LOCAL Data:")
            mart_bq.show(n = 100, truncate = False)

            # Escribir outputs a Gold local
            (mart_bq.coalesce(1).write.mode("overwrite").option("header", "true").csv(gold_out))
            
            # Renombrar los output files de part-*.csv a nombre fijo
            out_dir    = Path(gold_out)
            part_files = list(out_dir.glob("part-*.csv"))

            if part_files:
                final_name = out_dir / f"gdelt_gold_{ingestion_date}.csv"
                success    = out_dir / "_SUCCESS"
                
                if final_name.exists():
                    final_name.unlink()
                
                if success.exists():
                    success.unlink()

                shutil.move(str(part_files[0]), str(final_name))

            # Limpiar archivos _SUCCESS y *.crc
            for f in out_dir.glob("*.crc"):
                try:
                    f.unlink()

                except:
                    pass

            return str(final_name) if part_files else gold_out

        ###########################
        # MODO CLOUD: BIG-QUERY   #
        ###########################
        # Escribir outputs a Gold en GCP
        full_table = self.io.write_bigquery(mart_bq, bq_project, bq_dataset, bq_table, bq_gcs_bucket, mode_bq)

        return full_table

############################
#        ETL Pipeline      #
############################
class NewRiskMonitorPipeline:
    def __init__(self, io: IOAdapter):
        self.bronze = BronzeLayer(io)
        self.silver = SilverLayer(io)
        self.gold   = GoldLayer(io)
        
    def run(self, cfg: ETLConfig):
        df_events_raw, df_ref_raw = self.bronze.run(cfg.events_input, cfg.reference_input, cfg.bronze_events_out, cfg.bronze_country_risk_out)                               # BRONZE
        df_silver                 = self.silver.run(df_events_raw, df_ref_raw, cfg.silver_out, cfg.mode_silver, cfg.ingestion_date)                                          # SILVER
        full_table                = self.gold.run(df_silver, cfg.bq_project, cfg.bq_dataset, cfg.bq_table, cfg.bq_gcs_bucket, cfg.mode_bq, cfg.gold_out, cfg.ingestion_date) # GOLD
        
        print("Silver out:", cfg.silver_out)
        print("Gold out  :", full_table)
        print("PROCESO DE ETL COMPLETADO!")
        
def main():
    cfg   = parse_args()
    spark = build_spark(cfg.app_name)

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.debug.maxToStringFields", "1000")
    
    io = IOAdapter(spark)
    
    pipeline = NewRiskMonitorPipeline(io)
    pipeline.run(cfg)
    
    spark.stop()
    
if __name__ == "__main__":
    main()
    