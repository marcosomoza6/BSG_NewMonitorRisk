#!/usr/bin/env python3
import argparse
import os

from google.cloud import storage
from datetime     import datetime, timezone
from pyspark.sql  import SparkSession
from pyspark.sql  import functions as F
from pyspark.sql  import types as T

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

    return (SparkSession.builder.appName(app_name).getOrCreate())

def parse_args():
    p = argparse.ArgumentParser(description = "GDELT New Risk Monitor ETL (Landing/Bronze/Silver/Gold) - Spark/Dataproc")
    p.add_argument("--events_input"           , required = True, help = "Ruta eventos CSV/TSV")
    p.add_argument("--reference_input"        , required = True, help = "Ruta reference CSV")
    p.add_argument("--bronze_events_out"      , required = True, help = "Salida Bronze para events")
    p.add_argument("--bronze_country_risk_out", required = True, help = "Salida Bronze para country risk")
    p.add_argument("--silver_out"             , required = True, help = "Salida Silver parquet")
    p.add_argument("--bq_project"             , required = True, help = "GCP Project ID")
    p.add_argument("--bq_dataset"             , required = True, help = "BigQuery dataset (BSG_DS_NMR)")
    p.add_argument("--bq_table"               , required = True, help = "BigQuery table name (T_DW_BSG_GDELT_RISK_EVENTS)")
    p.add_argument("--bq_gcs_bucket"          , required = True, help = "Sección del Bucket para staging del connector de Big-Query")
    p.add_argument("--ingestion_date"         , required = True, help = "YYYY-MM-DD (para landing bronze)")
    
    p.add_argument("--app_name"   , default  = "NewRiskMonitor-ETL", help    = "Nombre del Script de Python")
    p.add_argument("--mode_silver", default  = "overwrite"         , choices = ["overwrite", "append"])
    p.add_argument("--mode_bq"    , default  = "overwrite"         , choices = ["overwrite", "append"])

    return p.parse_args()

def main():
    args  = parse_args()
    spark = build_spark(args.app_name)

    ingestion_ts   = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ingestion_date = args.ingestion_date

    # -----------------------------------------------------------------
    #                             BRONZE                              -
    # -----------------------------------------------------------------
    # Copiar raw tal cual (events + reference).                       -
    # Events: TSV (GDELT es tab-separated)                            -
    # Guardamos tal cual en Bronze (copiamos contenido a un folder).  -
    #                                                                 -
    # -----------------------------------------------------------------
    df_events_raw = (spark.read.option("sep", "\t").option("header", "false").csv(args.events_input))
    df_ref_raw    = (spark.read.option("header", "true").option("inferSchema", "true").csv(args.reference_input))

    (df_events_raw.coalesce(1).write.mode("overwrite").option("header", "false").option("sep", "\t").csv(args.bronze_events_out))
    (df_ref_raw.coalesce(1).write.mode("overwrite").option("header", "true").csv(args.bronze_country_risk_out))

    # -----------------------------------------------------------------
    #                             SILVER                              -
    # -----------------------------------------------------------------
    # Normalizar, tipar, DQ y enriquecer con reference.               -
    #                                                                 -
    # -----------------------------------------------------------------
    df = df_events_raw.select(F.col("_c0").cast(T.LongType()).alias("globaleventid")     ,
                              F.to_date(F.col("_c1"), "yyyyMMdd").alias("event_date")    ,
                              F.col("_c29").cast(T.IntegerType()).alias("quadclass")     ,
                              F.col("_c30").cast(T.DoubleType()).alias("goldstein_scale"),
                              F.col("_c50").alias("actiongeo_fullname")                  ,
                              F.col("_c51").alias("actiongeo_countrycode")               ,
                              F.col("_c52").alias("actiongeo_adm1code")                  ,
                              F.col("_c53").cast(T.DoubleType()).alias("actiongeo_lat")  ,
                              F.col("_c54").cast(T.DoubleType()).alias("actiongeo_long"))

    df = (df.withColumn("country"       , F.upper(F.col("actiongeo_countrycode")))
            .withColumn("city"          , F.when(F.col("actiongeo_fullname").isNull(), F.lit(None)).otherwise(F.trim(F.split(F.col("actiongeo_fullname"), ",").getItem(0))))
            .withColumn("ingestion_date", F.lit(ingestion_date))
            .withColumn("ingestion_ts"  , F.lit(ingestion_ts)))

    df = (df.filter((F.col("event_date").isNotNull())    &
                    (F.col("globaleventid").isNotNull()) &
                    (F.col("country").isNotNull())       &
                    (F.length(F.col("country")) == 2)).dropDuplicates(["globaleventid"]))

    df = df.withColumn("risk_weight", F.when(F.col("quadclass") == 1, F.lit(0.25))
                                       .when(F.col("quadclass") == 2, F.lit(0.50))
                                       .when(F.col("quadclass") == 3, F.lit(0.75))
                                       .when(F.col("quadclass") == 4, F.lit(1.00)).otherwise(F.lit(0.50)))

    df_ref = (df_ref_raw.select(F.upper(F.col("country")).alias("country")                                    , 
                                        F.col("baseline_risk").cast(T.DoubleType()).alias("baseline_risk")    ,                                    
                                        F.col("risk_multiplier").cast(T.DoubleType()).alias("risk_multiplier"),
                                        F.col("updated_at").cast(T.StringType()).alias("updated_at")).dropDuplicates(["country"]))

    df = (df.join(df_ref, on = "country", how = "left").withColumn("risk_weight_adj", F.col("risk_weight") * F.coalesce(F.col("risk_multiplier"), F.lit(1.0))))

    (df.write.mode(args.mode_silver).partitionBy("event_date").parquet(args.silver_out))

    # -----------------------------------------------------------------
    #                             GOLD                                -
    # -----------------------------------------------------------------
    # Mart en BigQuery (Connector).                                   -
    #                                                                 -
    # -----------------------------------------------------------------
    mart = (df.groupBy("event_date", "country", "city").agg(F.count("*").alias("events_count")              ,
                                                            F.sum("risk_weight_adj").alias("risk_score_raw"),
                                                            F.avg("goldstein_scale").alias("avg_goldstein")).withColumn("risk_score", F.round((F.col("risk_score_raw") / F.col("events_count")) * 100, 2)).drop("risk_score_raw"))

    mart_bq = (mart.withColumnRenamed("event_date"   , "DTE_EVENT")
                   .withColumnRenamed("country"      , "NAM_COUNTRY")
                   .withColumnRenamed("city"         , "NAM_CITY")
                   .withColumnRenamed("events_count" , "CNT_EVENTS")
                   .withColumnRenamed("risk_score"   , "NUM_RISK_SCORE")
                   .withColumnRenamed("avg_goldstein", "NUM_GOLDSTEIN_AVG"))

    full_table = f"{args.bq_project}:{args.bq_dataset}.{args.bq_table}"
    
    (mart_bq.write.format("bigquery").option("table"             , full_table)
                                     .option("temporaryGcsBucket", args.bq_gcs_bucket).mode(args.mode_bq).save())

    print("Silver out    :", args.silver_out)
    print("BigQuery table:", full_table)

    print("PROCESO DE ETL COMPLETADO!")

    spark.stop()

if __name__ == "__main__":
    main()
