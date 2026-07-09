# =========================================================
# JOB: job_dim_fila_gold.py
# PIPELINE: Silver → Gold
# DIMENSÃO: dim_fila
# GLUE VERSION: 4.0
# WORKER TYPE: G.1X / 2 workers
# =========================================================

import sys
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME", "ENV"])
JOB_NAME = args["JOB_NAME"]
BUCKET   = args["BUCKET_NAME"]
ENV      = args["ENV"]

sc           = SparkContext()
glue_context = GlueContext(sc)
spark        = glue_context.spark_session
job          = Job(glue_context)
job.init(JOB_NAME, args)

print(f"[INFO] Job iniciado | dim_fila | ENV: {ENV}")

SILVER_TABLE = "db_silver.fila_atendimento"
GOLD_PATH    = f"s3://{BUCKET}/gold/dimensoes/dim_fila/"
GOLD_TABLE   = "db_gold.dim_fila"

spark.conf.set("spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.glue_catalog",
    "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl",
    "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl",
    "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse",
    f"s3://{BUCKET}/gold/")

now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

df_silver = spark.table(f"glue_catalog.{SILVER_TABLE}")

window_sk = Window.orderBy("nk_fila")

df_dim = (
    df_silver
    .filter(F.col("op_cdc") != "DELETE")
    .select(
        F.col("id_fila").alias("nk_fila"),
        F.col("nm_fila"),
        F.col("ds_tipo_canal"),
        F.col("nr_sla_segundos"),
        F.col("nr_sla_minutos"),
    )
    .withColumn("sk_fila",
        F.row_number().over(window_sk).cast("int"))

    .withColumn("fl_fila_voz",
        F.when(F.col("ds_tipo_canal") == "TELEFONE", F.lit(1))
         .otherwise(F.lit(0)).cast("smallint"))

    .withColumn("fl_fila_digital",
        F.when(
            F.col("ds_tipo_canal").isin("CHAT", "WHATSAPP", "EMAIL"),
            F.lit(1)
        ).otherwise(F.lit(0)).cast("smallint"))

    .withColumn("dt_ingestao_gold", F.lit(now_ts).cast(TimestampType()))
)

df_desconhecido = spark.createDataFrame(
    [(-1, -1, "DESCONHECIDO", "DESCONHECIDO", 0, 0.0, 0, 0, now_ts)],
    ["sk_fila", "nk_fila", "nm_fila", "ds_tipo_canal",
     "nr_sla_segundos", "nr_sla_minutos",
     "fl_fila_voz", "fl_fila_digital", "dt_ingestao_gold"]
).withColumn("dt_ingestao_gold", F.col("dt_ingestao_gold").cast(TimestampType())) \
 .withColumn("fl_fila_voz",     F.col("fl_fila_voz").cast("smallint")) \
 .withColumn("fl_fila_digital", F.col("fl_fila_digital").cast("smallint"))

df_final = df_dim.unionByName(df_desconhecido)

print(f"[INFO] Filas geradas: {df_final.count()}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{GOLD_TABLE} (
        sk_fila             INT,
        nk_fila             BIGINT,
        nm_fila             STRING,
        ds_tipo_canal       STRING,
        nr_sla_segundos     INT,
        nr_sla_minutos      DOUBLE,
        fl_fila_voz         SMALLINT,
        fl_fila_digital     SMALLINT,
        dt_ingestao_gold    TIMESTAMP
    )
    USING iceberg
    LOCATION '{GOLD_PATH}'
    TBLPROPERTIES (
        'format-version'                  = '2',
        'write.format.default'            = 'parquet',
        'write.parquet.compression-codec' = 'snappy',
        'write.target-file-size-bytes'    = '134217728'
    )
""")

df_final.createOrReplaceTempView("stg_dim_fila")

spark.sql(f"""
    MERGE INTO glue_catalog.{GOLD_TABLE} AS target
    USING stg_dim_fila AS source
    ON target.sk_fila = source.sk_fila
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Gold: {GOLD_TABLE}")
job.commit()
print("[INFO] Job finalizado com sucesso.")