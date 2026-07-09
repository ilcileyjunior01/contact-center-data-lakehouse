# =========================================================
# JOB: job_dim_status_chamada_gold.py
# PIPELINE: Silver → Gold
# DIMENSÃO: dim_status_chamada
# GLUE VERSION: 4.0
# WORKER TYPE: G.1X / 2 workers
# =========================================================
# OBSERVAÇÃO:
#   Derivada dos valores distintos de st_chamada presentes
#   na Silver. Inclui registro padrão para valores nulos
#   ou desconhecidos (sk_status = -1).
# =========================================================

import sys
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, TimestampType, ShortType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME", "ENV"])
JOB_NAME = args["JOB_NAME"]
BUCKET   = args["BUCKET_NAME"]
ENV      = args["ENV"]

sc           = SparkContext()
glue_context = GlueContext(sc)
spark        = glue_context.spark_session
job          = Job(glue_context)
job.init(JOB_NAME, args)

print(f"[INFO] Job iniciado | dim_status_chamada | ENV: {ENV}")

SILVER_TABLE = "db_silver.chamada"
GOLD_PATH    = f"s3://{BUCKET}/gold/dimensoes/dim_status_chamada/"
GOLD_TABLE   = "db_gold.dim_status_chamada"

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

df_status = (
    df_silver
    .select(F.col("st_chamada").alias("ds_status"))
    .filter(F.col("ds_status").isNotNull())
    .distinct()
    .withColumn("ds_status", F.upper(F.trim(F.col("ds_status"))))
)

# Gera surrogate key sequencial
window_sk = Window.orderBy("ds_status")

df_dim = (
    df_status
    .withColumn("sk_status",
        F.row_number().over(window_sk).cast("int"))

    .withColumn("fl_chamada_concluida",
        F.when(F.col("ds_status") == "ATENDIDA",    F.lit(1))
         .when(F.col("ds_status") == "CONCLUIDA",   F.lit(1))
         .otherwise(F.lit(0)).cast("smallint"))

    .withColumn("fl_chamada_abandonada",
        F.when(F.col("ds_status").isin("ABANDONADA", "ABANDONO"), F.lit(1))
         .otherwise(F.lit(0)).cast("smallint"))

    .withColumn("dt_ingestao_gold", F.lit(now_ts).cast(TimestampType()))
)

# Adiciona registro padrão para nulos / desconhecidos
df_desconhecido = spark.createDataFrame(
    [(-1, "DESCONHECIDO", 0, 0, now_ts)],
    ["sk_status", "ds_status", "fl_chamada_concluida",
     "fl_chamada_abandonada", "dt_ingestao_gold"]
).withColumn("dt_ingestao_gold", F.col("dt_ingestao_gold").cast(TimestampType())) \
 .withColumn("fl_chamada_concluida",  F.col("fl_chamada_concluida").cast("smallint")) \
 .withColumn("fl_chamada_abandonada", F.col("fl_chamada_abandonada").cast("smallint"))

df_final = df_dim.unionByName(df_desconhecido)

print(f"[INFO] Status gerados: {df_final.count()}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{GOLD_TABLE} (
        sk_status               INT,
        ds_status               STRING,
        fl_chamada_concluida    SMALLINT,
        fl_chamada_abandonada   SMALLINT,
        dt_ingestao_gold        TIMESTAMP
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

df_final.createOrReplaceTempView("stg_dim_status_chamada")

spark.sql(f"""
    MERGE INTO glue_catalog.{GOLD_TABLE} AS target
    USING stg_dim_status_chamada AS source
    ON target.sk_status = source.sk_status
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Gold: {GOLD_TABLE}")
job.commit()
print("[INFO] Job finalizado com sucesso.")