# =========================================================
# JOB: job_fato_jornada_operador_gold.py
# PIPELINE: Silver → Gold
# FATO: fato_jornada_operador
# GLUE VERSION: 4.0
# WORKER TYPE: G.1X / 2 workers
# =========================================================
# DEPENDÊNCIAS: dim_operador, dim_data
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

print(f"[INFO] Job iniciado | fato_jornada_operador | ENV: {ENV}")

SILVER_TABLE = "db_silver.jornada_operador"
DIM_OPERADOR = "db_gold.dim_operador"
DIM_DATA     = "db_gold.dim_data"
GOLD_PATH    = f"s3://{BUCKET}/gold/fatos/fato_jornada_operador/"
GOLD_TABLE   = "db_gold.fato_jornada_operador"

spark.conf.set("spark.sql.catalog.glue_catalog",
    "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl",
    "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl",
    "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse",
    f"s3://{BUCKET}/gold/")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

df_jornada  = spark.table(f"glue_catalog.{SILVER_TABLE}").filter(F.col("op_cdc") != "DELETE")
df_dim_op   = spark.table(f"glue_catalog.{DIM_OPERADOR}").select("sk_operador", "nk_operador")
df_dim_data = spark.table(f"glue_catalog.{DIM_DATA}").select("sk_data", "dt_completa")

df_fato = (
    df_jornada

    # sk_operador
    .join(df_dim_op.withColumnRenamed("sk_operador", "_sk_op")
                   .withColumnRenamed("nk_operador", "_nk_op"),
          df_jornada["id_operador"] == F.col("_nk_op"), how="left")

    # sk_data_inicio
    .join(df_dim_data.withColumnRenamed("sk_data",    "_sk_dt_ini")
                     .withColumnRenamed("dt_completa", "_dt_ini"),
          F.to_date(df_jornada["dt_inicio_turno"]) == F.col("_dt_ini"), how

