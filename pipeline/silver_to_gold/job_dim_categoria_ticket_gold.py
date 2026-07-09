# =========================================================
# JOB: job_dim_categoria_ticket_gold.py
# PIPELINE: Silver → Gold
# DIMENSÃO: dim_categoria_ticket
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

print(f"[INFO] Job iniciado | dim_categoria_ticket | ENV: {ENV}")

SILVER_TABLE = "db_silver.ticket"
GOLD_PATH    = f"s3://{BUCKET}/gold/dimensoes/dim_categoria_ticket/"
GOLD_TABLE   = "db_gold.dim_categoria_ticket"

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

window_sk = Window.orderBy("nm_categoria")

df_dim = (
    df_silver
    .select(F.col("ds_categoria").alias("nm_categoria"))
    .filter(F.col("nm_categoria").isNotNull())
    .distinct()
    .withColumn("nm_categoria", F.upper(F.trim(F.col("nm_categoria"))))
    .withColumn("sk_categoria",
        F.row_number().over(window_sk).cast("int"))
    .withColumn("dt_ingestao_gold", F.lit(now_ts).cast(TimestampType()))
)

df_desconhecido = spark.createDataFrame(
    [(-1, "DESCONHECIDO", now_ts)],
    ["sk_categoria", "nm_categoria", "dt_ingestao_gold"]
).withColumn("dt_ingestao_gold", F.col("dt_ingestao_gold").cast(TimestampType()))

df_final = df_dim.unionByName(df_desconhecido)

print(f"[INFO] Categorias geradas: {df_final.count()}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{GOLD_TABLE} (
        sk_categoria        INT,
        nm_categoria        STRING,
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

df_final.createOrReplaceTempView("stg_dim_categoria_ticket")

spark.sql(f"""
    MERGE INTO glue_catalog.{GOLD_TABLE} AS target
    USING stg_dim_categoria_ticket AS source
    ON target.sk_categoria = source.sk_categoria
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Gold: {GOLD_TABLE}")
job.commit()
print("[INFO] Job finalizado com sucesso.")