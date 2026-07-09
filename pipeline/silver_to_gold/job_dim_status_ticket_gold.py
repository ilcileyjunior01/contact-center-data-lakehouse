# =========================================================
# JOB: job_dim_status_ticket_gold.py
# PIPELINE: Silver → Gold
# DIMENSÃO: dim_status_ticket
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

print(f"[INFO] Job iniciado | dim_status_ticket | ENV: {ENV}")

SILVER_TABLE = "db_silver.ticket"
GOLD_PATH    = f"s3://{BUCKET}/gold/dimensoes/dim_status_ticket/"
GOLD_TABLE   = "db_gold.dim_status_ticket"

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

window_sk = Window.orderBy("ds_status")

df_dim = (
    df_silver
    .select(F.col("st_ticket").alias("ds_status"))
    .filter(F.col("ds_status").isNotNull())
    .distinct()
    .withColumn("ds_status", F.upper(F.trim(F.col("ds_status"))))
    .withColumn("sk_status",
        F.row_number().over(window_sk).cast("int"))

    .withColumn("fl_ticket_aberto",
        F.when(F.col("ds_status") == "ABERTO", F.lit(1))
         .otherwise(F.lit(0)).cast("smallint"))

    .withColumn("fl_ticket_resolvido",
        F.when(F.col("ds_status").isin("RESOLVIDO", "FECHADO"), F.lit(1))
         .otherwise(F.lit(0)).cast("smallint"))

    .withColumn("fl_ticket_cancelado",
        F.when(F.col("ds_status") == "CANCELADO", F.lit(1))
         .otherwise(F.lit(0)).cast("smallint"))

    .withColumn("dt_ingestao_gold", F.lit(now_ts).cast(TimestampType()))
)

df_desconhecido = spark.createDataFrame(
    [(-1, "DESCONHECIDO", 0, 0, 0, now_ts)],
    ["sk_status", "ds_status", "fl_ticket_aberto",
     "fl_ticket_resolvido", "fl_ticket_cancelado", "dt_ingestao_gold"]
).withColumn("dt_ingestao_gold",     F.col("dt_ingestao_gold").cast(TimestampType())) \
 .withColumn("fl_ticket_aberto",     F.col("fl_ticket_aberto").cast("smallint")) \
 .withColumn("fl_ticket_resolvido",  F.col("fl_ticket_resolvido").cast("smallint")) \
 .withColumn("fl_ticket_cancelado",  F.col("fl_ticket_cancelado").cast("smallint"))

df_final = df_dim.unionByName(df_desconhecido)

print(f"[INFO] Status gerados: {df_final.count()}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{GOLD_TABLE} (
        sk_status               INT,
        ds_status               STRING,
        fl_ticket_aberto        SMALLINT,
        fl_ticket_resolvido     SMALLINT,
        fl_ticket_cancelado     SMALLINT,
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

df_final.createOrReplaceTempView("stg_dim_status_ticket")

spark.sql(f"""
    MERGE INTO glue_catalog.{GOLD_TABLE} AS target
    USING stg_dim_status_ticket AS source
    ON target.sk_status = source.sk_status
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Gold: {GOLD_TABLE}")
job.commit()
print("[INFO] Job finalizado com sucesso.")