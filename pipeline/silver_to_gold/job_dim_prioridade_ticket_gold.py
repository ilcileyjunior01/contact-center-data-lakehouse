# =========================================================
# JOB: job_dim_prioridade_ticket_gold.py
# PIPELINE: Silver → Gold
# DIMENSÃO: dim_prioridade_ticket
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

print(f"[INFO] Job iniciado | dim_prioridade_ticket | ENV: {ENV}")

SILVER_TABLE = "db_silver.ticket"
GOLD_PATH    = f"s3://{BUCKET}/gold/dimensoes/dim_prioridade_ticket/"
GOLD_TABLE   = "db_gold.dim_prioridade_ticket"

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

window_sk = Window.orderBy("nm_prioridade")

df_dim = (
    df_silver
    .select(F.col("ds_prioridade").alias("nm_prioridade"))
    .filter(F.col("nm_prioridade").isNotNull())
    .distinct()
    .withColumn("nm_prioridade", F.upper(F.trim(F.col("nm_prioridade"))))
    .withColumn("sk_prioridade",
        F.row_number().over(window_sk).cast("int"))

    .withColumn("nr_ordem_prioridade",
        F.when(F.col("nm_prioridade") == "CRITICA",  F.lit(1))
         .when(F.col("nm_prioridade") == "ALTA",     F.lit(2))
         .when(F.col("nm_prioridade") == "MEDIA",    F.lit(3))
         .when(F.col("nm_prioridade") == "BAIXA",    F.lit(4))
         .otherwise(F.lit(99)).cast("int"))

    .withColumn("fl_prioridade_critica",
        F.when(F.col("nm_prioridade") == "CRITICA", F.lit(1))
         .otherwise(F.lit(0)).cast("smallint"))

    .withColumn("dt_ingestao_gold", F.lit(now_ts).cast(TimestampType()))
)

df_desconhecido = spark.createDataFrame(
    [(-1, "DESCONHECIDO", 99, 0, now_ts)],
    ["sk_prioridade", "nm_prioridade", "nr_ordem_prioridade",
     "fl_prioridade_critica", "dt_ingestao_gold"]
).withColumn("dt_ingestao_gold",      F.col("dt_ingestao_gold").cast(TimestampType())) \
 .withColumn("fl_prioridade_critica", F.col("fl_prioridade_critica").cast("smallint"))

df_final = df_dim.unionByName(df_desconhecido)

print(f"[INFO] Prioridades geradas: {df_final.count()}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{GOLD_TABLE} (
        sk_prioridade           INT,
        nm_prioridade           STRING,
        nr_ordem_prioridade     INT,
        fl_prioridade_critica   SMALLINT,
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

df_final.createOrReplaceTempView("stg_dim_prioridade_ticket")

spark.sql(f"""
    MERGE INTO glue_catalog.{GOLD_TABLE} AS target
    USING stg_dim_prioridade_ticket AS source
    ON target.sk_prioridade = source.sk_prioridade
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Gold: {GOLD_TABLE}")
job.commit()
print("[INFO] Job finalizado com sucesso.")