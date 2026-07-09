# =========================================================
# JOB: job_dim_campanha_gold.py
# PIPELINE: Silver → Gold
# DIMENSÃO: dim_campanha
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

print(f"[INFO] Job iniciado | dim_campanha | ENV: {ENV}")

SILVER_TABLE = "db_silver.campanha"
GOLD_PATH    = f"s3://{BUCKET}/gold/dimensoes/dim_campanha/"
GOLD_TABLE   = "db_gold.dim_campanha"

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

window_sk = Window.orderBy("nk_campanha")

df_dim = (
    df_silver
    .filter(F.col("op_cdc") != "DELETE")
    .select(
        F.col("id_campanha").alias("nk_campanha"),
        F.col("nm_campanha"),
        F.col("dt_inicio"),
        F.col("dt_fim"),
        F.col("st_campanha"),
        F.col("nr_duracao_dias"),
        F.col("fl_campanha_ativa"),
        F.col("fl_campanha_vigente"),
    )
    .withColumn("sk_campanha",
        F.row_number().over(window_sk).cast("int"))
    .withColumn("dt_ingestao_gold", F.lit(now_ts).cast(TimestampType()))
)

df_desconhecido = spark.createDataFrame(
    [(-1, -1, "DESCONHECIDO", None, None, "DESCONHECIDO", None, 0, 0, now_ts)],
    ["sk_campanha", "nk_campanha", "nm_campanha", "dt_inicio", "dt_fim",
     "st_campanha", "nr_duracao_dias", "fl_campanha_ativa",
     "fl_campanha_vigente", "dt_ingestao_gold"]
).withColumn("dt_ingestao_gold",    F.col("dt_ingestao_gold").cast(TimestampType())) \
 .withColumn("fl_campanha_ativa",   F.col("fl_campanha_ativa").cast("smallint")) \
 .withColumn("fl_campanha_vigente", F.col("fl_campanha_vigente").cast("smallint"))

df_final = df_dim.unionByName(df_desconhecido)

print(f"[INFO] Campanhas geradas: {df_final.count()}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{GOLD_TABLE} (
        sk_campanha             INT,
        nk_campanha             BIGINT,
        nm_campanha             STRING,
        dt_inicio               DATE,
        dt_fim                  DATE,
        st_campanha             STRING,
        nr_duracao_dias         INT,
        fl_campanha_ativa       SMALLINT,
        fl_campanha_vigente     SMALLINT,
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

df_final.createOrReplaceTempView("stg_dim_campanha")

spark.sql(f"""
    MERGE INTO glue_catalog.{GOLD_TABLE} AS target
    USING stg_dim_campanha AS source
    ON target.sk_campanha = source.sk_campanha
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Gold: {GOLD_TABLE}")
job.commit()
print("[INFO] Job finalizado com sucesso.")