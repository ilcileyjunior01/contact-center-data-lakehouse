# =========================================================
# JOB: job_fato_mensagem_chat_gold.py
# PIPELINE: Silver → Gold
# FATO: fato_mensagem_chat
# GLUE VERSION: 4.0
# WORKER TYPE: G.1X / 2 workers
# =========================================================
# DEPENDÊNCIAS: fato_chat, dim_data
# =========================================================

import sys
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
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

print(f"[INFO] Job iniciado | fato_mensagem_chat | ENV: {ENV}")

SILVER_TABLE = "db_silver.mensagem_chat"
FATO_CHAT    = "db_gold.fato_chat"
DIM_DATA     = "db_gold.dim_data"
GOLD_PATH    = f"s3://{BUCKET}/gold/fatos/fato_mensagem_chat/"
GOLD_TABLE   = "db_gold.fato_mensagem_chat"

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

df_mensagem  = spark.table(f"glue_catalog.{SILVER_TABLE}").filter(F.col("op_cdc") != "DELETE")
df_fato_chat = spark.table(f"glue_catalog.{FATO_CHAT}").select("sk_chat", "nk_chat")
df_dim_data  = spark.table(f"glue_catalog.{DIM_DATA}").select("sk_data", "dt_completa")

df_fato = (
    df_mensagem

    .join(df_fato_chat.withColumnRenamed("sk_chat", "_sk_chat")
                      .withColumnRenamed("nk_chat", "_nk_chat"),
          df_mensagem["id_chat"] == F.col("_nk_chat"), how="left")

    .join(df_dim_data.withColumnRenamed("sk_data", "_sk_data")
                     .withColumnRenamed("dt_completa", "_dt"),
          F.to_date(df_mensagem["dt_mensagem"]) == F.col("_dt"), how="left")

    .withColumn("sk_chat",
        F.coalesce(F.col("_sk_chat"),  F.lit(-1).cast("long")))
    .withColumn("sk_data",
        F.coalesce(F.col("_sk_data"),  F.lit(-1).cast("int")))

    .withColumn("sk_mensagem",
        F.monotonically_increasing_id())

    .withColumn("dt_ingestao_gold", F.lit(now_ts).cast(TimestampType()))

    .select(
        "sk_mensagem",
        F.col("id_mensagem").alias("nk_mensagem"),
        "sk_chat",
        "sk_data",
        F.col("ds_remetente"),
        F.col("nr_tamanho_chars"),
        F.col("fl_mensagem_cliente"),
        F.col("fl_mensagem_operador"),
        "dt_ingestao_gold",
    )
)

print(f"[INFO] Registros fato_mensagem_chat: {df_fato.count()}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{GOLD_TABLE} (
        sk_mensagem             BIGINT,
        nk_mensagem             BIGINT,
        sk_chat                 BIGINT,
        sk_data                 INT,
        ds_remetente            STRING,
        nr_tamanho_chars        INT,
        fl_mensagem_cliente     SMALLINT,
        fl_mensagem_operador    SMALLINT,
        dt_ingestao_gold        TIMESTAMP
    )
    USING iceberg
    LOCATION '{GOLD_PATH}'
    PARTITIONED BY (sk_data)
    TBLPROPERTIES (
        'format-version'                  = '2',
        'write.format.default'            = 'parquet',
        'write.parquet.compression-codec' = 'snappy',
        'write.target-file-size-bytes'    = '134217728'
    )
""")

df_fato.createOrReplaceTempView("stg_fato_mensagem_chat")

spark.sql(f"""
    MERGE INTO glue_catalog.{GOLD_TABLE} AS target
    USING stg_fato_mensagem_chat AS source
    ON target.nk_mensagem = source.nk_mensagem
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Gold: {GOLD_TABLE}")
job.commit()
print("[INFO] Job finalizado com sucesso.")