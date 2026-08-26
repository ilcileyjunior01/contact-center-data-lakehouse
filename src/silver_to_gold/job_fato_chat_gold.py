# =========================================================
# JOB: job_fato_chat_gold.py
# PIPELINE: Silver → Gold
# FATO: fato_chat
# GLUE VERSION: 4.0
# WORKER TYPE: G.1X / 2 workers
# =========================================================
# DEPENDÊNCIAS:
#   dim_data, dim_cliente, dim_operador, dim_canal
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

print(f"[INFO] Job iniciado | fato_chat | ENV: {ENV}")

SILVER_TABLE = "db_silver.chat"
DIM_CLIENTE  = "db_gold.dim_cliente"
DIM_OPERADOR = "db_gold.dim_operador"
DIM_CANAL    = "db_gold.dim_canal"
DIM_DATA     = "db_gold.dim_data"
GOLD_PATH    = f"s3://{BUCKET}/gold/fatos/fato_chat/"
GOLD_TABLE   = "db_gold.fato_chat"

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

df_chat      = spark.table(f"glue_catalog.{SILVER_TABLE}").filter(F.col("op_cdc") != "DELETE")
df_dim_cli   = spark.table(f"glue_catalog.{DIM_CLIENTE}").select("sk_cliente", "nk_cliente")
df_dim_op    = spark.table(f"glue_catalog.{DIM_OPERADOR}").select("sk_operador", "nk_operador")
df_dim_canal = spark.table(f"glue_catalog.{DIM_CANAL}").select("sk_canal", "nm_canal")
df_dim_data  = spark.table(f"glue_catalog.{DIM_DATA}").select("sk_data", "dt_completa")

# sk_canal fixo para CHAT = 2 conforme dim_canal sintética
SK_CANAL_CHAT = 2

df_fato = (
    df_chat

    .join(df_dim_cli.withColumnRenamed("sk_cliente", "_sk_cli")
                    .withColumnRenamed("nk_cliente", "_nk_cli"),
          df_chat["id_cliente"] == F.col("_nk_cli"), how="left")

    .join(df_dim_op.withColumnRenamed("sk_operador", "_sk_op")
                   .withColumnRenamed("nk_operador", "_nk_op"),
          df_chat["id_operador"] == F.col("_nk_op"), how="left")

    .join(df_dim_data.withColumnRenamed("sk_data", "_sk_dt_ini")
                     .withColumnRenamed("dt_completa", "_dt_ini"),
          F.to_date(df_chat["dt_inicio"]) == F.col("_dt_ini"), how="left")

    .join(df_dim_data.withColumnRenamed("sk_data", "_sk_dt_fim")
                     .withColumnRenamed("dt_completa", "_dt_fim"),
          F.to_date(df_chat["dt_fim"]) == F.col("_dt_fim"), how="left")

    .withColumn("sk_cliente",
        F.coalesce(F.col("_sk_cli"),    F.lit(-1).cast("int")))
    .withColumn("sk_operador",
        F.coalesce(F.col("_sk_op"),     F.lit(-1).cast("int")))
    .withColumn("sk_canal",
        F.lit(SK_CANAL_CHAT).cast("int"))
    .withColumn("sk_data_inicio",
        F.coalesce(F.col("_sk_dt_ini"), F.lit(-1).cast("int")))
    .withColumn("sk_data_fim",
        F.coalesce(F.col("_sk_dt_fim"), F.lit(-1).cast("int")))

    .withColumn("sk_chat",
        F.monotonically_increasing_id())

    .withColumn("dt_ingestao_gold", F.lit(now_ts).cast(TimestampType()))

    .select(
        "sk_chat",
        F.col("id_chat").alias("nk_chat"),
        "sk_cliente",
        "sk_operador",
        "sk_canal",
        "sk_data_inicio",
        "sk_data_fim",
        F.col("nr_duracao_segundos"),
        F.col("nr_duracao_minutos"),
        F.col("fl_chat_completo"),
        F.col("st_chat"),
        "dt_ingestao_gold",
    )
)

print(f"[INFO] Registros fato_chat: {df_fato.count()}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{GOLD_TABLE} (
        sk_chat             BIGINT,
        nk_chat             BIGINT,
        sk_cliente          INT,
        sk_operador         INT,
        sk_canal            INT,
        sk_data_inicio      INT,
        sk_data_fim         INT,
        nr_duracao_segundos INT,
        nr_duracao_minutos  DOUBLE,
        fl_chat_completo    SMALLINT,
        st_chat             STRING,
        dt_ingestao_gold    TIMESTAMP
    )
    USING iceberg
    LOCATION '{GOLD_PATH}'
    PARTITIONED BY (sk_data_inicio)
    TBLPROPERTIES (
        'format-version'                  = '2',
        'write.format.default'            = 'parquet',
        'write.parquet.compression-codec' = 'snappy',
        'write.target-file-size-bytes'    = '134217728'
    )
""")

df_fato.createOrReplaceTempView("stg_fato_chat")

spark.sql(f"""
    MERGE INTO glue_catalog.{GOLD_TABLE} AS target
    USING stg_fato_chat AS source
    ON target.nk_chat = source.nk_chat
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Gold: {GOLD_TABLE}")
job.commit()
print("[INFO] Job finalizado com sucesso.")