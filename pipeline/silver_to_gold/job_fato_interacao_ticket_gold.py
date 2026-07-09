# =========================================================
# JOB: job_fato_interacao_ticket_gold.py
# PIPELINE: Silver → Gold
# FATO: fato_interacao_ticket
# GLUE VERSION: 4.0
# WORKER TYPE: G.1X / 2 workers
# =========================================================
# DEPENDÊNCIAS: fato_ticket, dim_operador, dim_data
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

print(f"[INFO] Job iniciado | fato_interacao_ticket | ENV: {ENV}")

SILVER_TABLE = "db_silver.interacao_ticket"
FATO_TICKET  = "db_gold.fato_ticket"
DIM_OPERADOR = "db_gold.dim_operador"
DIM_DATA     = "db_gold.dim_data"
GOLD_PATH    = f"s3://{BUCKET}/gold/fatos/fato_interacao_ticket/"
GOLD_TABLE   = "db_gold.fato_interacao_ticket"

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
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

df_interacao = spark.table(f"glue_catalog.{SILVER_TABLE}").filter(F.col("op_cdc") != "DELETE")
df_fato_tick = spark.table(f"glue_catalog.{FATO_TICKET}").select("sk_ticket", "nk_ticket")
df_dim_op    = spark.table(f"glue_catalog.{DIM_OPERADOR}").select("sk_operador", "nk_operador")
df_dim_data  = spark.table(f"glue_catalog.{DIM_DATA}").select("sk_data", "dt_completa")

df_fato = (
    df_interacao

    .join(df_fato_tick.withColumnRenamed("sk_ticket", "_sk_ticket")
                      .withColumnRenamed("nk_ticket", "_nk_ticket"),
          df_interacao["id_ticket"] == F.col("_nk_ticket"), how="left")

    .join(df_dim_op.withColumnRenamed("sk_operador", "_sk_op")
                   .withColumnRenamed("nk_operador", "_nk_op"),
          df_interacao["id_operador"] == F.col("_nk_op"), how="left")

    .join(df_dim_data.withColumnRenamed("sk_data", "_sk_data")
                     .withColumnRenamed("dt_completa", "_dt"),
          F.to_date(df_interacao["dt_interacao"]) == F.col("_dt"), how="left")

    .withColumn("sk_ticket",
        F.coalesce(F.col("_sk_ticket"), F.lit(-1).cast("int")))
    .withColumn("sk_operador",
        F.coalesce(F.col("_sk_op"),     F.lit(-1).cast("int")))
    .withColumn("sk_data",
        F.coalesce(F.col("_sk_data"),   F.lit(-1).cast("int")))

    .withColumn("sk_interacao",
        F.row_number().over(Window.orderBy("id_interacao")).cast("int"))

    .withColumn("dt_ingestao_gold", F.lit(now_ts).cast(TimestampType()))

    .select(
        "sk_interacao",
        F.col("id_interacao").alias("nk_interacao"),
        "sk_ticket",
        "sk_operador",
        "sk_data",
        F.col("ds_canal"),
        F.col("nr_tamanho_observacao_chars"),
        F.col("fl_tem_observacao"),
        "dt_ingestao_gold",
    )
)

print(f"[INFO] Registros fato_interacao_ticket: {df_fato.count()}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{GOLD_TABLE} (
        sk_interacao                    INT,
        nk_interacao                    BIGINT,
        sk_ticket                       INT,
        sk_operador                     INT,
        sk_data                         INT,
        ds_canal                        STRING,
        nr_tamanho_observacao_chars     INT,
        fl_tem_observacao               SMALLINT,
        dt_ingestao_gold                TIMESTAMP
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

df_fato.createOrReplaceTempView("stg_fato_interacao_ticket")

spark.sql(f"""
    MERGE INTO glue_catalog.{GOLD_TABLE} AS target
    USING stg_fato_interacao_ticket AS source
    ON target.nk_interacao = source.nk_interacao
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Gold: {GOLD_TABLE}")
job.commit()
print("[INFO] Job finalizado com sucesso.")