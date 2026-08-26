# =========================================================
# JOB: job_fato_ura_navegacao_gold.py
# PIPELINE: Silver → Gold
# FATO: fato_ura_navegacao
# GLUE VERSION: 4.0
# WORKER TYPE: G.1X / 2 workers
# =========================================================
# DEPENDÊNCIAS: fato_chamada, dim_data
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

print(f"[INFO] Job iniciado | fato_ura_navegacao | ENV: {ENV}")

SILVER_TABLE = "db_silver.ura_navegacao"
FATO_CHAMADA = "db_gold.fato_chamada"
DIM_DATA     = "db_gold.dim_data"
GOLD_PATH    = f"s3://{BUCKET}/gold/fatos/fato_ura_navegacao/"
GOLD_TABLE   = "db_gold.fato_ura_navegacao"

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

df_ura       = spark.table(f"glue_catalog.{SILVER_TABLE}").filter(F.col("op_cdc") != "DELETE")
df_fato_cham = spark.table(f"glue_catalog.{FATO_CHAMADA}").select("sk_chamada", "nk_chamada")
df_dim_data  = spark.table(f"glue_catalog.{DIM_DATA}").select("sk_data", "dt_completa")

df_fato = (
    df_ura

    # sk_chamada (opcional — id_chamada pode ser null na Silver)
    .join(df_fato_cham.withColumnRenamed("sk_chamada", "_sk_chamada")
                      .withColumnRenamed("nk_chamada", "_nk_chamada"),
          df_ura["id_chamada"] == F.col("_nk_chamada"), how="left")

    # sk_data via dt_navegacao
    .join(df_dim_data.withColumnRenamed("sk_data",    "_sk_data")
                     .withColumnRenamed("dt_completa", "_dt"),
          F.to_date(df_ura["dt_navegacao"]) == F.col("_dt"), how="left")

    .withColumn("sk_chamada",
        F.coalesce(F.col("_sk_chamada"), F.lit(-1).cast("int")))
    .withColumn("sk_data",
        F.coalesce(F.col("_sk_data"),   F.lit(-1).cast("int")))

    .withColumn("sk_ura",
        F.monotonically_increasing_id())

    .withColumn("dt_ingestao_gold", F.lit(now_ts).cast(TimestampType()))

    .select(
        "sk_ura",
        F.col("id_ura").alias("nk_ura"),
        "sk_chamada",
        "sk_data",
        F.col("ds_opcao_selecionada"),
        F.col("nr_duracao_segundos"),
        F.col("fl_abandonou_ura"),
        F.col("ds_faixa_espera"),
        "dt_ingestao_gold",
    )
)

print(f"[INFO] Registros fato_ura_navegacao: {df_fato.count()}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{GOLD_TABLE} (
        sk_ura                  BIGINT,
        nk_ura                  BIGINT,
        sk_chamada              INT,
        sk_data                 INT,
        ds_opcao_selecionada    STRING,
        nr_duracao_segundos     INT,
        fl_abandonou_ura        SMALLINT,
        ds_faixa_espera         STRING,
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

df_fato.createOrReplaceTempView("stg_fato_ura_navegacao")

spark.sql(f"""
    MERGE INTO glue_catalog.{GOLD_TABLE} AS target
    USING stg_fato_ura_navegacao AS source
    ON target.nk_ura = source.nk_ura
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Gold: {GOLD_TABLE}")
job.commit()
print("[INFO] Job finalizado com sucesso.")

