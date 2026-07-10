# =========================================================
# JOB: job_fato_discagem_gold.py
# PIPELINE: Silver → Gold
# FATO: fato_discagem
# GLUE VERSION: 4.0
# WORKER TYPE: G.1X / 2 workers
# =========================================================
# DEPENDÊNCIAS: dim_campanha, dim_cliente, dim_data
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

print(f"[INFO] Job iniciado | fato_discagem | ENV: {ENV}")

SILVER_TABLE  = "db_silver.discagem"
DIM_CAMPANHA  = "db_gold.dim_campanha"
DIM_CLIENTE   = "db_gold.dim_cliente"
DIM_DATA      = "db_gold.dim_data"
GOLD_PATH     = f"s3://{BUCKET}/gold/fatos/fato_discagem/"
GOLD_TABLE    = "db_gold.fato_discagem"

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

df_discagem   = spark.table(f"glue_catalog.{SILVER_TABLE}").filter(F.col("op_cdc") != "DELETE")
df_dim_camp   = spark.table(f"glue_catalog.{DIM_CAMPANHA}").select("sk_campanha", "nk_campanha")
df_dim_cli    = spark.table(f"glue_catalog.{DIM_CLIENTE}").select("sk_cliente", "nk_cliente")
df_dim_data   = spark.table(f"glue_catalog.{DIM_DATA}").select("sk_data", "dt_completa")

df_fato = (
    df_discagem

    # sk_campanha
    .join(df_dim_camp.withColumnRenamed("sk_campanha", "_sk_camp")
                     .withColumnRenamed("nk_campanha", "_nk_camp"),
          df_discagem["id_campanha"] == F.col("_nk_camp"), how="left")

    # sk_cliente
    .join(df_dim_cli.withColumnRenamed("sk_cliente", "_sk_cli")
                    .withColumnRenamed("nk_cliente", "_nk_cli"),
          df_discagem["id_cliente"] == F.col("_nk_cli"), how="left")

    # sk_data
    .join(df_dim_data.withColumnRenamed("sk_data",    "_sk_data")
                     .withColumnRenamed("dt_completa", "_dt"),
          F.to_date(df_discagem["dt_ultima_tentativa"]) == F.col("_dt"), how="left")

    .withColumn("sk_campanha",
        F.coalesce(F.col("_sk_camp"), F.lit(-1).cast("int")))
    .withColumn("sk_cliente",
        F.coalesce(F.col("_sk_cli"),  F.lit(-1).cast("int")))
    .withColumn("sk_data",
        F.coalesce(F.col("_sk_data"), F.lit(-1).cast("int")))

    .withColumn("sk_discagem",
        F.monotonically_increasing_id())

    .withColumn("dt_ingestao_gold", F.lit(now_ts).cast(TimestampType()))

    .select(
        "sk_discagem",
        F.col("id_discagem").alias("nk_discagem"),
        "sk_campanha",
        "sk_cliente",
        "sk_data",
        F.col("nr_telefone_mascarado"),
        F.col("st_discagem"),
        F.col("fl_discagem_atendida"),
        F.col("fl_discagem_nao_atendida"),
        "dt_ingestao_gold",
    )
)

print(f"[INFO] Registros fato_discagem: {df_fato.count()}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{GOLD_TABLE} (
        sk_discagem                 BIGINT,
        nk_discagem                 BIGINT,
        sk_campanha                 INT,
        sk_cliente                  INT,
        sk_data                     INT,
        nr_telefone_mascarado       STRING,
        st_discagem                 STRING,
        fl_discagem_atendida        SMALLINT,
        fl_discagem_nao_atendida    SMALLINT,
        dt_ingestao_gold            TIMESTAMP
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

df_fato.createOrReplaceTempView("stg_fato_discagem")

spark.sql(f"""
    MERGE INTO glue_catalog.{GOLD_TABLE} AS target
    USING stg_fato_discagem AS source
    ON target.nk_discagem = source.nk_discagem
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Gold: {GOLD_TABLE}")
job.commit()
print("[INFO] Job finalizado com sucesso.")