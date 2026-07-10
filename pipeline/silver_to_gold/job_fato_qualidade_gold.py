# =========================================================
# JOB: job_fato_qualidade_gold.py
# PIPELINE: Silver → Gold
# FATO: fato_qualidade
# GLUE VERSION: 4.0
# WORKER TYPE: G.1X / 2 workers
# =========================================================
# DEPENDÊNCIAS:
#   fato_chamada, dim_operador, dim_data
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

print(f"[INFO] Job iniciado | fato_qualidade | ENV: {ENV}")

SILVER_TABLE  = "db_silver.avaliacao_qualidade"
FATO_CHAMADA  = "db_gold.fato_chamada"
DIM_OPERADOR  = "db_gold.dim_operador"
DIM_DATA      = "db_gold.dim_data"
GOLD_PATH     = f"s3://{BUCKET}/gold/fatos/fato_qualidade/"
GOLD_TABLE    = "db_gold.fato_qualidade"

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

df_aval       = spark.table(f"glue_catalog.{SILVER_TABLE}").filter(F.col("op_cdc") != "DELETE")
df_fato_cham  = spark.table(f"glue_catalog.{FATO_CHAMADA}").select("sk_chamada", "nk_chamada", "sk_operador")
df_dim_op     = spark.table(f"glue_catalog.{DIM_OPERADOR}").select("sk_operador", "nk_operador")
df_dim_data   = spark.table(f"glue_catalog.{DIM_DATA}").select("sk_data", "dt_completa")

df_fato = (
    df_aval

    # sk_chamada e sk_operador_avaliado via fato_chamada
    .join(df_fato_cham.withColumnRenamed("sk_chamada",  "_sk_chamada")
                      .withColumnRenamed("nk_chamada",  "_nk_chamada")
                      .withColumnRenamed("sk_operador", "_sk_op_avaliado"),
          df_aval["id_chamada"] == F.col("_nk_chamada"), how="left")

    # sk_avaliador via dim_operador
    .join(df_dim_op.withColumnRenamed("sk_operador", "_sk_avaliador")
                   .withColumnRenamed("nk_operador", "_nk_avaliador"),
          df_aval["id_supervisor"] == F.col("_nk_avaliador"), how="left")

    # sk_data
    .join(df_dim_data.withColumnRenamed("sk_data",    "_sk_data")
                     .withColumnRenamed("dt_completa", "_dt"),
          F.to_date(df_aval["dt_avaliacao"]) == F.col("_dt"), how="left")

    .withColumn("sk_chamada",
        F.coalesce(F.col("_sk_chamada"),    F.lit(-1).cast("int")))
    .withColumn("sk_operador_avaliado",
        F.coalesce(F.col("_sk_op_avaliado"), F.lit(-1).cast("int")))
    .withColumn("sk_avaliador",
        F.coalesce(F.col("_sk_avaliador"),  F.lit(-1).cast("int")))
    .withColumn("sk_data",
        F.coalesce(F.col("_sk_data"),       F.lit(-1).cast("int")))

    .withColumn("sk_avaliacao",
        F.monotonically_increasing_id())

    .withColumn("dt_ingestao_gold", F.lit(now_ts).cast(TimestampType()))

    .select(
        "sk_avaliacao",
        F.col("id_avaliacao").alias("nk_avaliacao"),
        "sk_chamada",
        "sk_operador_avaliado",
        "sk_avaliador",
        "sk_data",
        F.col("nr_nota_geral").alias("nr_nota"),
        F.col("ds_faixa_nota"),
        F.col("fl_aprovado"),
        F.col("fl_critico"),
        F.col("fl_tem_feedback"),
        F.col("nr_tamanho_feedback_chars"),
        "dt_ingestao_gold",
    )
)

print(f"[INFO] Registros fato_qualidade: {df_fato.count()}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{GOLD_TABLE} (
        sk_avaliacao                BIGINT,
        nk_avaliacao                BIGINT,
        sk_chamada                  INT,
        sk_operador_avaliado        INT,
        sk_avaliador                INT,
        sk_data                     INT,
        nr_nota                     DOUBLE,
        ds_faixa_nota               STRING,
        fl_aprovado                 SMALLINT,
        fl_critico                  SMALLINT,
        fl_tem_feedback             SMALLINT,
        nr_tamanho_feedback_chars   INT,
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

df_fato.createOrReplaceTempView("stg_fato_qualidade")

spark.sql(f"""
    MERGE INTO glue_catalog.{GOLD_TABLE} AS target
    USING stg_fato_qualidade AS source
    ON target.nk_avaliacao = source.nk_avaliacao
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Gold: {GOLD_TABLE}")
job.commit()
print("[INFO] Job finalizado com sucesso.")