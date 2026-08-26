# =========================================================
# JOB: job_fato_metricas_operacionais_gold.py
# PIPELINE: Silver → Gold
# FATO: fato_metricas_operacionais
# GLUE VERSION: 4.0
# WORKER TYPE: G.1X / 2 workers
# =========================================================
# DEPENDÊNCIAS: dim_fila, dim_data
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

print(f"[INFO] Job iniciado | fato_metricas_operacionais | ENV: {ENV}")

SILVER_TABLE = "db_silver.metricas_operacionais"
DIM_FILA     = "db_gold.dim_fila"
DIM_DATA     = "db_gold.dim_data"
GOLD_PATH    = f"s3://{BUCKET}/gold/fatos/fato_metricas_operacionais/"
GOLD_TABLE   = "db_gold.fato_metricas_operacionais"

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

df_metricas = spark.table(f"glue_catalog.{SILVER_TABLE}").filter(F.col("op_cdc") != "DELETE")
df_dim_fila = spark.table(f"glue_catalog.{DIM_FILA}").select("sk_fila", "nk_fila")
df_dim_data = spark.table(f"glue_catalog.{DIM_DATA}").select("sk_data", "dt_completa")

df_fato = (
    df_metricas

    # sk_fila
    .join(df_dim_fila.withColumnRenamed("sk_fila", "_sk_fila")
                     .withColumnRenamed("nk_fila", "_nk_fila"),
          df_metricas["id_fila"] == F.col("_nk_fila"), how="left")

    # sk_data
    .join(df_dim_data.withColumnRenamed("sk_data",    "_sk_data")
                     .withColumnRenamed("dt_completa", "_dt"),
          F.to_date(df_metricas["dt_referencia"]) == F.col("_dt"), how="left")

    .withColumn("sk_fila",
        F.coalesce(F.col("_sk_fila"), F.lit(-1).cast("int")))
    .withColumn("sk_data",
        F.coalesce(F.col("_sk_data"), F.lit(-1).cast("int")))

    .withColumn("sk_metrica",
        F.monotonically_increasing_id())

    .withColumn("dt_ingestao_gold", F.lit(now_ts).cast(TimestampType()))

    .select(
        "sk_metrica",
        F.col("id_metrica").alias("nk_metrica"),
        "sk_fila",
        "sk_data",
        (F.col("nr_chamadas_atendidas") + F.col("nr_chamadas_abandonadas")).alias("nr_chamadas_recebidas"),
        F.col("nr_chamadas_atendidas"),
        F.col("nr_chamadas_abandonadas"),
        F.col("nr_tma_segundos"),
        F.col("nr_tma_minutos"),
        F.col("nr_tme_segundos"),
        F.col("nr_tme_minutos"),
        F.col("nr_nivel_servico"),
        F.col("nr_taxa_atendimento"),
        F.col("nr_taxa_abandono"),
        F.col("fl_meta_nivel_servico"),
        F.col("fl_alto_abandono"),
        "dt_ingestao_gold",
    )
)

print(f"[INFO] Registros fato_metricas_operacionais: {df_fato.count()}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{GOLD_TABLE} (
        sk_metrica                  BIGINT,
        nk_metrica                  BIGINT,
        sk_fila                     INT,
        sk_data                     INT,
        nr_chamadas_recebidas       INT,
        nr_chamadas_atendidas       INT,
        nr_chamadas_abandonadas     INT,
        nr_tma_segundos             INT,
        nr_tma_minutos              DOUBLE,
        nr_tme_segundos             INT,
        nr_tme_minutos              DOUBLE,
        nr_nivel_servico            DOUBLE,
        nr_taxa_atendimento         DOUBLE,
        nr_taxa_abandono            DOUBLE,
        fl_meta_nivel_servico       SMALLINT,
        fl_alto_abandono            SMALLINT,
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

df_fato.createOrReplaceTempView("stg_fato_metricas_operacionais")

spark.sql(f"""
    MERGE INTO glue_catalog.{GOLD_TABLE} AS target
    USING stg_fato_metricas_operacionais AS source
    ON target.nk_metrica = source.nk_metrica
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Gold: {GOLD_TABLE}")
job.commit()
print("[INFO] Job finalizado com sucesso.")