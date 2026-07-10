# =========================================================
# JOB: job_fato_chamada_gold.py
# PIPELINE: Silver → Gold
# FATO: fato_chamada
# GLUE VERSION: 4.0
# WORKER TYPE: G.1X / 2 workers
# =========================================================
# DEPENDÊNCIAS:
#   dim_data, dim_cliente, dim_operador,
#   dim_fila, dim_canal, dim_status_chamada
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

print(f"[INFO] Job iniciado | fato_chamada | ENV: {ENV}")

SILVER_CHAMADA = "db_silver.chamada"
DIM_CLIENTE    = "db_gold.dim_cliente"
DIM_OPERADOR   = "db_gold.dim_operador"
DIM_FILA       = "db_gold.dim_fila"
DIM_CANAL      = "db_gold.dim_canal"
DIM_STATUS     = "db_gold.dim_status_chamada"
DIM_DATA       = "db_gold.dim_data"
GOLD_PATH      = f"s3://{BUCKET}/gold/fatos/fato_chamada/"
GOLD_TABLE     = "db_gold.fato_chamada"

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

# =========================================================
# LEITURA DAS FONTES
# =========================================================

df_chamada = (
    spark.table(f"glue_catalog.{SILVER_CHAMADA}")
    .filter(F.col("op_cdc") != "DELETE")
)

df_dim_cliente  = spark.table(f"glue_catalog.{DIM_CLIENTE}") \
    .select("sk_cliente", "nk_cliente")
df_dim_operador = spark.table(f"glue_catalog.{DIM_OPERADOR}") \
    .select("sk_operador", "nk_operador")
df_dim_fila     = spark.table(f"glue_catalog.{DIM_FILA}") \
    .select("sk_fila", "nk_fila")
df_dim_canal    = spark.table(f"glue_catalog.{DIM_CANAL}") \
    .select("sk_canal", "nm_canal")
df_dim_status   = spark.table(f"glue_catalog.{DIM_STATUS}") \
    .select("sk_status", "ds_status")
df_dim_data     = spark.table(f"glue_catalog.{DIM_DATA}") \
    .select("sk_data", "dt_completa")

# =========================================================
# RESOLUÇÃO DAS SURROGATE KEYS
# =========================================================

# Mapeia canal pelo tipo de chamada
df_canal_map = df_dim_canal.withColumnRenamed("nm_canal", "_nm_canal") \
                            .withColumnRenamed("sk_canal", "_sk_canal")

df_fato = (
    df_chamada

    # sk_cliente
    .join(df_dim_cliente.withColumnRenamed("sk_cliente", "_sk_cliente")
                        .withColumnRenamed("nk_cliente", "_nk_cliente"),
          df_chamada["id_cliente"] == F.col("_nk_cliente"), how="left")

    # sk_operador
    .join(df_dim_operador.withColumnRenamed("sk_operador", "_sk_operador")
                         .withColumnRenamed("nk_operador", "_nk_operador"),
          df_chamada["id_operador"] == F.col("_nk_operador"), how="left")

    # sk_fila
    .join(df_dim_fila.withColumnRenamed("sk_fila", "_sk_fila")
                     .withColumnRenamed("nk_fila", "_nk_fila"),
          df_chamada["id_fila"] == F.col("_nk_fila"), how="left")

    # sk_status_chamada
    .join(df_dim_status.withColumnRenamed("sk_status", "_sk_status")
                       .withColumnRenamed("ds_status", "_ds_status"),
          F.upper(df_chamada["st_chamada"]) == F.col("_ds_status"), how="left")

    # sk_canal via tp_chamada (ENTRADA/SAIDA → TELEFONE)
    .join(df_dim_canal.withColumnRenamed("sk_canal", "_sk_canal")
                      .withColumnRenamed("nm_canal", "_nm_canal"),
          F.upper(df_chamada["tp_chamada"]) == F.col("_nm_canal"), how="left")

    # sk_data_inicio
    .join(df_dim_data.withColumnRenamed("sk_data", "_sk_data_inicio")
                     .withColumnRenamed("dt_completa", "_dt_inicio"),
          F.to_date(df_chamada["dt_inicio"]) == F.col("_dt_inicio"), how="left")

    # sk_data_fim
    .join(df_dim_data.withColumnRenamed("sk_data", "_sk_data_fim")
                     .withColumnRenamed("dt_completa", "_dt_fim"),
          F.to_date(df_chamada["dt_fim"]) == F.col("_dt_fim"), how="left")

    .withColumn("sk_cliente",
        F.coalesce(F.col("_sk_cliente"),   F.lit(-1).cast("int")))
    .withColumn("sk_operador",
        F.coalesce(F.col("_sk_operador"),  F.lit(-1).cast("int")))
    .withColumn("sk_fila",
        F.coalesce(F.col("_sk_fila"),      F.lit(-1).cast("int")))
    .withColumn("sk_status_chamada",
        F.coalesce(F.col("_sk_status"),    F.lit(-1).cast("int")))
    .withColumn("sk_canal",
        F.coalesce(F.col("_sk_canal"),     F.lit(1).cast("int")))  # default TELEFONE
    .withColumn("sk_data_inicio",
        F.coalesce(F.col("_sk_data_inicio"), F.lit(-1).cast("int")))
    .withColumn("sk_data_fim",
        F.coalesce(F.col("_sk_data_fim"),  F.lit(-1).cast("int")))

    # surrogate key do fato
    .withColumn("sk_chamada",
        F.monotonically_increasing_id())

    .withColumn("dt_ingestao_gold", F.lit(now_ts).cast(TimestampType()))

    .select(
        "sk_chamada",
        F.col("id_chamada").alias("nk_chamada"),
        "sk_cliente",
        "sk_operador",
        "sk_fila",
        "sk_canal",
        "sk_status_chamada",
        "sk_data_inicio",
        "sk_data_fim",
        F.col("tp_chamada"),
        F.col("nr_duracao_segundos"),
        F.col("nr_duracao_minutos"),
        F.col("fl_duracao_valida"),
        F.col("fl_chamada_completa"),
        "dt_ingestao_gold",
    )
)

print(f"[INFO] Registros fato_chamada: {df_fato.count()}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{GOLD_TABLE} (
        sk_chamada          BIGINT,
        nk_chamada          BIGINT,
        sk_cliente          INT,
        sk_operador         INT,
        sk_fila             INT,
        sk_canal            INT,
        sk_status_chamada   INT,
        sk_data_inicio      INT,
        sk_data_fim         INT,
        tp_chamada          STRING,
        nr_duracao_segundos INT,
        nr_duracao_minutos  DOUBLE,
        fl_duracao_valida   SMALLINT,
        fl_chamada_completa SMALLINT,
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

df_fato.createOrReplaceTempView("stg_fato_chamada")

spark.sql(f"""
    MERGE INTO glue_catalog.{GOLD_TABLE} AS target
    USING stg_fato_chamada AS source
    ON target.nk_chamada = source.nk_chamada
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Gold: {GOLD_TABLE}")
job.commit()
print("[INFO] Job finalizado com sucesso.")