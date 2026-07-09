# =========================================================
# JOB: job_fato_ticket_gold.py
# PIPELINE: Silver → Gold
# FATO: fato_ticket
# GLUE VERSION: 4.0
# WORKER TYPE: G.1X / 2 workers
# =========================================================
# DEPENDÊNCIAS:
#   dim_data, dim_cliente, dim_operador,
#   dim_status_ticket, dim_categoria_ticket,
#   dim_prioridade_ticket
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

print(f"[INFO] Job iniciado | fato_ticket | ENV: {ENV}")

SILVER_TICKET  = "db_silver.ticket"
DIM_CLIENTE    = "db_gold.dim_cliente"
DIM_OPERADOR   = "db_gold.dim_operador"
DIM_STATUS     = "db_gold.dim_status_ticket"
DIM_CATEGORIA  = "db_gold.dim_categoria_ticket"
DIM_PRIORIDADE = "db_gold.dim_prioridade_ticket"
DIM_DATA       = "db_gold.dim_data"
GOLD_PATH      = f"s3://{BUCKET}/gold/fatos/fato_ticket/"
GOLD_TABLE     = "db_gold.fato_ticket"

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

df_ticket      = spark.table(f"glue_catalog.{SILVER_TICKET}").filter(F.col("op_cdc") != "DELETE")
df_dim_cliente = spark.table(f"glue_catalog.{DIM_CLIENTE}").select("sk_cliente", "nk_cliente")
df_dim_op      = spark.table(f"glue_catalog.{DIM_OPERADOR}").select("sk_operador", "nk_operador")
df_dim_status  = spark.table(f"glue_catalog.{DIM_STATUS}").select("sk_status", "ds_status")
df_dim_cat     = spark.table(f"glue_catalog.{DIM_CATEGORIA}").select("sk_categoria", "nm_categoria")
df_dim_pri     = spark.table(f"glue_catalog.{DIM_PRIORIDADE}").select("sk_prioridade", "nm_prioridade")
df_dim_data    = spark.table(f"glue_catalog.{DIM_DATA}").select("sk_data", "dt_completa")

df_fato = (
    df_ticket

    .join(df_dim_cliente.withColumnRenamed("sk_cliente", "_sk_cli")
                        .withColumnRenamed("nk_cliente", "_nk_cli"),
          df_ticket["id_cliente"] == F.col("_nk_cli"), how="left")

    .join(df_dim_op.withColumnRenamed("sk_operador", "_sk_op")
                   .withColumnRenamed("nk_operador", "_nk_op"),
          df_ticket["id_operador_abertura"] == F.col("_nk_op"), how="left")

    .join(df_dim_status.withColumnRenamed("sk_status", "_sk_st")
                       .withColumnRenamed("ds_status", "_ds_st"),
          F.upper(df_ticket["st_ticket"]) == F.col("_ds_st"), how="left")

    .join(df_dim_cat.withColumnRenamed("sk_categoria", "_sk_cat")
                    .withColumnRenamed("nm_categoria", "_nm_cat"),
          F.upper(df_ticket["ds_categoria"]) == F.col("_nm_cat"), how="left")

    .join(df_dim_pri.withColumnRenamed("sk_prioridade", "_sk_pri")
                    .withColumnRenamed("nm_prioridade", "_nm_pri"),
          F.upper(df_ticket["ds_prioridade"]) == F.col("_nm_pri"), how="left")

    .join(df_dim_data.withColumnRenamed("sk_data", "_sk_dt_ab")
                     .withColumnRenamed("dt_completa", "_dt_ab"),
          F.to_date(df_ticket["dt_abertura"]) == F.col("_dt_ab"), how="left")

    .join(df_dim_data.withColumnRenamed("sk_data", "_sk_dt_fc")
                     .withColumnRenamed("dt_completa", "_dt_fc"),
          F.to_date(df_ticket["dt_fechamento"]) == F.col("_dt_fc"), how="left")

    .withColumn("sk_cliente",
        F.coalesce(F.col("_sk_cli"), F.lit(-1).cast("int")))
    .withColumn("sk_operador_abertura",
        F.coalesce(F.col("_sk_op"),  F.lit(-1).cast("int")))
    .withColumn("sk_status_ticket",
        F.coalesce(F.col("_sk_st"),  F.lit(-1).cast("int")))
    .withColumn("sk_categoria",
        F.coalesce(F.col("_sk_cat"), F.lit(-1).cast("int")))
    .withColumn("sk_prioridade",
        F.coalesce(F.col("_sk_pri"), F.lit(-1).cast("int")))
    .withColumn("sk_data_abertura",
        F.coalesce(F.col("_sk_dt_ab"), F.lit(-1).cast("int")))
    .withColumn("sk_data_fechamento",
        F.coalesce(F.col("_sk_dt_fc"), F.lit(-1).cast("int")))

    .withColumn("sk_ticket",
        F.row_number().over(Window.orderBy("id_ticket")).cast("int"))

    .withColumn("dt_ingestao_gold", F.lit(now_ts).cast(TimestampType()))

    .select(
        "sk_ticket",
        F.col("id_ticket").alias("nk_ticket"),
        F.col("nr_protocolo"),
        "sk_cliente",
        "sk_operador_abertura",
        "sk_status_ticket",
        "sk_categoria",
        "sk_prioridade",
        "sk_data_abertura",
        "sk_data_fechamento",
        F.col("nr_tempo_resolucao_min"),
        F.col("fl_ticket_resolvido"),
        F.col("fl_dentro_sla"),
        "dt_ingestao_gold",
    )
)

print(f"[INFO] Registros fato_ticket: {df_fato.count()}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{GOLD_TABLE} (
        sk_ticket               INT,
        nk_ticket               BIGINT,
        nr_protocolo            STRING,
        sk_cliente              INT,
        sk_operador_abertura    INT,
        sk_status_ticket        INT,
        sk_categoria            INT,
        sk_prioridade           INT,
        sk_data_abertura        INT,
        sk_data_fechamento      INT,
        nr_tempo_resolucao_min  DOUBLE,
        fl_ticket_resolvido     SMALLINT,
        fl_dentro_sla           SMALLINT,
        dt_ingestao_gold        TIMESTAMP
    )
    USING iceberg
    LOCATION '{GOLD_PATH}'
    PARTITIONED BY (sk_data_abertura)
    TBLPROPERTIES (
        'format-version'                  = '2',
        'write.format.default'            = 'parquet',
        'write.parquet.compression-codec' = 'snappy',
        'write.target-file-size-bytes'    = '134217728'
    )
""")

df_fato.createOrReplaceTempView("stg_fato_ticket")

spark.sql(f"""
    MERGE INTO glue_catalog.{GOLD_TABLE} AS target
    USING stg_fato_ticket AS source
    ON target.nk_ticket = source.nk_ticket
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Gold: {GOLD_TABLE}")
job.commit()
print("[INFO] Job finalizado com sucesso.")