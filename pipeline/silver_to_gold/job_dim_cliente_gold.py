# =========================================================
# JOB: job_dim_cliente_gold.py
# PIPELINE: Silver → Gold
# DIMENSÃO: dim_cliente
# GLUE VERSION: 4.0
# WORKER TYPE: G.1X / 2 workers
# =========================================================
# OBSERVAÇÃO:
#   dim_cliente é construída via join entre silver.cliente
#   e silver.endereco_cliente. Quando o cliente tem mais
#   de um endereço, usa-se o mais recente via window.
#   SCD Tipo 1 — sobrescreve atributos alterados.
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

print(f"[INFO] Job iniciado | dim_cliente | ENV: {ENV}")

SILVER_CLIENTE  = "db_silver.cliente"
SILVER_ENDERECO = "db_silver.endereco_cliente"
GOLD_PATH       = f"s3://{BUCKET}/gold/dimensoes/dim_cliente/"
GOLD_TABLE      = "db_gold.dim_cliente"

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
# LEITURA DA SILVER
# =========================================================

df_cliente = (
    spark.table(f"glue_catalog.{SILVER_CLIENTE}")
    .filter(F.col("op_cdc") != "DELETE")
    .select(
        F.col("id_cliente").alias("nk_cliente"),
        F.col("nm_cliente"),
        F.col("nr_documento_mascarado"),
        F.col("ds_email_mascarado"),
        F.col("nr_telefone_mascarado"),
        F.col("dt_cadastro"),
        F.col("st_cliente"),
        F.col("fl_cliente_ativo"),
    )
)

# Seleciona o endereço mais recente por cliente
window_end = Window.partitionBy("id_cliente").orderBy(F.col("dt_ingestao_silver").desc())

df_endereco = (
    spark.table(f"glue_catalog.{SILVER_ENDERECO}")
    .filter(F.col("op_cdc") != "DELETE")
    .withColumn("_row_num", F.row_number().over(window_end))
    .filter(F.col("_row_num") == 1)
    .select(
        F.col("id_cliente"),
        F.col("ds_cidade"),
        F.col("ds_estado"),
        F.col("ds_bairro"),
        F.col("nr_cep_mascarado"),
    )
)

# =========================================================
# JOIN CLIENTE + ENDEREÇO
# =========================================================

df_joined = (
    df_cliente
    .join(df_endereco,
          df_cliente["nk_cliente"] == df_endereco["id_cliente"],
          how="left")
    .drop("id_cliente")
)

# =========================================================
# SURROGATE KEY
# =========================================================

window_sk = Window.orderBy("nk_cliente")

df_dim = (
    df_joined
    .withColumn("sk_cliente",
        F.row_number().over(window_sk).cast("int"))

    .withColumn("ds_cidade",
        F.coalesce(F.col("ds_cidade"), F.lit("NAO_INFORMADO")))
    .withColumn("ds_estado",
        F.coalesce(F.col("ds_estado"), F.lit("NAO_INFORMADO")))
    .withColumn("ds_bairro",
        F.coalesce(F.col("ds_bairro"), F.lit("NAO_INFORMADO")))
    .withColumn("nr_cep_mascarado",
        F.coalesce(F.col("nr_cep_mascarado"), F.lit("")))

    .withColumn("dt_ingestao_gold", F.lit(now_ts).cast(TimestampType()))

    .select(
        "sk_cliente",
        "nk_cliente",
        "nm_cliente",
        "nr_documento_mascarado",
        "ds_email_mascarado",
        "nr_telefone_mascarado",
        "dt_cadastro",
        "st_cliente",
        "fl_cliente_ativo",
        "ds_cidade",
        "ds_estado",
        "ds_bairro",
        "nr_cep_mascarado",
        "dt_ingestao_gold",
    )
)

# Registro padrão para clientes não identificados
# spark.sql usado para evitar inferência de tipo com colunas NULL (dt_cadastro)
df_desconhecido = spark.sql(f"""
    SELECT
        CAST(-1  AS INT)         AS sk_cliente,
        CAST(-1  AS BIGINT)      AS nk_cliente,
        'DESCONHECIDO'           AS nm_cliente,
        ''                       AS nr_documento_mascarado,
        ''                       AS ds_email_mascarado,
        ''                       AS nr_telefone_mascarado,
        CAST(NULL AS TIMESTAMP)  AS dt_cadastro,
        'D'                      AS st_cliente,
        CAST(0 AS SMALLINT)      AS fl_cliente_ativo,
        'NAO_INFORMADO'          AS ds_cidade,
        'NAO_INFORMADO'          AS ds_estado,
        'NAO_INFORMADO'          AS ds_bairro,
        ''                       AS nr_cep_mascarado,
        CAST('{now_ts}' AS TIMESTAMP) AS dt_ingestao_gold
""")

df_final = df_dim.unionByName(df_desconhecido)

print(f"[INFO] Clientes gerados: {df_final.count()}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{GOLD_TABLE} (
        sk_cliente              INT,
        nk_cliente              BIGINT,
        nm_cliente              STRING,
        nr_documento_mascarado  STRING,
        ds_email_mascarado      STRING,
        nr_telefone_mascarado   STRING,
        dt_cadastro             TIMESTAMP,
        st_cliente              STRING,
        fl_cliente_ativo        SMALLINT,
        ds_cidade               STRING,
        ds_estado               STRING,
        ds_bairro               STRING,
        nr_cep_mascarado        STRING,
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

df_final.createOrReplaceTempView("stg_dim_cliente")

spark.sql(f"""
    MERGE INTO glue_catalog.{GOLD_TABLE} AS target
    USING stg_dim_cliente AS source
    ON target.nk_cliente = source.nk_cliente
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Gold: {GOLD_TABLE}")
job.commit()
print("[INFO] Job finalizado com sucesso.")