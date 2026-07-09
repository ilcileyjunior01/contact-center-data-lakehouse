# =========================================================
# JOB: job_dim_operador_gold.py
# PIPELINE: Silver → Gold
# DIMENSÃO: dim_operador + dim_supervisor (view)
# GLUE VERSION: 4.0
# WORKER TYPE: G.1X / 2 workers
# =========================================================
# OBSERVAÇÃO:
#   dim_operador é construída a partir de silver.operador.
#   O self-join de supervisor é preservado via sk_supervisor
#   que aponta para o próprio sk_operador do supervisor.
#   Após a carga, cria a view dim_supervisor derivada.
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

print(f"[INFO] Job iniciado | dim_operador | ENV: {ENV}")

SILVER_TABLE      = "db_silver.operador"
GOLD_PATH         = f"s3://{BUCKET}/gold/dimensoes/dim_operador/"
GOLD_TABLE        = "db_gold.dim_operador"
GOLD_PATH_SUP     = f"s3://{BUCKET}/gold/dimensoes/dim_supervisor/"
GOLD_TABLE_SUP    = "db_gold.dim_supervisor"

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

# =========================================================
# LEITURA DA SILVER
# =========================================================

df_silver = (
    spark.table(f"glue_catalog.{SILVER_TABLE}")
    .filter(F.col("op_cdc") != "DELETE")
    .select(
        F.col("id_operador").alias("nk_operador"),
        F.col("nm_operador"),
        F.col("ds_login_mascarado"),
        F.col("ds_email_mascarado"),
        F.col("dt_admissao"),
        F.col("st_operador"),
        F.col("fl_operador_ativo"),
        F.col("nr_dias_casa"),
        F.col("ds_faixa_tempo_casa"),
        F.col("id_supervisor").alias("nk_supervisor"),
    )
)

# =========================================================
# SURROGATE KEY DO OPERADOR
# =========================================================

window_sk = Window.orderBy("nk_operador")

df_dim = (
    df_silver
    .withColumn("sk_operador",
        F.row_number().over(window_sk).cast("int"))
)

# =========================================================
# RESOLVE sk_supervisor via self-join
# =========================================================
# Mapeia nk_supervisor → sk_operador do supervisor

df_sk_map = (
    df_dim
    .select(
        F.col("nk_operador"),
        F.col("sk_operador")
    )
)

df_dim = (
    df_dim
    .join(
        df_sk_map.select(
            F.col("nk_operador").alias("_nk_sup"),
            F.col("sk_operador").alias("sk_supervisor")
        ),
        df_dim["nk_supervisor"] == F.col("_nk_sup"),
        how="left"
    )
    .drop("_nk_sup")
    # Supervisor desconhecido recebe sk = -1
    .withColumn("sk_supervisor",
        F.coalesce(F.col("sk_supervisor"), F.lit(-1).cast("int")))

    .withColumn("dt_ingestao_gold", F.lit(now_ts).cast(TimestampType()))

    .select(
        "sk_operador",
        "nk_operador",
        "nm_operador",
        "ds_login_mascarado",
        "ds_email_mascarado",
        "dt_admissao",
        "st_operador",
        "fl_operador_ativo",
        "nr_dias_casa",
        "ds_faixa_tempo_casa",
        "sk_supervisor",
        "nk_supervisor",
        "dt_ingestao_gold",
    )
)

# Registro padrão para operadores não identificados
df_desconhecido = spark.createDataFrame(
    [(-1, -1, "DESCONHECIDO", "", "", None, "D", 0, 0,
      "DESCONHECIDO", -1, -1, now_ts)],
    ["sk_operador", "nk_operador", "nm_operador",
     "ds_login_mascarado", "ds_email_mascarado",
     "dt_admissao", "st_operador", "fl_operador_ativo",
     "nr_dias_casa", "ds_faixa_tempo_casa",
     "sk_supervisor", "nk_supervisor", "dt_ingestao_gold"]
).withColumn("dt_ingestao_gold",  F.col("dt_ingestao_gold").cast(TimestampType())) \
 .withColumn("fl_operador_ativo", F.col("fl_operador_ativo").cast("smallint"))

df_final = df_dim.unionByName(df_desconhecido)

print(f"[INFO] Operadores gerados: {df_final.count()}")

# =========================================================
# CRIA TABELA dim_operador
# =========================================================

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{GOLD_TABLE} (
        sk_operador             INT,
        nk_operador             BIGINT,
        nm_operador             STRING,
        ds_login_mascarado      STRING,
        ds_email_mascarado      STRING,
        dt_admissao             DATE,
        st_operador             STRING,
        fl_operador_ativo       SMALLINT,
        nr_dias_casa            INT,
        ds_faixa_tempo_casa     STRING,
        sk_supervisor           INT,
        nk_supervisor           BIGINT,
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

df_final.createOrReplaceTempView("stg_dim_operador")

spark.sql(f"""
    MERGE INTO glue_catalog.{GOLD_TABLE} AS target
    USING stg_dim_operador AS source
    ON target.nk_operador = source.nk_operador
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Gold: {GOLD_TABLE}")

# =========================================================
# CRIA dim_supervisor como tabela derivada de dim_operador
# =========================================================
# Conforme decidido no início do projeto, dim_supervisor
# não tem job de ingestão Bronze → Silver próprio.
# É materializada aqui como tabela Iceberg separada
# filtrando operadores que aparecem como supervisores.

print("[INFO] Construindo dim_supervisor a partir de dim_operador...")

df_supervisores = (
    spark.table(f"glue_catalog.{GOLD_TABLE}")
    .filter(F.col("sk_operador") != -1)
    .join(
        spark.table(f"glue_catalog.{GOLD_TABLE}")
             .select(F.col("sk_supervisor"))
             .distinct()
             .filter(F.col("sk_supervisor") != -1),
        spark.table(f"glue_catalog.{GOLD_TABLE}")["sk_operador"] ==
        spark.table(f"glue_catalog.{GOLD_TABLE}")["sk_supervisor"],
        how="inner"
    )
)

# Recarrega após MERGE para garantir dados atualizados
df_op_gold = spark.table(f"glue_catalog.{GOLD_TABLE}")

df_sk_supervisores = (
    df_op_gold
    .select(F.col("sk_supervisor"))
    .distinct()
    .filter(F.col("sk_supervisor") != -1)
)

df_dim_supervisor = (
    df_op_gold
    .join(df_sk_supervisores,
          df_op_gold["sk_operador"] == df_sk_supervisores["sk_supervisor"],
          how="inner")
    .select(
        F.col("sk_operador").alias("sk_supervisor"),
        F.col("nk_operador").alias("nk_supervisor"),
        F.col("nm_operador").alias("nm_supervisor"),
        F.col("ds_email_mascarado"),
        F.col("dt_admissao"),
        F.col("st_operador"),
        F.col("fl_operador_ativo"),
        F.col("nr_dias_casa"),
        F.col("ds_faixa_tempo_casa"),
        F.col("dt_ingestao_gold"),
    )
    .distinct()
)

df_sup_desconhecido = spark.createDataFrame(
    [(-1, -1, "DESCONHECIDO", "", None, "D", 0, 0, "DESCONHECIDO", now_ts)],
    ["sk_supervisor", "nk_supervisor", "nm_supervisor",
     "ds_email_mascarado", "dt_admissao", "st_operador",
     "fl_operador_ativo", "nr_dias_casa", "ds_faixa_tempo_casa",
     "dt_ingestao_gold"]
).withColumn("dt_ingestao_gold",  F.col("dt_ingestao_gold").cast(TimestampType())) \
 .withColumn("fl_operador_ativo", F.col("fl_operador_ativo").cast("smallint"))

df_dim_supervisor_final = df_dim_supervisor.unionByName(df_sup_desconhecido)

print(f"[INFO] Supervisores gerados: {df_dim_supervisor_final.count()}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{GOLD_TABLE_SUP} (
        sk_supervisor           INT,
        nk_supervisor           BIGINT,
        nm_supervisor           STRING,
        ds_email_mascarado      STRING,
        dt_admissao             DATE,
        st_operador             STRING,
        fl_operador_ativo       SMALLINT,
        nr_dias_casa            INT,
        ds_faixa_tempo_casa     STRING,
        dt_ingestao_gold        TIMESTAMP
    )
    USING iceberg
    LOCATION '{GOLD_PATH_SUP}'
    TBLPROPERTIES (
        'format-version'                  = '2',
        'write.format.default'            = 'parquet',
        'write.parquet.compression-codec' = 'snappy',
        'write.target-file-size-bytes'    = '134217728'
    )
""")

df_dim_supervisor_final.createOrReplaceTempView("stg_dim_supervisor")

spark.sql(f"""
    MERGE INTO glue_catalog.{GOLD_TABLE_SUP} AS target
    USING stg_dim_supervisor AS source
    ON target.sk_supervisor = source.sk_supervisor
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Gold: {GOLD_TABLE_SUP}")
job.commit()
print("[INFO] Job finalizado com sucesso.")