# =========================================================
# JOB: job_dim_data_gold.py
# PIPELINE: Silver → Gold
# DIMENSÃO: dim_data
# GLUE VERSION: 4.0
# WORKER TYPE: G.1X / 2 workers
# =========================================================
# OBSERVAÇÃO:
#   dim_data é gerada sinteticamente — não depende de
#   nenhuma tabela Silver. É populada para um range de
#   datas configurável via parâmetro do job.
#   Geração única (full refresh) pois é uma dimensão
#   estática que cresce apenas com o passar do tempo.
# =========================================================

import sys
import boto3
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DateType

args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "BUCKET_NAME", "ENV",
    "DT_INICIO",  # ex: "2015-01-01"
    "DT_FIM",     # ex: "2030-12-31"
])

JOB_NAME = args["JOB_NAME"]
BUCKET   = args["BUCKET_NAME"]
ENV      = args["ENV"]
DT_INICIO = args["DT_INICIO"]
DT_FIM    = args["DT_FIM"]

sc           = SparkContext()
glue_context = GlueContext(sc)
spark        = glue_context.spark_session
job          = Job(glue_context)
job.init(JOB_NAME, args)

print(f"[INFO] Job iniciado | dim_data | ENV: {ENV}")
print(f"[INFO] Range: {DT_INICIO} → {DT_FIM}")

GOLD_PATH  = f"s3://{BUCKET}/gold/dimensoes/dim_data/"
GOLD_TABLE = "db_gold.dim_data"

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

# =========================================================
# GERAÇÃO SINTÉTICA DO CALENDÁRIO
# =========================================================
# Gera sequência de datas usando o recurso nativo do Spark
# via sequence() + explode(), sem dependência de UDFs Python.

df_datas = spark.sql(f"""
    SELECT explode(
        sequence(
            to_date('{DT_INICIO}', 'yyyy-MM-dd'),
            to_date('{DT_FIM}',    'yyyy-MM-dd'),
            interval 1 day
        )
    ) AS dt_completa
""")

# Feriados nacionais fixos BR (expansível via tabela de referência)
FERIADOS_BR = [
    "01-01",  # Confraternização Universal
    "04-21",  # Tiradentes
    "05-01",  # Dia do Trabalho
    "09-07",  # Independência
    "10-12",  # N. Sra. Aparecida
    "11-02",  # Finados
    "11-15",  # Proclamação da República
    "12-25",  # Natal
]

feriados_expr = " OR ".join(
    [f"date_format(dt_completa, 'MM-dd') = '{f}'" for f in FERIADOS_BR]
)

now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

df_dim_data = (
    df_datas

    # --- Surrogate key: formato YYYYMMDD como inteiro ---
    .withColumn("sk_data",
        F.date_format(F.col("dt_completa"), "yyyyMMdd").cast("int"))

    # --- Atributos de data ---
    .withColumn("nr_ano",
        F.year(F.col("dt_completa")))
    .withColumn("nr_mes",
        F.month(F.col("dt_completa")))
    .withColumn("nr_dia",
        F.dayofmonth(F.col("dt_completa")))
    .withColumn("nr_trimestre",
        F.quarter(F.col("dt_completa")))
    .withColumn("nr_semana_ano",
        F.weekofyear(F.col("dt_completa")))
    .withColumn("nr_dia_semana",
        F.dayofweek(F.col("dt_completa")))

    # --- Descrições ---
    .withColumn("ds_dia_semana",
        F.when(F.dayofweek(F.col("dt_completa")) == 1, F.lit("DOMINGO"))
         .when(F.dayofweek(F.col("dt_completa")) == 2, F.lit("SEGUNDA"))
         .when(F.dayofweek(F.col("dt_completa")) == 3, F.lit("TERCA"))
         .when(F.dayofweek(F.col("dt_completa")) == 4, F.lit("QUARTA"))
         .when(F.dayofweek(F.col("dt_completa")) == 5, F.lit("QUINTA"))
         .when(F.dayofweek(F.col("dt_completa")) == 6, F.lit("SEXTA"))
         .when(F.dayofweek(F.col("dt_completa")) == 7, F.lit("SABADO"))
         .otherwise(F.lit("DESCONHECIDO")))

    .withColumn("ds_mes",
        F.when(F.month(F.col("dt_completa")) == 1,  F.lit("JANEIRO"))
         .when(F.month(F.col("dt_completa")) == 2,  F.lit("FEVEREIRO"))
         .when(F.month(F.col("dt_completa")) == 3,  F.lit("MARCO"))
         .when(F.month(F.col("dt_completa")) == 4,  F.lit("ABRIL"))
         .when(F.month(F.col("dt_completa")) == 5,  F.lit("MAIO"))
         .when(F.month(F.col("dt_completa")) == 6,  F.lit("JUNHO"))
         .when(F.month(F.col("dt_completa")) == 7,  F.lit("JULHO"))
         .when(F.month(F.col("dt_completa")) == 8,  F.lit("AGOSTO"))
         .when(F.month(F.col("dt_completa")) == 9,  F.lit("SETEMBRO"))
         .when(F.month(F.col("dt_completa")) == 10, F.lit("OUTUBRO"))
         .when(F.month(F.col("dt_completa")) == 11, F.lit("NOVEMBRO"))
         .when(F.month(F.col("dt_completa")) == 12, F.lit("DEZEMBRO"))
         .otherwise(F.lit("DESCONHECIDO")))

    .withColumn("ds_trimestre",
        F.when(F.quarter(F.col("dt_completa")) == 1, F.lit("Q1"))
         .when(F.quarter(F.col("dt_completa")) == 2, F.lit("Q2"))
         .when(F.quarter(F.col("dt_completa")) == 3, F.lit("Q3"))
         .when(F.quarter(F.col("dt_completa")) == 4, F.lit("Q4"))
         .otherwise(F.lit("DESCONHECIDO")))

    # --- Flags ---
    .withColumn("fl_fim_de_semana",
        F.when(F.dayofweek(F.col("dt_completa")).isin(1, 7), F.lit(1))
         .otherwise(F.lit(0)).cast("smallint"))

    .withColumn("fl_feriado",
        F.when(F.expr(feriados_expr), F.lit(1))
         .otherwise(F.lit(0)).cast("smallint"))

    .withColumn("fl_dia_util",
        F.when(
            (F.dayofweek(F.col("dt_completa")).isin(1, 7)) |
            F.expr(feriados_expr),
            F.lit(0)
        ).otherwise(F.lit(1)).cast("smallint"))

    .withColumn("dt_ingestao_gold", F.lit(now_ts).cast("timestamp"))
)

# Registro padrão para datas desconhecidas
df_desconhecido = spark.createDataFrame(
    [(-1, None, -1, -1, -1, -1, -1, -1,
      "DESCONHECIDO", "DESCONHECIDO", "DESCONHECIDO", 0, 0, 0, now_ts)],
    ["sk_data", "dt_completa", "nr_ano", "nr_mes", "nr_dia", "nr_trimestre",
     "nr_semana_ano", "nr_dia_semana", "ds_dia_semana", "ds_mes",
     "ds_trimestre", "fl_fim_de_semana", "fl_feriado", "fl_dia_util",
     "dt_ingestao_gold"]
).withColumn("dt_ingestao_gold", F.col("dt_ingestao_gold").cast("timestamp")) \
 .withColumn("fl_fim_de_semana", F.col("fl_fim_de_semana").cast("smallint")) \
 .withColumn("fl_feriado",       F.col("fl_feriado").cast("smallint")) \
 .withColumn("fl_dia_util",      F.col("fl_dia_util").cast("smallint"))

df_final = df_dim_data.unionByName(df_desconhecido)

print(f"[INFO] Datas geradas: {df_final.count()}")

# =========================================================
# CRIAÇÃO DA TABELA ICEBERG (idempotente)
# =========================================================

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{GOLD_TABLE} (
        sk_data             INT,
        dt_completa         DATE,
        nr_ano              INT,
        nr_mes              INT,
        nr_dia              INT,
        nr_trimestre        INT,
        nr_semana_ano       INT,
        nr_dia_semana       INT,
        ds_dia_semana       STRING,
        ds_mes              STRING,
        ds_trimestre        STRING,
        fl_fim_de_semana    SMALLINT,
        fl_feriado          SMALLINT,
        fl_dia_util         SMALLINT,
        dt_ingestao_gold    TIMESTAMP
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

df_final.createOrReplaceTempView("stg_dim_data")

spark.sql(f"""
    MERGE INTO glue_catalog.{GOLD_TABLE} AS target
    USING stg_dim_data AS source
    ON target.sk_data = source.sk_data
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Gold: {GOLD_TABLE}")
job.commit()
print("[INFO] Job finalizado com sucesso.")

