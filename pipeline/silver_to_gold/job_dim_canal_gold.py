# =========================================================
# JOB: job_dim_canal_gold.py
# PIPELINE: Silver → Gold
# DIMENSÃO: dim_canal
# GLUE VERSION: 4.0
# WORKER TYPE: G.1X / 2 workers
# =========================================================
# OBSERVAÇÃO:
#   dim_canal é gerada sinteticamente a partir dos tipos
#   de canal conhecidos do negócio. Não depende de Silver.
#   É uma dimensão pequena e estável — full refresh.
# =========================================================

import sys
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, TimestampType, ShortType
)

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME", "ENV"])
JOB_NAME = args["JOB_NAME"]
BUCKET   = args["BUCKET_NAME"]
ENV      = args["ENV"]

sc           = SparkContext()
glue_context = GlueContext(sc)
spark        = glue_context.spark_session
job          = Job(glue_context)
job.init(JOB_NAME, args)

print(f"[INFO] Job iniciado | dim_canal | ENV: {ENV}")

GOLD_PATH  = f"s3://{BUCKET}/gold/dimensoes/dim_canal/"
GOLD_TABLE = "db_gold.dim_canal"

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

now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# =========================================================
# CANAIS CONHECIDOS DO NEGÓCIO
# =========================================================
canais = [
    (1, "TELEFONE",   "VOZ",     "Atendimento via chamada telefônica",      1, 1),
    (2, "CHAT",       "DIGITAL", "Atendimento via chat web",                1, 1),
    (3, "WHATSAPP",   "DIGITAL", "Atendimento via WhatsApp",                1, 1),
    (4, "EMAIL",      "DIGITAL", "Atendimento via e-mail",                  1, 0),
    (5, "URA",        "VOZ",     "Atendimento automatizado via URA/IVR",    1, 0),
    (6, "DESCONHECIDO","OUTROS", "Canal não identificado",                  0, 0),
]

schema = StructType([
    StructField("sk_canal",         IntegerType(), False),
    StructField("nm_canal",         StringType(),  False),
    StructField("ds_tipo_canal",    StringType(),  False),
    StructField("ds_descricao",     StringType(),  True),
    StructField("fl_canal_ativo",   ShortType(),   False),
    StructField("fl_canal_digital", ShortType(),   False),
])

df_dim_canal = (
    spark.createDataFrame(canais, schema=schema)
    .withColumn("dt_ingestao_gold", F.lit(now_ts).cast(TimestampType()))
)

print(f"[INFO] Canais gerados: {df_dim_canal.count()}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{GOLD_TABLE} (
        sk_canal            INT,
        nm_canal            STRING,
        ds_tipo_canal       STRING,
        ds_descricao        STRING,
        fl_canal_ativo      SMALLINT,
        fl_canal_digital    SMALLINT,
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

print(f"[INFO] Tabela Iceberg verificada: {GOLD_TABLE}")

df_dim_canal.createOrReplaceTempView("stg_dim_canal")

spark.sql(f"""
    MERGE INTO glue_catalog.{GOLD_TABLE} AS target
    USING stg_dim_canal AS source
    ON target.sk_canal = source.sk_canal
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Gold: {GOLD_TABLE}")
job.commit()
print("[INFO] Job finalizado com sucesso.")