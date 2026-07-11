# =========================================================
# JOB: job_tb_mensagem_chat_bronze_to_silver.py
# PIPELINE: Bronze → Silver
# TABELA: tb_mensagem_chat
# GLUE VERSION: 4.0
# WORKER TYPE: G.1X / 2 workers
# =========================================================
# OBSERVAÇÃO:
#   ds_mensagem é dado sensível (conteúdo da conversa).
#   Na Silver é omitido e substituído por nr_tamanho_chars.
#   O conteúdo original permanece apenas no Bronze com
#   acesso restrito via Lake Formation.
# =========================================================

import sys
import json
import boto3
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, TimestampType, LongType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME", "ENV"])
JOB_NAME = args["JOB_NAME"]
BUCKET   = args["BUCKET_NAME"]
ENV      = args["ENV"]

sc           = SparkContext()
glue_context = GlueContext(sc)
spark        = glue_context.spark_session
job          = Job(glue_context)
job.init(JOB_NAME, args)

print(f"[INFO] Job iniciado | Tabela: tb_mensagem_chat | ENV: {ENV}")

BRONZE_DATABASE = "db_bronze"
BRONZE_TABLE    = "mensagem_chat"
SILVER_PATH     = f"s3://{BUCKET}/silver/canais/mensagem_chat/"
CHECKPOINT_KEY  = "checkpoints/tb_mensagem_chat/watermark.json"
QUARANTINE_PATH = f"s3://{BUCKET}/quarantine/tb_mensagem_chat/"
SILVER_TABLE    = "db_silver.mensagem_chat"

spark.conf.set("spark.sql.catalog.glue_catalog",
    "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl",
    "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl",
    "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse",
    f"s3://{BUCKET}/silver/")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

DEFAULT_WATERMARK = "1970-01-01T00:00:00Z"
s3_client = boto3.client("s3")

def get_watermark():
    try:
        obj     = s3_client.get_object(Bucket=BUCKET, Key=CHECKPOINT_KEY)
        content = json.loads(obj["Body"].read().decode("utf-8"))
        wm      = content.get("last_watermark", DEFAULT_WATERMARK)
        print(f"[INFO] Watermark recuperado: {wm}")
        return wm
    except s3_client.exceptions.NoSuchKey:
        print("[INFO] Nenhum watermark encontrado. Executando full load.")
        return DEFAULT_WATERMARK
    except Exception as e:
        raise RuntimeError(f"[ERROR] Falha ao ler watermark: {str(e)}")

def save_watermark(new_ts):
    payload = {
        "table_name":     "tb_mensagem_chat",
        "last_watermark": new_ts,
        "updated_at":     datetime.now(timezone.utc).isoformat(),
        "job_name":       JOB_NAME,
    }
    s3_client.put_object(
        Bucket=BUCKET,
        Key=CHECKPOINT_KEY,
        Body=json.dumps(payload, indent=2, ensure_ascii=False),
        ContentType="application/json",
    )
    print(f"[INFO] Watermark atualizado para: {new_ts}")

last_watermark = get_watermark()

BRONZE_S3_PATH = f"s3://{BUCKET}/bronze/suporte/mensagem-chat/"
df_bronze = (
    spark.read.parquet(BRONZE_S3_PATH)
    .filter(F.col("_timestamp") > F.lit(last_watermark))
)

count_bronze = df_bronze.count()
print(f"[INFO] Registros lidos do Bronze (incremental): {count_bronze}")

if count_bronze == 0:
    print("[INFO] Nenhum registro novo. Job encerrado sem processamento.")
    job.commit()
    import os as _os; _os._exit(0)  # exit limpo sem SystemExit

df_cdc = (
    df_bronze
    .withColumn("dt_cdc_evento", F.to_timestamp(F.col("_timestamp")))
    .withColumn("op_cdc",
        F.when(F.col("op") == "I", F.lit("INSERT"))
         .when(F.col("op") == "U", F.lit("UPDATE"))
         .when(F.col("op") == "D", F.lit("DELETE"))
         .otherwise(F.lit("UNKNOWN")))
)

window_dedup = (
    Window.partitionBy("id_mensagem").orderBy(F.col("dt_cdc_evento").desc())
)

df_dedup = (
    df_cdc
    .withColumn("_row_num", F.row_number().over(window_dedup))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num", "Op", "_timestamp")
)

print(f"[INFO] Registros após deduplicação CDC: {df_dedup.count()}")

now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

df_transformed = (
    df_dedup

    # IDs: cast explícito de STRING (bronze/CSV) para BIGINT
    .withColumn("id_mensagem", F.col("id_mensagem").cast(LongType()))
    .withColumn("id_chat", F.col("id_chat").cast(LongType()))
    .withColumn("dt_mensagem",
        F.to_timestamp(F.col("dt_mensagem"), "yyyy-MM-dd HH:mm:ss"))

    .withColumn("ds_remetente",
        F.upper(F.trim(F.col("ds_remetente"))))
    .withColumn("ds_remetente",
        F.coalesce(F.col("ds_remetente"), F.lit("DESCONHECIDO")))

    # Substitui conteúdo da mensagem por tamanho (LGPD)
    .withColumn("nr_tamanho_chars",
        F.when(
            F.col("ds_conteudo").isNotNull(),
            F.length(F.col("ds_conteudo"))
        ).otherwise(F.lit(0)))

    .withColumn("fl_mensagem_cliente",
        F.when(F.col("ds_remetente") == "CLIENTE", F.lit(1))
         .otherwise(F.lit(0)).cast("smallint"))

    .withColumn("fl_mensagem_operador",
        F.when(F.col("ds_remetente") == "OPERADOR", F.lit(1))
         .otherwise(F.lit(0)).cast("smallint"))

    .drop("ds_conteudo")

    .withColumn("hash_registro",
        F.md5(F.concat_ws("|",
            F.coalesce(F.col("id_mensagem").cast("string"), F.lit("")),
            F.coalesce(F.col("id_chat").cast("string"),     F.lit("")),
            F.coalesce(F.col("ds_remetente"),               F.lit("")),
            F.coalesce(F.col("dt_mensagem").cast("string"),    F.lit("")),
        )))

    .withColumn("dt_ingestao_silver", F.lit(now_ts).cast(TimestampType()))
    .withColumn("ano", F.year(F.col("dt_mensagem")))
    .withColumn("mes", F.month(F.col("dt_mensagem")))
    .withColumn("dia", F.dayofmonth(F.col("dt_mensagem")))
)

df_transformed = df_transformed.withColumn(
    "_motivo_quarentena",
    F.when(F.col("id_mensagem").isNull(), F.lit("id_mensagem_nulo"))
     .when(F.col("id_chat").isNull(),     F.lit("id_chat_nulo"))
     .when(F.col("dt_mensagem").isNull(),    F.lit("dt_envio_nula"))
     .otherwise(F.lit(None).cast(StringType()))
)

df_valid      = df_transformed.filter(F.col("_motivo_quarentena").isNull()).drop("_motivo_quarentena")
df_quarantine = df_transformed.filter(F.col("_motivo_quarentena").isNotNull())

print(f"[INFO] Registros válidos:       {df_valid.count()}")
print(f"[INFO] Registros em quarentena: {df_quarantine.count()}")

if not df_quarantine.rdd.isEmpty():
    (
        df_quarantine
        .withColumn("ano_ingestao", F.year(F.current_timestamp()))
        .withColumn("mes_ingestao", F.month(F.current_timestamp()))
        .withColumn("dia_ingestao", F.dayofmonth(F.current_timestamp()))
        .write.mode("append")
        .partitionBy("ano_ingestao", "mes_ingestao", "dia_ingestao")
        .parquet(QUARANTINE_PATH)
    )
    print("[INFO] Registros de quarentena gravados.")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{SILVER_TABLE} (
        id_mensagem             BIGINT,
        id_chat                 BIGINT,
        ds_remetente            STRING,
        dt_mensagem             TIMESTAMP,
        nr_tamanho_chars        INT,
        fl_mensagem_cliente     SMALLINT,
        fl_mensagem_operador    SMALLINT,
        dt_cdc_evento           TIMESTAMP,
        op_cdc                  STRING,
        hash_registro           STRING,
        dt_ingestao_silver      TIMESTAMP,
        ano                     INT,
        mes                     INT,
        dia                     INT
    )
    USING iceberg
    LOCATION '{SILVER_PATH}'
    PARTITIONED BY (ano, mes, dia)
    TBLPROPERTIES (
        'format-version'                  = '2',
        'write.format.default'            = 'parquet',
        'write.parquet.compression-codec' = 'snappy',
        'write.target-file-size-bytes'    = '134217728'
    )
""")

# Seleciona apenas as colunas Silver (garante compatibilidade com INSERT *)
SILVER_COLS = [
    "id_mensagem", "id_chat", "ds_remetente", "dt_mensagem",
    "nr_tamanho_chars", "fl_mensagem_cliente", "fl_mensagem_operador",
    "dt_cdc_evento", "op_cdc", "hash_registro", "dt_ingestao_silver",
    "ano", "mes", "dia",
]
df_valid = df_valid.select(*SILVER_COLS)

df_valid.createOrReplaceTempView("stg_mensagem_chat")

spark.sql(f"""
    MERGE INTO glue_catalog.{SILVER_TABLE} AS target
    USING stg_mensagem_chat AS source
    ON target.id_mensagem = source.id_mensagem
    WHEN MATCHED AND target.hash_registro <> source.hash_registro
    THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Silver: {SILVER_TABLE}")

new_watermark = df_bronze.agg(F.max("_timestamp").alias("max_ts")).collect()[0]["max_ts"]
save_watermark(new_watermark)
job.commit()
print("[INFO] Job finalizado com sucesso.")