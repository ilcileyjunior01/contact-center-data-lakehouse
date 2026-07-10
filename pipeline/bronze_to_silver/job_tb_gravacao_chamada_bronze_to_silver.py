# =========================================================
# JOB: job_tb_gravacao_chamada_bronze_to_silver.py
# PIPELINE: Bronze → Silver
# TABELA: tb_gravacao_chamada
# GLUE VERSION: 4.0
# WORKER TYPE: G.1X / 2 workers
# =========================================================
# OBSERVAÇÃO:
#   ds_url_arquivo é dado sensível — omitido na Silver e
#   substituído por fl_tem_gravacao + nr_tamanho_mb.
#   O acesso à URL real permanece restrito ao Bronze
#   via Lake Formation.
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

print(f"[INFO] Job iniciado | Tabela: tb_gravacao_chamada | ENV: {ENV}")

BRONZE_DATABASE = "db_bronze"
BRONZE_TABLE    = "gravacao"
SILVER_PATH     = f"s3://{BUCKET}/silver/operacao/gravacao_chamada/"
CHECKPOINT_KEY  = "checkpoints/tb_gravacao_chamada/watermark.json"
QUARANTINE_PATH = f"s3://{BUCKET}/quarantine/tb_gravacao_chamada/"
SILVER_TABLE    = "db_silver.gravacao_chamada"

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
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

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
        "table_name":     "tb_gravacao_chamada",
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

BRONZE_S3_PATH = f"s3://{BUCKET}/bronze/operacao/gravacao/"
df_bronze = (
    spark.read.parquet(BRONZE_S3_PATH)
    .filter(F.col("_timestamp") > F.lit(last_watermark))
)

count_bronze = df_bronze.count()
print(f"[INFO] Registros lidos do Bronze (incremental): {count_bronze}")

if count_bronze == 0:
    print("[INFO] Nenhum registro novo. Job encerrado.")
    job.commit()
    sys.exit(0)

df_cdc = (
    df_bronze
    .withColumn("dt_cdc_evento", F.to_timestamp(F.col("_timestamp")))
    .withColumn("op_cdc",
        F.when(F.col("Op") == "I", F.lit("INSERT"))
         .when(F.col("Op") == "U", F.lit("UPDATE"))
         .when(F.col("Op") == "D", F.lit("DELETE"))
         .otherwise(F.lit("UNKNOWN")))
)

window_dedup = (
    Window.partitionBy("id_gravacao").orderBy(F.col("dt_cdc_evento").desc())
)

df_dedup = (
    df_cdc
    .withColumn("_row_num", F.row_number().over(window_dedup))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num", "Op", "_timestamp", "fl_processada")
)

print(f"[INFO] Registros após deduplicação CDC: {df_dedup.count()}")

now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

df_transformed = (
    df_dedup

    # --- Conversão de tipos ---
    # IDs: cast explícito de STRING (bronze/CSV) para BIGINT
    .withColumn("id_gravacao", F.col("id_gravacao").cast(LongType()))
    .withColumn("id_chamada", F.col("id_chamada").cast(LongType()))
    .withColumn("dt_gravacao",
        F.to_date(F.col("dt_gravacao"), "yyyy-MM-dd"))
    .withColumn("nr_tamanho_mb",
        F.col("nr_tamanho_mb").cast("double"))
    .withColumn("nr_tamanho_mb",
        F.coalesce(F.col("nr_tamanho_mb"), F.lit(0.0)))

    # --- Omite URL real da Silver (dado sensível) ---
    .withColumn("fl_tem_gravacao",
        F.when(
            F.col("ds_url_gravacao").isNotNull() &
            (F.length(F.col("ds_url_gravacao")) > 0),
            F.lit(1)
        ).otherwise(F.lit(0)).cast("smallint"))

    .drop("ds_url_gravacao")

    # --- Campos derivados ---
    .withColumn("fl_expirada",
        F.when(
            F.col("dt_gravacao").isNotNull() &
            (F.col("dt_gravacao") < F.current_date()),
            F.lit(1)
        ).otherwise(F.lit(0)).cast("smallint"))

    .withColumn("nr_dias_para_expirar",
        F.when(
            F.col("dt_gravacao").isNotNull(),
            F.datediff(F.col("dt_gravacao"), F.current_date())
        ).otherwise(F.lit(None)))

    # --- Hash de integridade ---
    .withColumn("hash_registro",
        F.md5(F.concat_ws("|",
            F.coalesce(F.col("id_gravacao").cast("string"),   F.lit("")),
            F.coalesce(F.col("id_chamada").cast("string"),    F.lit("")),
            F.coalesce(F.col("nr_tamanho_mb").cast("string"), F.lit("")),
            F.coalesce(F.col("dt_gravacao").cast("string"),  F.lit("")),
        )))

    # --- Auditoria ---
    .withColumn("dt_ingestao_silver", F.lit(now_ts).cast(TimestampType()))
)

df_transformed = df_transformed.withColumn(
    "_motivo_quarentena",
    F.when(F.col("id_gravacao").isNull(), F.lit("id_gravacao_nulo"))
     .when(F.col("id_chamada").isNull(),  F.lit("id_chamada_nulo"))
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
        id_gravacao             BIGINT,
        id_chamada              BIGINT,
        nr_tamanho_mb           DOUBLE,
        dt_gravacao            DATE,
        fl_tem_gravacao         SMALLINT,
        fl_expirada             SMALLINT,
        nr_dias_para_expirar    INT,
        dt_cdc_evento           TIMESTAMP,
        op_cdc                  STRING,
        hash_registro           STRING,
        dt_ingestao_silver      TIMESTAMP
    )
    USING iceberg
    LOCATION '{SILVER_PATH}'
    TBLPROPERTIES (
        'format-version'                  = '2',
        'write.format.default'            = 'parquet',
        'write.parquet.compression-codec' = 'snappy',
        'write.target-file-size-bytes'    = '134217728'
    )
""")

print(f"[INFO] Tabela Iceberg verificada: {SILVER_TABLE}")

df_valid.createOrReplaceTempView("stg_gravacao_chamada")

spark.sql(f"""
    MERGE INTO glue_catalog.{SILVER_TABLE} AS target
    USING stg_gravacao_chamada AS source
    ON target.id_gravacao = source.id_gravacao
    WHEN MATCHED AND target.hash_registro <> source.hash_registro
    THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Silver: {SILVER_TABLE}")

new_watermark = df_bronze.agg(F.max("_timestamp").alias("max_ts")).collect()[0]["max_ts"]
save_watermark(new_watermark)
job.commit()
print("[INFO] Job finalizado com sucesso.")