# =========================================================
# JOB: job_tb_discagem_bronze_to_silver.py
# PIPELINE: Bronze → Silver
# TABELA: tb_discagem
# GLUE VERSION: 4.0
# WORKER TYPE: G.1X / 2 workers
# =========================================================
# OBSERVAÇÃO:
#   nr_telefone é dado PII — mascarado na Silver.
#   Depende de tb_campanha estar disponível na Silver
#   antes de popular fato_discagem na Gold.
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

print(f"[INFO] Job iniciado | Tabela: tb_discagem | ENV: {ENV}")

BRONZE_DATABASE = "db_bronze"
BRONZE_TABLE    = "discagem"
SILVER_PATH     = f"s3://{BUCKET}/silver/comercial/discagem/"
CHECKPOINT_KEY  = "checkpoints/tb_discagem/watermark.json"
QUARANTINE_PATH = f"s3://{BUCKET}/quarantine/tb_discagem/"
SILVER_TABLE    = "db_silver.discagem"

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
        "table_name":     "tb_discagem",
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

dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
    database=BRONZE_DATABASE,
    table_name=BRONZE_TABLE,
    transformation_ctx="bronze_tb_discagem",
)

df_bronze = (
    dynamic_frame.toDF()
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
    Window.partitionBy("id_discagem").orderBy(F.col("dt_cdc_evento").desc())
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
    .withColumn("id_discagem", F.col("id_discagem").cast(LongType()))
    .withColumn("id_campanha", F.col("id_campanha").cast(LongType()))
    .withColumn("id_cliente", F.col("id_cliente").cast(LongType()))
    .drop("id_operador")
    .withColumn("dt_ultima_tentativa",
        F.to_timestamp(F.col("dt_ultima_tentativa"), "yyyy-MM-dd'T'HH:mm:ss"))

    .withColumn("st_discagem",
        F.upper(F.trim(F.col("st_discagem"))))
    .withColumn("st_discagem",
        F.coalesce(F.col("st_discagem"), F.lit("DESCONHECIDO")))

    # Mascara telefone (LGPD)
    .withColumn("nr_telefone",
        F.regexp_replace(F.col("nr_telefone"), r"[^\d+]", ""))
    .withColumn("nr_telefone_mascarado",
        F.when(
            F.col("nr_telefone").isNotNull() & (F.length(F.col("nr_telefone")) > 0),
            F.concat(F.lit("******"), F.substring(F.col("nr_telefone"), -4, 4))
        ).otherwise(F.lit("")))
    .drop("nr_telefone")

    .withColumn("fl_discagem_atendida",
        F.when(F.col("st_discagem") == "ATENDIDO", F.lit(1))
         .otherwise(F.lit(0)).cast("smallint"))

    .withColumn("fl_discagem_nao_atendida",
        F.when(
            F.col("st_discagem").isin("NAO_ATENDIDO", "OCUPADO", "INVALIDO"),
            F.lit(1)
        ).otherwise(F.lit(0)).cast("smallint"))

    .withColumn("hash_registro",
        F.md5(F.concat_ws("|",
            F.coalesce(F.col("id_discagem").cast("string"),   F.lit("")),
            F.coalesce(F.col("id_campanha").cast("string"),   F.lit("")),
            F.coalesce(F.col("id_cliente").cast("string"),    F.lit("")),
            F.coalesce(F.col("nr_telefone_mascarado"), F.lit("")),
            F.coalesce(F.col("dt_ultima_tentativa").cast("string"),   F.lit("")),
            F.coalesce(F.col("st_discagem"),                  F.lit("")),
        )))

    .withColumn("dt_ingestao_silver", F.lit(now_ts).cast(TimestampType()))
    .withColumn("ano", F.year(F.col("dt_ultima_tentativa")))
    .withColumn("mes", F.month(F.col("dt_ultima_tentativa")))
    .withColumn("dia", F.dayofmonth(F.col("dt_ultima_tentativa")))
)

df_transformed = df_transformed.withColumn(
    "_motivo_quarentena",
    F.when(F.col("id_discagem").isNull(),  F.lit("id_discagem_nulo"))
     .when(F.col("id_campanha").isNull(),  F.lit("id_campanha_nulo"))
     .when(F.col("id_cliente").isNull(),   F.lit("id_cliente_nulo"))
     .when(F.col("dt_ultima_tentativa").isNull(),  F.lit("dt_ultima_tentativa_nula"))
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
        id_discagem                 BIGINT,
        id_campanha                 BIGINT,
        id_cliente                  BIGINT,
        nr_telefone_mascarado       STRING,
        dt_ultima_tentativa                 TIMESTAMP,
        st_discagem                 STRING,
        fl_discagem_atendida        SMALLINT,
        fl_discagem_nao_atendida    SMALLINT,
        dt_cdc_evento               TIMESTAMP,
        op_cdc                      STRING,
        hash_registro               STRING,
        dt_ingestao_silver          TIMESTAMP,
        ano                         INT,
        mes                         INT,
        dia                         INT
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

df_valid.createOrReplaceTempView("stg_discagem")

spark.sql(f"""
    MERGE INTO glue_catalog.{SILVER_TABLE} AS target
    USING stg_discagem AS source
    ON target.id_discagem = source.id_discagem
    WHEN MATCHED AND target.hash_registro <> source.hash_registro
    THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Silver: {SILVER_TABLE}")

new_watermark = df_bronze.agg(F.max("_timestamp").alias("max_ts")).collect()[0]["max_ts"]
save_watermark(new_watermark)
job.commit()
print("[INFO] Job finalizado com sucesso.")