# =========================================================
# JOB: job_tb_jornada_operador_bronze_to_silver.py
# PIPELINE: Bronze → Silver
# TABELA: tb_jornada_operador
# GLUE VERSION: 4.0
# WORKER TYPE: G.1X / 2 workers
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
from pyspark.sql.types import StringType, IntegerType, TimestampType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME", "ENV"])
JOB_NAME = args["JOB_NAME"]
BUCKET   = args["BUCKET_NAME"]
ENV      = args["ENV"]

sc           = SparkContext()
glue_context = GlueContext(sc)
spark        = glue_context.spark_session
job          = Job(glue_context)
job.init(JOB_NAME, args)

print(f"[INFO] Job iniciado | Tabela: tb_jornada_operador | ENV: {ENV}")

BRONZE_DATABASE = "db_bronze"
BRONZE_TABLE    = "tb_jornada_operador"
SILVER_PATH     = f"s3://{BUCKET}/silver/operacao/jornada_operador/"
CHECKPOINT_KEY  = "checkpoints/tb_jornada_operador/watermark.json"
QUARANTINE_PATH = f"s3://{BUCKET}/quarantine/tb_jornada_operador/"
SILVER_TABLE    = "db_silver.jornada_operador"

spark.conf.set("spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
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
        "table_name":     "tb_jornada_operador",
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
    transformation_ctx="bronze_tb_jornada_operador",
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
    Window.partitionBy("id_jornada").orderBy(F.col("dt_cdc_evento").desc())
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

    .withColumn("dt_inicio_turno",
        F.to_timestamp(F.col("dt_inicio_turno"), "yyyy-MM-dd'T'HH:mm:ss"))
    .withColumn("dt_fim_turno",
        F.to_timestamp(F.col("dt_fim_turno"), "yyyy-MM-dd'T'HH:mm:ss"))

    .withColumn("nr_tempo_pausa_min",
        F.col("nr_tempo_pausa_min").cast(IntegerType()))
    .withColumn("nr_tempo_pausa_min",
        F.coalesce(F.col("nr_tempo_pausa_min"), F.lit(0)))

    # --- Campos derivados ---
    .withColumn("nr_duracao_turno_min",
        F.when(
            F.col("dt_fim_turno").isNotNull() & F.col("dt_inicio_turno").isNotNull(),
            F.round(
                (F.unix_timestamp(F.col("dt_fim_turno")) -
                 F.unix_timestamp(F.col("dt_inicio_turno"))) / 60.0, 2
            )
        ).otherwise(F.lit(None)))

    .withColumn("nr_duracao_produtiva_min",
        F.when(
            F.col("nr_duracao_turno_min").isNotNull(),
            F.round(F.col("nr_duracao_turno_min") - F.col("nr_tempo_pausa_min"), 2)
        ).otherwise(F.lit(None)))

    .withColumn("fl_turno_completo",
        F.when(
            F.col("dt_fim_turno").isNotNull() & F.col("dt_inicio_turno").isNotNull(),
            F.lit(1)
        ).otherwise(F.lit(0)).cast("smallint"))

    # Turno considerado normal entre 4h e 10h (240 a 600 min)
    .withColumn("fl_turno_normal",
        F.when(
            F.col("nr_duracao_turno_min").isNotNull() &
            (F.col("nr_duracao_turno_min") >= 240) &
            (F.col("nr_duracao_turno_min") <= 600),
            F.lit(1)
        ).otherwise(F.lit(0)).cast("smallint"))

    .withColumn("ds_turno",
        F.when(
            F.col("dt_inicio_turno").isNotNull(),
            F.when(F.hour(F.col("dt_inicio_turno")).between(5,  11), F.lit("MANHA"))


