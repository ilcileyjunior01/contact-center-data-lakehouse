# =========================================================
# JOB: job_tb_metricas_operacionais_bronze_to_silver.py
# PIPELINE: Bronze → Silver
# TABELA: tb_metricas_operacionais
# GLUE VERSION: 4.0
# WORKER TYPE: G.1X / 2 workers
# =========================================================
# OBSERVAÇÃO:
#   tb_metricas_operacionais é uma tabela de agregados
#   gerados pelo sistema do contact center. Não tem
#   chave natural de negócio além de dt_referencia +
#   id_fila — a deduplicação usa essa combinação.
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

print(f"[INFO] Job iniciado | Tabela: tb_metricas_operacionais | ENV: {ENV}")

BRONZE_DATABASE = "db_bronze"
BRONZE_TABLE    = "tb_metricas_operacionais"
SILVER_PATH     = f"s3://{BUCKET}/silver/operacao/metricas_operacionais/"
CHECKPOINT_KEY  = "checkpoints/tb_metricas_operacionais/watermark.json"
QUARANTINE_PATH = f"s3://{BUCKET}/quarantine/tb_metricas_operacionais/"
SILVER_TABLE    = "db_silver.metricas_operacionais"

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
        "table_name":     "tb_metricas_operacionais",
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
    transformation_ctx="bronze_tb_metricas_operacionais",
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

# Deduplicação por id_metrica (chave natural da tabela)
window_dedup = (
    Window.partitionBy("id_metrica").orderBy(F.col("dt_cdc_evento").desc())
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

    .withColumn("dt_referencia",
        F.to_timestamp(F.col("dt_referencia"), "yyyy-MM-dd'T'HH:mm:ss"))

    .withColumn("nr_chamadas_recebidas",
        F.coalesce(F.col("nr_chamadas_recebidas").cast(IntegerType()),  F.lit(0)))
    .withColumn("nr_chamadas_atendidas",
        F.coalesce(F.col("nr_chamadas_atendidas").cast(IntegerType()),  F.lit(0)))
    .withColumn("nr_chamadas_abandonadas",
        F.coalesce(F.col("nr_chamadas_abandonadas").cast(IntegerType()), F.lit(0)))
    .withColumn("nr_tma_segundos",
        F.coalesce(F.col("nr_tma_segundos").cast(IntegerType()),        F.lit(0)))
    .withColumn("nr_tme_segundos",
        F.coalesce(F.col("nr_tme_segundos").cast(IntegerType()),        F.lit(0)))
    .withColumn("nr_nivel_servico",
        F.coalesce(F.col("nr_nivel_servico").cast("double"),            F.lit(0.0)))
    .withColumn("id_fila",
        F.coalesce(F.col("id_fila"), F.lit(-1).cast("long")))

    # --- Campos derivados ---
    .withColumn("nr_taxa_atendimento",
        F.when(
            F.col("nr_chamadas_recebidas") > 0,
            F.round(
                F.col("nr_chamadas_atendidas") * 100.0 /
                F.col("nr_chamadas_recebidas"), 2
            )
        ).otherwise(F.lit(0.0)))

    .withColumn("nr_taxa_abandono",
        F.when(
            F.col("nr_chamadas_recebidas") > 0,
            F.round(
                F.col("nr_chamadas_abandonadas") * 100.0 /
                F.col("nr_chamadas_recebidas"), 2
            )
        ).otherwise(F.lit(0.0)))

    .withColumn("nr_tma_minutos",
        F.round(F.col("nr_tma_segundos") / 60.0, 2))

    .withColumn("nr_tme_minutos",
        F.round(F.col("nr_tme_segundos") / 60.0, 2))

    .withColumn("fl_meta_nivel_servico",
        F.when(F.col("nr_nivel_servico") >= 80.0, F.lit(1))
         .otherwise(F.lit(0)).cast("smallint"))

    .withColumn("fl_alto_abandono",
        F.when(F.col("nr_taxa_abandono") > 10.0, F.lit(1))
         .otherwise(F.lit(0)).cast("smallint"))

    .withColumn("hash_registro",
        F.md5(F.concat_ws("|",
            F.coalesce(F.col("id_metrica").cast("string"),              F.lit("")),
            F.coalesce(F.col("id_fila").cast("string"),                 F.lit("")),
            F.coalesce(F.col("dt_referencia").cast("string"),           F.lit("")),
            F.coalesce(F.col("nr_chamadas_recebidas").cast("string"),   F.lit("")),
            F.coalesce(F.col("nr_chamadas_atendidas").cast("string"),   F.lit("")),
            F.coalesce(F.col("nr_chamadas_abandonadas").cast("string"), F.lit("")),
            F.coalesce(F.col("nr_nivel_servico").cast("string"),        F.lit("")),
        )))

    .withColumn("dt_ingestao_silver", F.lit(now_ts).cast(TimestampType()))
    .withColumn("ano", F.year(F.col("dt_referencia")))
    .withColumn("mes", F.month(F.col("dt_referencia")))
    .withColumn("dia", F.dayofmonth(F.col("dt_referencia")))
)

df_transformed = df_transformed.withColumn(
    "_motivo_quarentena",
    F.when(F.col("id_metrica").isNull(),    F.lit("id_metrica_nulo"))
     .when(F.col("dt_referencia").isNull(), F.lit("dt_referencia_nula"))
     .when(
         F.col("nr_nivel_servico").isNotNull() &
         ((F.col("nr_nivel_servico") < 0) | (F.col("nr_nivel_servico") > 100)),
         F.lit("nr_nivel_servico_fora_do_range")
     )
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
        id_metrica                  BIGINT,
        dt_referencia               TIMESTAMP,
        id_fila                     BIGINT,
        nr_chamadas_recebidas       INT,
        nr_chamadas_atendidas       INT,
        nr_chamadas_abandonadas     INT,
        nr_tma_segundos             INT,
        nr_tma_minutos              DOUBLE,
        nr_tme_segundos             INT,
        nr_tme_minutos              DOUBLE,
        nr_nivel_servico            DOUBLE,
        nr_taxa_atendimento         DOUBLE,
        nr_taxa_abandono            DOUBLE,
        fl_meta_nivel_servico       SMALLINT,
        fl_alto_abandono            SMALLINT,
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

df_valid.createOrReplaceTempView("stg_metricas_operacionais")

spark.sql(f"""
    MERGE INTO glue_catalog.{SILVER_TABLE} AS target
    USING stg_metricas_operacionais AS source
    ON target.id_metrica = source.id_metrica
    WHEN MATCHED AND target.hash_registro <> source.hash_registro
    THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Silver: {SILVER_TABLE}")

new_watermark = df_bronze.agg(F.max("_timestamp").alias("max_ts")).collect()[0]["max_ts"]
save_watermark(new_watermark)
job.commit()
print("[INFO] Job finalizado com sucesso.")