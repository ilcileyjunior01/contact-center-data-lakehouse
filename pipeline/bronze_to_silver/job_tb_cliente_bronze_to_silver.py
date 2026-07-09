# =========================================================
# JOB: job_tb_cliente_bronze_to_silver.py
# PIPELINE: Bronze → Silver
# TABELA: tb_cliente
# GLUE VERSION: 4.0
# WORKER TYPE: G.1X / 2 workers
# =========================================================
# RECURSOS AWS NATIVOS UTILIZADOS:
#   - Glue Job Bookmarks  → controle de arquivos processados
#   - Watermark (S3/JSON) → controle incremental por timestamp CDC
#   - Glue Data Catalog   → leitura da tabela Bronze
#   - Apache Iceberg      → escrita idempotente na Silver
#   - CloudWatch Logs     → monitoramento via print()
#   - S3                  → quarentena de registros inválidos
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
from pyspark.sql.types import (
    LongType, StringType, TimestampType
)

# =========================================================
# ARGUMENTOS DO GLUE JOB
# =========================================================
# Configurar no console do Glue em:
# Job details → Job parameters
#
# --BUCKET_NAME    act-cc-dev-lakehouse
# --ENV            dev

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "BUCKET_NAME",
    "ENV",
])

JOB_NAME = args["JOB_NAME"]
BUCKET   = args["BUCKET_NAME"]
ENV      = args["ENV"]

# =========================================================
# INICIALIZAÇÃO DO GLUE
# =========================================================

sc           = SparkContext()
glue_context = GlueContext(sc)
spark        = glue_context.spark_session
job          = Job(glue_context)
job.init(JOB_NAME, args)

print(f"[INFO] Job iniciado | Tabela: tb_cliente | ENV: {ENV}")

# =========================================================
# CAMINHOS S3
# =========================================================

BRONZE_DATABASE = "db_bronze"
BRONZE_TABLE    = "tb_cliente"
SILVER_PATH     = f"s3://{BUCKET}/silver/cadastro/cliente/"
CHECKPOINT_KEY  = "checkpoints/tb_cliente/watermark.json"
QUARANTINE_PATH = f"s3://{BUCKET}/quarantine/tb_cliente/"
SILVER_TABLE    = "db_silver.cliente"

# =========================================================
# CONFIGURAÇÃO ICEBERG
# =========================================================

spark.conf.set(
    "spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
)
spark.conf.set(
    "spark.sql.catalog.glue_catalog",
    "org.apache.iceberg.spark.SparkCatalog"
)
spark.conf.set(
    "spark.sql.catalog.glue_catalog.catalog-impl",
    "org.apache.iceberg.aws.glue.GlueCatalog"
)
spark.conf.set(
    "spark.sql.catalog.glue_catalog.io-impl",
    "org.apache.iceberg.aws.s3.S3FileIO"
)
spark.conf.set(
    "spark.sql.catalog.glue_catalog.warehouse",
    f"s3://{BUCKET}/silver/"
)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# =========================================================
# WATERMARK
# =========================================================
# tb_cliente é uma tabela de cadastro — atualizações
# são menos frequentes que tb_chamada ou tb_ticket, mas
# o watermark continua necessário para garantir que
# atualizações cadastrais (ex: mudança de st_cliente)
# sejam capturadas corretamente pelo CDC.

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
        "table_name":     "tb_cliente",
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

# =========================================================
# LEITURA INCREMENTAL
# =========================================================
# Job Bookmark controla quais arquivos já foram lidos.
# Watermark filtra registros pelo timestamp CDC dentro
# dos arquivos lidos, necessário porque o Firehose pode
# agrupar eventos de diferentes períodos no mesmo arquivo.

last_watermark = get_watermark()

dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
    database=BRONZE_DATABASE,
    table_name=BRONZE_TABLE,
    transformation_ctx="bronze_tb_cliente",
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

# =========================================================
# PROCESSAMENTO CDC
# =========================================================
# Deduplica por id_cliente mantendo apenas o evento
# mais recente do batch. Importante para tb_cliente pois
# uma atualização cadastral pode gerar múltiplos eventos
# CDC no mesmo arquivo (ex: troca de e-mail + telefone).

df_cdc = (
    df_bronze
    .withColumn("dt_cdc_evento",
        F.to_timestamp(F.col("_timestamp")))
    .withColumn("op_cdc",
        F.when(F.col("Op") == "I", F.lit("INSERT"))
         .when(F.col("Op") == "U", F.lit("UPDATE"))
         .when(F.col("Op") == "D", F.lit("DELETE"))
         .otherwise(F.lit("UNKNOWN")))
)

window_dedup = (
    Window
    .partitionBy("id_cliente")
    .orderBy(F.col("dt_cdc_evento").desc())
)

df_dedup = (
    df_cdc
    .withColumn("_row_num", F.row_number().over(window_dedup))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num", "Op", "_timestamp")
)

print(f"[INFO] Registros após deduplicação CDC: {df_dedup.count()}")

# =========================================================
# TRANSFORMAÇÕES
# =========================================================
# tb_cliente contém dados pessoais (PII) — nr_documento,
# ds_email e nr_telefone são mascarados na Silver para
# conformidade com a LGPD. O dado original permanece
# apenas no Bronze com acesso restrito via Lake Formation.

now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

df_transformed = (
    df_dedup

    # --- Conversão de tipos ---
    .withColumn("dt_cadastro",
        F.to_timestamp(F.col("dt_cadastro"), "yyyy-MM-dd'T'HH:mm:ss"))

    # --- Normalização de strings ---
    .withColumn("nm_cliente",
        F.upper(F.trim(F.col("nm_cliente"))))
    .withColumn("st_cliente",
        F.upper(F.trim(F.col("st_cliente"))))
    .withColumn("ds_email",
        F.lower(F.trim(F.col("ds_email"))))

    # --- Limpeza de documentos e telefones ---
    .withColumn("nr_documento",
        F.regexp_replace(F.col("nr_documento"), r"[^\d]", ""))
    .withColumn("nr_telefone",
        F.regexp_replace(F.col("nr_telefone"), r"[^\d+]", ""))

    # --- Tratamento de nulos ---
    .withColumn("st_cliente",
        F.coalesce(F.col("st_cliente"), F.lit("A")))
    .withColumn("ds_email",
        F.coalesce(F.col("ds_email"), F.lit("")))
    .withColumn("nr_telefone",
        F.coalesce(F.col("nr_telefone"), F.lit("")))
    .withColumn("nr_documento",
        F.coalesce(F.col("nr_documento"), F.lit("")))

    # --- Mascaramento PII (LGPD) ---
    # nr_documento: mantém apenas os 3 primeiros e 2 últimos dígitos
    # ds_email: mantém apenas o domínio
    # nr_telefone: mantém apenas os 4 últimos dígitos
    .withColumn("nr_documento_mascarado",
        F.when(
            F.length(F.col("nr_documento")) > 0,
            F.concat(
                F.substring(F.col("nr_documento"), 1, 3),
                F.lit("*****"),
                F.substring(F.col("nr_documento"), -2, 2)
            )
        ).otherwise(F.lit("")))

    .withColumn("ds_email_mascarado",
        F.when(
            F.col("ds_email").contains("@"),
            F.concat(F.lit("***@"), F.element_at(F.split(F.col("ds_email"), "@"), 2))
        ).otherwise(F.lit("")))

    .withColumn("nr_telefone_mascarado",
        F.when(
            F.length(F.col("nr_telefone")) > 0,
            F.concat(F.lit("******"), F.substring(F.col("nr_telefone"), -4, 4))
        ).otherwise(F.lit("")))

    # --- Campos derivados ---
    .withColumn("fl_cliente_ativo",
        F.when(F.col("st_cliente") == "A", F.lit(1))
         .otherwise(F.lit(0)).cast("smallint"))

    .withColumn("fl_tem_email",
        F.when(
            F.col("ds_email").isNotNull() & (F.length(F.col("ds_email")) > 0),
            F.lit(1)
        ).otherwise(F.lit(0)).cast("smallint"))

    .withColumn("fl_tem_telefone",
        F.when(
            F.col("nr_telefone").isNotNull() & (F.length(F.col("nr_telefone")) > 0),
            F.lit(1)
        ).otherwise(F.lit(0)).cast("smallint"))

    .withColumn("fl_tem_documento",
        F.when(
            F.col("nr_documento").isNotNull() & (F.length(F.col("nr_documento")) > 0),
            F.lit(1)
        ).otherwise(F.lit(0)).cast("smallint"))

    # --- Remove colunas PII originais da Silver ---
    # Os dados originais ficam apenas no Bronze.
    .drop("nr_documento", "ds_email", "nr_telefone")

    # --- Hash de integridade ---
    .withColumn("hash_registro",
        F.md5(F.concat_ws("|",
            F.coalesce(F.col("id_cliente").cast("string"),     F.lit("")),
            F.coalesce(F.col("nm_cliente"),                    F.lit("")),
            F.coalesce(F.col("nr_documento_mascarado"),        F.lit("")),
            F.coalesce(F.col("ds_email_mascarado"),            F.lit("")),
            F.coalesce(F.col("nr_telefone_mascarado"),         F.lit("")),
            F.coalesce(F.col("st_cliente"),                    F.lit("")),
            F.coalesce(F.col("dt_cadastro").cast("string"),    F.lit("")),
        )))

    # --- Auditoria ---
    .withColumn("dt_ingestao_silver",
        F.lit(now_ts).cast(TimestampType()))

    # --- Particionamento ---
    # Particionado por ano/mes do cadastro para facilitar
    # análises de crescimento da base de clientes no Athena.
    .withColumn("ano", F.year(F.col("dt_cadastro")))
    .withColumn("mes", F.month(F.col("dt_cadastro")))
)

# =========================================================
# SEPARAÇÃO VÁLIDOS × QUARENTENA
# =========================================================
# id_cliente e nm_cliente são obrigatórios.
# dt_cadastro nulo indica problema na origem.

df_transformed = df_transformed.withColumn(
    "_motivo_quarentena",
    F.when(F.col("id_cliente").isNull(),  F.lit("id_cliente_nulo"))
     .when(F.col("nm_cliente").isNull(),  F.lit("nm_cliente_nulo"))
     .when(F.col("dt_cadastro").isNull(), F.lit("dt_cadastro_nulo"))
     .otherwise(F.lit(None).cast(StringType()))
)

df_valid      = df_transformed.filter(F.col("_motivo_quarentena").isNull()).drop("_motivo_quarentena")
df_quarantine = df_transformed.filter(F.col("_motivo_quarentena").isNotNull())

print(f"[INFO] Registros válidos:       {df_valid.count()}")
print(f"[INFO] Registros em quarentena: {df_quarantine.count()}")

# =========================================================
# QUARENTENA
# =========================================================

if not df_quarantine.rdd.isEmpty():
    (
        df_quarantine
        .withColumn("ano_ingestao", F.year(F.current_timestamp()))
        .withColumn("mes_ingestao", F.month(F.current_timestamp()))
        .withColumn("dia_ingestao", F.dayofmonth(F.current_timestamp()))
        .write
        .mode("append")
        .partitionBy("ano_ingestao", "mes_ingestao", "dia_ingestao")
        .parquet(QUARANTINE_PATH)
    )
    print("[INFO] Registros de quarentena gravados.")

# =========================================================
# CRIAÇÃO DA TABELA ICEBERG (idempotente)
# =========================================================

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{SILVER_TABLE} (
        id_cliente                BIGINT,
        nm_cliente                STRING,
        nr_documento_mascarado    STRING,
        ds_email_mascarado        STRING,
        nr_telefone_mascarado     STRING,
        dt_cadastro               TIMESTAMP,
        st_cliente                STRING,
        fl_cliente_ativo          SMALLINT,
        fl_tem_email              SMALLINT,
        fl_tem_telefone           SMALLINT,
        fl_tem_documento          SMALLINT,
        dt_cdc_evento             TIMESTAMP,
        op_cdc                    STRING,
        hash_registro             STRING,
        dt_ingestao_silver        TIMESTAMP,
        ano                       INT,
        mes                       INT
    )
    USING iceberg
    LOCATION '{SILVER_PATH}'
    PARTITIONED BY (ano, mes)
    TBLPROPERTIES (
        'format-version'                  = '2',
        'write.format.default'            = 'parquet',
        'write.parquet.compression-codec' = 'snappy',
        'write.target-file-size-bytes'    = '134217728'
    )
""")

print(f"[INFO] Tabela Iceberg verificada: {SILVER_TABLE}")

# =========================================================
# MERGE IDEMPOTENTE NA SILVER
# =========================================================

df_valid.createOrReplaceTempView("stg_cliente")

spark.sql(f"""
    MERGE INTO glue_catalog.{SILVER_TABLE} AS target
    USING stg_cliente AS source
    ON target.id_cliente = source.id_cliente

    WHEN MATCHED AND target.hash_registro <> source.hash_registro
    THEN UPDATE SET *

    WHEN NOT MATCHED
    THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Silver: {SILVER_TABLE}")

# =========================================================
# ATUALIZA WATERMARK
# =========================================================

new_watermark = (
    df_bronze
    .agg(F.max("_timestamp").alias("max_ts"))
    .collect()[0]["max_ts"]
)

save_watermark(new_watermark)

# =========================================================
# FINALIZAÇÃO
# =========================================================

job.commit()
print("[INFO] Job finalizado com sucesso.")