# =========================================================
# JOB: job_tb_ticket_bronze_to_silver.py
# PIPELINE: Bronze → Silver
# TABELA: tb_ticket
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
    StructType, StructField,
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

print(f"[INFO] Job iniciado | Tabela: tb_ticket | ENV: {ENV}")

# =========================================================
# CAMINHOS S3
# =========================================================

BRONZE_DATABASE = "db_bronze"
BRONZE_TABLE    = "tb_ticket"
SILVER_PATH     = f"s3://{BUCKET}/silver/operacao/ticket/"
CHECKPOINT_KEY  = "checkpoints/tb_ticket/watermark.json"
QUARANTINE_PATH = f"s3://{BUCKET}/quarantine/tb_ticket/"
SILVER_TABLE    = "db_silver.ticket"

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
# Complementa o Job Bookmark filtrando pelo conteúdo
# do evento CDC (_timestamp), não apenas pelo arquivo.
# Necessário porque o Firehose pode agrupar eventos
# novos e antigos no mesmo arquivo Parquet.

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
        "table_name":     "tb_ticket",
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
# dos arquivos lidos.

last_watermark = get_watermark()

dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
    database=BRONZE_DATABASE,
    table_name=BRONZE_TABLE,
    transformation_ctx="bronze_tb_ticket",
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
# Interpreta a coluna Op do DMS (I/U/D) e deduplica por
# id_ticket mantendo apenas o evento mais recente do batch.

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
    .partitionBy("id_ticket")
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

now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

df_transformed = (
    df_dedup

    # --- Conversão de tipos ---
    .withColumn("dt_abertura",
        F.to_timestamp(F.col("dt_abertura"), "yyyy-MM-dd'T'HH:mm:ss"))
    .withColumn("dt_fechamento",
        F.to_timestamp(F.col("dt_fechamento"), "yyyy-MM-dd'T'HH:mm:ss"))

    # --- Normalização de strings ---
    .withColumn("st_ticket",
        F.upper(F.trim(F.col("st_ticket"))))
    .withColumn("ds_prioridade",
        F.upper(F.trim(F.col("ds_prioridade"))))
    .withColumn("ds_categoria",
        F.upper(F.trim(F.col("ds_categoria"))))
    .withColumn("nr_protocolo",
        F.trim(F.col("nr_protocolo")))

    # --- Tratamento de nulos ---
    .withColumn("id_operador_abertura",
        F.coalesce(F.col("id_operador_abertura"), F.lit(-1).cast(LongType())))
    .withColumn("st_ticket",
        F.coalesce(F.col("st_ticket"), F.lit("DESCONHECIDO")))
    .withColumn("ds_prioridade",
        F.coalesce(F.col("ds_prioridade"), F.lit("DESCONHECIDO")))
    .withColumn("ds_categoria",
        F.coalesce(F.col("ds_categoria"), F.lit("DESCONHECIDO")))

    # --- Campos derivados ---
    .withColumn("nr_tempo_resolucao_min",
        F.when(
            F.col("dt_fechamento").isNotNull() & F.col("dt_abertura").isNotNull(),
            F.round(
                (F.unix_timestamp(F.col("dt_fechamento")) -
                 F.unix_timestamp(F.col("dt_abertura"))) / 60.0, 2
            )
        ).otherwise(F.lit(None)))

    .withColumn("fl_ticket_resolvido",
        F.when(
            F.col("st_ticket").isin("FECHADO", "RESOLVIDO"),
            F.lit(1)
        ).otherwise(F.lit(0)).cast("smallint"))

    .withColumn("fl_dentro_sla",
        F.when(
            F.col("nr_tempo_resolucao_min") <= F.lit(480),  # 8 horas como SLA padrão
            F.lit(1)
        ).otherwise(F.lit(0)).cast("smallint"))

    # --- Hash de integridade ---
    .withColumn("hash_registro",
        F.md5(F.concat_ws("|",
            F.coalesce(F.col("id_ticket").cast("string"),            F.lit("")),
            F.coalesce(F.col("nr_protocolo"),                        F.lit("")),
            F.coalesce(F.col("id_cliente").cast("string"),           F.lit("")),
            F.coalesce(F.col("id_operador_abertura").cast("string"), F.lit("")),
            F.coalesce(F.col("dt_abertura").cast("string"),          F.lit("")),
            F.coalesce(F.col("dt_fechamento").cast("string"),        F.lit("")),
            F.coalesce(F.col("ds_categoria"),                        F.lit("")),
            F.coalesce(F.col("ds_prioridade"),                       F.lit("")),
            F.coalesce(F.col("st_ticket"),                           F.lit("")),
        )))

    # --- Auditoria ---
    .withColumn("dt_ingestao_silver",
        F.lit(now_ts).cast(TimestampType()))

    # --- Particionamento ---
    # Particionado por dt_abertura para facilitar consultas
    # analíticas por período no Athena.
    .withColumn("ano", F.year(F.col("dt_abertura")))
    .withColumn("mes", F.month(F.col("dt_abertura")))
    .withColumn("dia", F.dayofmonth(F.col("dt_abertura")))
)

# =========================================================
# SEPARAÇÃO VÁLIDOS × QUARENTENA
# =========================================================
# id_ticket, nr_protocolo e id_cliente são indispensáveis
# para o modelo analítico — registros sem eles vão para
# quarentena com o motivo registrado para auditoria.

df_transformed = df_transformed.withColumn(
    "_motivo_quarentena",
    F.when(F.col("id_ticket").isNull(),    F.lit("id_ticket_nulo"))
     .when(F.col("nr_protocolo").isNull(), F.lit("nr_protocolo_nulo"))
     .when(F.col("id_cliente").isNull(),   F.lit("id_cliente_nulo"))
     .when(F.col("dt_abertura").isNull(),  F.lit("dt_abertura_nula"))
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
        id_ticket               BIGINT,
        nr_protocolo            STRING,
        id_cliente              BIGINT,
        id_operador_abertura    BIGINT,
        dt_abertura             TIMESTAMP,
        dt_fechamento           TIMESTAMP,
        ds_categoria            STRING,
        ds_prioridade           STRING,
        st_ticket               STRING,
        nr_tempo_resolucao_min  DOUBLE,
        fl_ticket_resolvido     SMALLINT,
        fl_dentro_sla           SMALLINT,
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

print(f"[INFO] Tabela Iceberg verificada: {SILVER_TABLE}")

# =========================================================
# MERGE IDEMPOTENTE NA SILVER
# =========================================================
# A chave do MERGE é id_ticket.
# nr_protocolo é único no PostgreSQL mas pode chegar
# com variações de capitalização — por isso normalizamos
# antes e usamos id_ticket como chave principal do MERGE.

df_valid.createOrReplaceTempView("stg_ticket")

spark.sql(f"""
    MERGE INTO glue_catalog.{SILVER_TABLE} AS target
    USING stg_ticket AS source
    ON target.id_ticket = source.id_ticket

    WHEN MATCHED AND target.hash_registro <> source.hash_registro
    THEN UPDATE SET *

    WHEN NOT MATCHED
    THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Silver: {SILVER_TABLE}")

# =========================================================
# ATUALIZA WATERMARK
# =========================================================
# Executado somente após o MERGE com sucesso.
# Se o job falhar antes deste ponto, o watermark não
# avança e a próxima execução reprocessa o mesmo intervalo.

new_watermark = (
    df_bronze
    .agg(F.max("_timestamp").alias("max_ts"))
    .collect()[0]["max_ts"]
)

save_watermark(new_watermark)

# =========================================================
# FINALIZAÇÃO
# =========================================================
# job.commit() avança o ponteiro do Job Bookmark.
# Watermark + Bookmark + MERGE garantem juntos a
# idempotência completa do pipeline.

job.commit()
print("[INFO] Job finalizado com sucesso.")