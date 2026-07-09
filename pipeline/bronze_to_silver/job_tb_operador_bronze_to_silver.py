# =========================================================
# JOB: job_tb_operador_bronze_to_silver.py
# PIPELINE: Bronze → Silver
# TABELA: tb_operador
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
# OBSERVAÇÃO:
#   tb_operador possui auto-referência (id_supervisor → id_operador).
#   Na Silver isso é preservado como coluna simples, sem FK física,
#   pois o modelo analítico resolve a hierarquia via JOIN ou
#   window function nas camadas superiores (Gold/Athena).
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
    LongType, StringType, TimestampType, DateType
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

print(f"[INFO] Job iniciado | Tabela: tb_operador | ENV: {ENV}")

# =========================================================
# CAMINHOS S3
# =========================================================

BRONZE_DATABASE = "db_bronze"
BRONZE_TABLE    = "tb_operador"
SILVER_PATH     = f"s3://{BUCKET}/silver/cadastro/operador/"
CHECKPOINT_KEY  = "checkpoints/tb_operador/watermark.json"
QUARANTINE_PATH = f"s3://{BUCKET}/quarantine/tb_operador/"
SILVER_TABLE    = "db_silver.operador"

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
# tb_operador é uma tabela de cadastro com baixo volume
# de atualizações, mas mudanças como troca de supervisor,
# alteração de status ou admissão de novos operadores
# precisam ser capturadas corretamente pelo CDC.

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
        "table_name":     "tb_operador",
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

last_watermark = get_watermark()

dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
    database=BRONZE_DATABASE,
    table_name=BRONZE_TABLE,
    transformation_ctx="bronze_tb_operador",
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
# A auto-referência id_supervisor → id_operador pode gerar
# eventos CDC encadeados no mesmo batch (ex: supervisor
# atualizado antes do operador). A deduplicação por
# dt_cdc_evento garante que processamos sempre o estado
# mais recente de cada operador.

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
    .partitionBy("id_operador")
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
# tb_operador contém dados pessoais (PII) — ds_email e
# ds_login são mascarados na Silver em conformidade com
# a LGPD. O dado original permanece apenas no Bronze
# com acesso restrito via Lake Formation.

now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

df_transformed = (
    df_dedup

    # --- Conversão de tipos ---
    .withColumn("dt_admissao",
        F.to_date(F.col("dt_admissao"), "yyyy-MM-dd"))

    # --- Normalização de strings ---
    .withColumn("nm_operador",
        F.upper(F.trim(F.col("nm_operador"))))
    .withColumn("st_operador",
        F.upper(F.trim(F.col("st_operador"))))
    .withColumn("ds_email",
        F.lower(F.trim(F.col("ds_email"))))
    .withColumn("ds_login",
        F.lower(F.trim(F.col("ds_login"))))

    # --- Tratamento de nulos ---
    .withColumn("st_operador",
        F.coalesce(F.col("st_operador"), F.lit("A")))
    .withColumn("id_supervisor",
        F.coalesce(F.col("id_supervisor"), F.lit(-1).cast(LongType())))
    .withColumn("ds_email",
        F.coalesce(F.col("ds_email"), F.lit("")))
    .withColumn("ds_login",
        F.coalesce(F.col("ds_login"), F.lit("")))

    # --- Mascaramento PII (LGPD) ---
    # ds_email: mantém apenas o domínio
    # ds_login: mantém apenas os 3 primeiros caracteres
    .withColumn("ds_email_mascarado",
        F.when(
            F.col("ds_email").contains("@"),
            F.concat(F.lit("***@"), F.element_at(F.split(F.col("ds_email"), "@"), 2))
        ).otherwise(F.lit("")))

    .withColumn("ds_login_mascarado",
        F.when(
            F.length(F.col("ds_login")) > 0,
            F.concat(
                F.substring(F.col("ds_login"), 1, 3),
                F.lit("***")
            )
        ).otherwise(F.lit("")))

    # --- Remove colunas PII originais da Silver ---
    .drop("ds_email", "ds_login")

    # --- Campos derivados ---
    .withColumn("fl_operador_ativo",
        F.when(F.col("st_operador") == "A", F.lit(1))
         .otherwise(F.lit(0)).cast("smallint"))

    .withColumn("fl_tem_supervisor",
        F.when(
            F.col("id_supervisor") != F.lit(-1),
            F.lit(1)
        ).otherwise(F.lit(0)).cast("smallint"))

    .withColumn("nr_dias_casa",
        F.when(
            F.col("dt_admissao").isNotNull(),
            F.datediff(F.current_date(), F.col("dt_admissao"))
        ).otherwise(F.lit(None)))

    .withColumn("ds_faixa_tempo_casa",
        F.when(F.col("nr_dias_casa") < 90,   F.lit("ATE_3_MESES"))
         .when(F.col("nr_dias_casa") < 180,  F.lit("3_A_6_MESES"))
         .when(F.col("nr_dias_casa") < 365,  F.lit("6_A_12_MESES"))
         .when(F.col("nr_dias_casa") < 730,  F.lit("1_A_2_ANOS"))
         .when(F.col("nr_dias_casa") >= 730, F.lit("ACIMA_2_ANOS"))
         .otherwise(F.lit("DESCONHECIDO")))

    # --- Hash de integridade ---
    .withColumn("hash_registro",
        F.md5(F.concat_ws("|",
            F.coalesce(F.col("id_operador").cast("string"),   F.lit("")),
            F.coalesce(F.col("nm_operador"),                  F.lit("")),
            F.coalesce(F.col("ds_email_mascarado"),           F.lit("")),
            F.coalesce(F.col("ds_login_mascarado"),           F.lit("")),
            F.coalesce(F.col("dt_admissao").cast("string"),   F.lit("")),
            F.coalesce(F.col("st_operador"),                  F.lit("")),
            F.coalesce(F.col("id_supervisor").cast("string"), F.lit("")),
        )))

    # --- Auditoria ---
    .withColumn("dt_ingestao_silver",
        F.lit(now_ts).cast(TimestampType()))

    # --- Particionamento ---
    # Particionado por ano/mes da admissão para facilitar
    # análises de crescimento e turnover da equipe.
    .withColumn("ano_admissao",
        F.year(F.col("dt_admissao")))
    .withColumn("mes_admissao",
        F.month(F.col("dt_admissao")))
)

# =========================================================
# SEPARAÇÃO VÁLIDOS × QUARENTENA
# =========================================================
# id_operador e nm_operador são obrigatórios.
# ds_login_mascarado vazio indica problema na origem
# pois é UNIQUE NOT NULL no PostgreSQL.

df_transformed = df_transformed.withColumn(
    "_motivo_quarentena",
    F.when(F.col("id_operador").isNull(),         F.lit("id_operador_nulo"))
     .when(F.col("nm_operador").isNull(),         F.lit("nm_operador_nulo"))
     .when(F.col("ds_login_mascarado") == "",     F.lit("ds_login_vazio"))
     .when(F.col("dt_admissao").isNull(),         F.lit("dt_admissao_nula"))
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
        id_operador           BIGINT,
        nm_operador           STRING,
        ds_email_mascarado    STRING,
        ds_login_mascarado    STRING,
        dt_admissao           DATE,
        st_operador           STRING,
        id_supervisor         BIGINT,
        fl_operador_ativo     SMALLINT,
        fl_tem_supervisor     SMALLINT,
        nr_dias_casa          INT,
        ds_faixa_tempo_casa   STRING,
        dt_cdc_evento         TIMESTAMP,
        op_cdc                STRING,
        hash_registro         STRING,
        dt_ingestao_silver    TIMESTAMP,
        ano_admissao          INT,
        mes_admissao          INT
    )
    USING iceberg
    LOCATION '{SILVER_PATH}'
    PARTITIONED BY (ano_admissao, mes_admissao)
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
# A chave do MERGE é id_operador.
# Mudanças de supervisor, status ou dados cadastrais
# são capturadas pelo hash_registro e atualizadas
# corretamente via UPDATE SET *.

df_valid.createOrReplaceTempView("stg_operador")

spark.sql(f"""
    MERGE INTO glue_catalog.{SILVER_TABLE} AS target
    USING stg_operador AS source
    ON target.id_operador = source.id_operador

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