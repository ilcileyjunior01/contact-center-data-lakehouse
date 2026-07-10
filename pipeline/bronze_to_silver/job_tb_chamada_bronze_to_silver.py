# =========================================================
# JOB: job_tb_chamada_bronze_to_silver.py
# PIPELINE: Bronze → Silver
# TABELA: tb_chamada
# GLUE VERSION: 4.0
# WORKER TYPE: G.1X / 2 workers
# =========================================================
# RECURSOS AWS NATIVOS UTILIZADOS:
#   - Glue Job Bookmarks  → controle incremental
#   - Glue Data Catalog   → leitura da tabela Bronze
#   - Apache Iceberg      → escrita idempotente na Silver
#   - CloudWatch Logs     → monitoramento via print()
#   - S3                  → quarentena de registros inválidos
# =========================================================

import sys
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    LongType, StringType, IntegerType, TimestampType
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
# A SparkSession é obtida do GlueContext — nunca criada
# diretamente no Glue Job.
# O Job Bookmark é habilitado no console do Glue em:
# Job details → Advanced properties → Job bookmark → Enable

sc           = SparkContext()
glue_context = GlueContext(sc)
spark        = glue_context.spark_session
job          = Job(glue_context)
job.init(JOB_NAME, args)

print(f"[INFO] Job iniciado | Tabela: tb_chamada | ENV: {ENV}")

# =========================================================
# CAMINHOS S3
# =========================================================

BRONZE_DATABASE = "db_bronze"
BRONZE_TABLE    = "chamada"
SILVER_PATH     = f"s3://{BUCKET}/silver/operacao/chamada/"
QUARANTINE_PATH = f"s3://{BUCKET}/quarantine/tb_chamada/"
SILVER_TABLE    = "db_silver.chamada"

# =========================================================
# CONFIGURAÇÃO ICEBERG
# =========================================================
# Aplicada sobre a sessão existente do Glue,
# antes de qualquer operação com tabelas Iceberg.

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
# LEITURA INCREMENTAL VIA GLUE JOB BOOKMARK
# =========================================================
# O Glue Job Bookmark rastreia automaticamente os arquivos
# já processados no S3 Bronze. A cada execução, o Glue
# lê apenas os arquivos novos desde a última execução
# bem-sucedida (após job.commit()).
#
# A leitura é feita via Glue Data Catalog (DynamicFrame)
# e convertida para DataFrame Spark para as transformações.

dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
    database=BRONZE_DATABASE,
    table_name=BRONZE_TABLE,
    transformation_ctx="bronze_tb_chamada",   # contexto do bookmark
)

df_bronze = dynamic_frame.toDF()

count_bronze = df_bronze.count()
print(f"[INFO] Registros lidos do Bronze (incremental via Bookmark): {count_bronze}")

if count_bronze == 0:
    print("[INFO] Nenhum registro novo. Job encerrado.")
    job.commit()
    sys.exit(0)

# =========================================================
# PROCESSAMENTO CDC
# =========================================================
# O DMS injeta a coluna Op (I/U/D) em cada evento.
# Quando chegam múltiplos eventos para o mesmo id_chamada
# no mesmo batch, mantemos apenas o mais recente.

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
    .partitionBy("id_chamada")
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
    # IDs: cast explícito de STRING (bronze/CSV) para BIGINT
    .withColumn("id_chamada", F.col("id_chamada").cast(LongType()))
    .withColumn("id_cliente", F.col("id_cliente").cast(LongType()))
    .withColumn("id_operador", F.col("id_operador").cast(LongType()))
    .withColumn("id_fila", F.col("id_fila").cast(LongType()))
    .withColumn("dt_inicio",
        F.to_timestamp(F.col("dt_inicio"), "yyyy-MM-dd'T'HH:mm:ss"))
    .withColumn("dt_fim",
        F.to_timestamp(F.col("dt_fim"), "yyyy-MM-dd'T'HH:mm:ss"))
    .withColumn("nr_duracao_segundos",
        F.col("nr_duracao_segundos").cast(IntegerType()))

    # --- Normalização de strings ---
    .withColumn("st_chamada",
        F.upper(F.trim(F.col("st_chamada"))))
    .withColumn("tp_chamada",
        F.upper(F.trim(F.col("tp_chamada"))))
    .withColumn("nr_telefone_origem",
        F.regexp_replace(F.col("nr_telefone_origem"), r"[^\d+]", ""))
    .withColumn("nr_telefone_destino",
        F.regexp_replace(F.col("nr_telefone_destino"), r"[^\d+]", ""))

    # --- Tratamento de nulos ---
    .withColumn("id_operador",
        F.coalesce(F.col("id_operador"), F.lit(-1).cast(LongType())))
    .withColumn("id_fila",
        F.coalesce(F.col("id_fila"), F.lit(-1).cast(LongType())))
    .withColumn("st_chamada",
        F.coalesce(F.col("st_chamada"), F.lit("DESCONHECIDO")))
    .withColumn("tp_chamada",
        F.coalesce(F.col("tp_chamada"), F.lit("DESCONHECIDO")))
    .withColumn("nr_duracao_segundos",
        F.coalesce(F.col("nr_duracao_segundos"), F.lit(0)))
    .withColumn("nr_telefone_origem",
        F.coalesce(F.col("nr_telefone_origem"), F.lit("")))
    .withColumn("nr_telefone_destino",
        F.coalesce(F.col("nr_telefone_destino"), F.lit("")))

    # --- Campos derivados ---
    .withColumn("fl_duracao_valida",
        F.when(
            (F.col("nr_duracao_segundos") > 0) &
            (F.col("dt_fim") > F.col("dt_inicio")),
            F.lit(1)
        ).otherwise(F.lit(0)).cast("smallint"))

    .withColumn("fl_chamada_completa",
        F.when(
            F.col("dt_fim").isNotNull() & F.col("dt_inicio").isNotNull(),
            F.lit(1)
        ).otherwise(F.lit(0)).cast("smallint"))

    .withColumn("nr_duracao_minutos",
        F.round(F.col("nr_duracao_segundos") / 60.0, 2))

    # --- Hash de integridade ---
    # Usado no MERGE para evitar re-escrita de registros
    # que não mudaram de fato, mesmo com evento CDC UPDATE.
    .withColumn("hash_registro",
        F.md5(F.concat_ws("|",
            F.coalesce(F.col("id_chamada").cast("string"),          F.lit("")),
            F.coalesce(F.col("id_cliente").cast("string"),          F.lit("")),
            F.coalesce(F.col("id_operador").cast("string"),         F.lit("")),
            F.coalesce(F.col("id_fila").cast("string"),             F.lit("")),
            F.coalesce(F.col("dt_inicio").cast("string"),           F.lit("")),
            F.coalesce(F.col("dt_fim").cast("string"),              F.lit("")),
            F.coalesce(F.col("nr_duracao_segundos").cast("string"), F.lit("")),
            F.coalesce(F.col("st_chamada"),                         F.lit("")),
            F.coalesce(F.col("tp_chamada"),                         F.lit("")),
        )))

    # --- Auditoria ---
    .withColumn("dt_ingestao_silver",
        F.lit(now_ts).cast(TimestampType()))

    # --- Particionamento ---
    .withColumn("ano", F.year(F.col("dt_inicio")))
    .withColumn("mes", F.month(F.col("dt_inicio")))
    .withColumn("dia", F.dayofmonth(F.col("dt_inicio")))
)

# =========================================================
# SEPARAÇÃO VÁLIDOS × QUARENTENA
# =========================================================

df_transformed = df_transformed.withColumn(
    "_motivo_quarentena",
    F.when(F.col("id_chamada").isNull(),     F.lit("id_chamada_nulo"))
     .when(F.col("id_cliente").isNull(),     F.lit("id_cliente_nulo"))
     .when(F.col("dt_inicio").isNull(),      F.lit("dt_inicio_nulo"))
     .when(F.col("nr_duracao_segundos") < 0, F.lit("duracao_negativa"))
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
        id_chamada              BIGINT,
        id_cliente              BIGINT,
        id_operador             BIGINT,
        id_fila                 BIGINT,
        nr_telefone_origem      STRING,
        nr_telefone_destino     STRING,
        dt_inicio               TIMESTAMP,
        dt_fim                  TIMESTAMP,
        nr_duracao_segundos     INT,
        st_chamada              STRING,
        tp_chamada              STRING,
        fl_duracao_valida       SMALLINT,
        fl_chamada_completa     SMALLINT,
        nr_duracao_minutos      DOUBLE,
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
# INSERT: registro novo (id_chamada não existe na Silver)
# UPDATE: registro existe mas hash mudou (dado alterado)
# Registros com mesmo hash não são reescritos.

df_valid.createOrReplaceTempView("stg_chamada")

spark.sql(f"""
    MERGE INTO glue_catalog.{SILVER_TABLE} AS target
    USING stg_chamada AS source
    ON target.id_chamada = source.id_chamada

    WHEN MATCHED AND target.hash_registro <> source.hash_registro
    THEN UPDATE SET *

    WHEN NOT MATCHED
    THEN INSERT *
""")

print(f"[INFO] MERGE concluído na Silver: {SILVER_TABLE}")

# =========================================================
# FINALIZAÇÃO
# =========================================================
# O job.commit() sinaliza ao Glue que a execução foi
# bem-sucedida e avança o ponteiro do Job Bookmark.
# Se o job falhar antes deste ponto, o Bookmark não avança
# e a próxima execução reprocessa o mesmo intervalo.

job.commit()
print("[INFO] Job finalizado com sucesso.")