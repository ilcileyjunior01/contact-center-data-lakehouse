# =========================================================
# JOB: job_tb_jornada_operador_bronze_to_silver.py
# PIPELINE: Bronze -> Silver
# TABELA: tb_jornada_operador
# GLUE VERSION: 4.0
# WORKER TYPE: G.1X / 2 workers
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
    StringType, IntegerType, TimestampType, LongType
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

print(f"[INFO] Job iniciado | Tabela: tb_jornada_operador | ENV: {ENV}")

BRONZE_DATABASE = "db_bronze"
BRONZE_TABLE    = "jornada"
SILVER_PATH     = f"s3://{BUCKET}/silver/qualidade/jornada/"
QUARANTINE_PATH = f"s3://{BUCKET}/quarantine/tb_jornada_operador/"
SILVER_TABLE    = "db_silver.jornada_operador"

spark.conf.set("spark.sql.catalog.glue_catalog",
    "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl",
    "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl",
    "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse",
    f"s3://{BUCKET}/silver/")

dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
    database=BRONZE_DATABASE,
    table_name=BRONZE_TABLE,
    transformation_ctx="bronze_jornada_operador",
)
df_bronze = dynamic_frame.toDF()

count_bronze = df_bronze.count()
print(f"[INFO] Registros lidos do Bronze: {count_bronze}")

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

window_dedup = Window.partitionBy("id_jornada").orderBy(F.col("dt_cdc_evento").desc())
df_dedup = (
    df_cdc
    .withColumn("_row_num", F.row_number().over(window_dedup))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num", "Op", "_timestamp")
)

print(f"[INFO] Registros apos dedup: {df_dedup.count()}")

now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

df_transformed = (
    df_dedup
    .withColumn("id_jornada",  F.col("id_jornada").cast(LongType()))
    .withColumn("id_operador", F.col("id_operador").cast(LongType()))
    .withColumn("dt_jornada",  F.to_date(F.col("dt_jornada"),  "yyyy-MM-dd"))
    .withColumn("dt_entrada",  F.to_timestamp(F.col("dt_entrada"),  "yyyy-MM-dd'T'HH:mm:ss"))
    .withColumn("dt_saida",    F.to_timestamp(F.col("dt_saida"),    "yyyy-MM-dd'T'HH:mm:ss"))
    .withColumn("nr_horas_trabalhadas",  F.col("nr_horas_trabalhadas").cast("double"))
    .withColumn("nr_chamadas_atendidas", F.col("nr_chamadas_atendidas").cast(IntegerType()))
    .withColumn("nr_tickets_resolvidos", F.col("nr_tickets_resolvidos").cast(IntegerType()))
    .withColumn("st_presenca", F.upper(F.trim(F.col("st_presenca"))))
    .withColumn("st_presenca",
        F.coalesce(F.col("st_presenca"), F.lit("DESCONHECIDO")))
    .withColumn("nr_horas_trabalhadas",
        F.coalesce(F.col("nr_horas_trabalhadas"), F.lit(0.0)))
    .withColumn("nr_chamadas_atendidas",
        F.coalesce(F.col("nr_chamadas_atendidas"), F.lit(0)))
    .withColumn("nr_tickets_resolvidos",
        F.coalesce(F.col("nr_tickets_resolvidos"), F.lit(0)))
    .withColumn("fl_presente",
        F.when(F.col("st_presenca") == "PRESENTE", F.lit(1))
         .otherwise(F.lit(0)).cast("smallint"))
    .withColumn("hash_registro",
        F.md5(F.concat_ws("|",
            F.coalesce(F.col("id_jornada").cast("string"),          F.lit("")),
            F.coalesce(F.col("id_operador").cast("string"),         F.lit("")),
            F.coalesce(F.col("dt_jornada").cast("string"),          F.lit("")),
            F.coalesce(F.col("nr_horas_trabalhadas").cast("string"),F.lit("")),
            F.coalesce(F.col("st_presenca"),                        F.lit("")),
        )))
    .withColumn("dt_ingestao_silver", F.lit(now_ts).cast(TimestampType()))
    .withColumn("ano", F.year(F.col("dt_jornada")))
    .withColumn("mes", F.month(F.col("dt_jornada")))
    .withColumn("dia", F.dayofmonth(F.col("dt_jornada")))
)

df_transformed = df_transformed.withColumn(
    "_motivo_quarentena",
    F.when(F.col("id_jornada").isNull(),  F.lit("id_jornada_nulo"))
     .when(F.col("id_operador").isNull(), F.lit("id_operador_nulo"))
     .when(F.col("dt_jornada").isNull(),  F.lit("dt_jornada_nulo"))
     .otherwise(F.lit(None).cast(StringType()))
)

df_valid      = df_transformed.filter(F.col("_motivo_quarentena").isNull()).drop("_motivo_quarentena")
df_quarantine = df_transformed.filter(F.col("_motivo_quarentena").isNotNull())

print(f"[INFO] Validos: {df_valid.count()} | Quarentena: {df_quarantine.count()}")

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

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{SILVER_TABLE} (
        id_jornada              BIGINT,
        id_operador             BIGINT,
        dt_jornada              DATE,
        dt_entrada              TIMESTAMP,
        dt_saida                TIMESTAMP,
        nr_horas_trabalhadas    DOUBLE,
        nr_chamadas_atendidas   INT,
        nr_tickets_resolvidos   INT,
        st_presenca             STRING,
        fl_presente             SMALLINT,
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

df_valid.createOrReplaceTempView("stg_jornada_operador")

spark.sql(f"""
    MERGE INTO glue_catalog.{SILVER_TABLE} AS target
    USING stg_jornada_operador AS source
    ON target.id_jornada = source.id_jornada

    WHEN MATCHED AND target.hash_registro <> source.hash_registro
    THEN UPDATE SET *

    WHEN NOT MATCHED
    THEN INSERT *
""")

print(f"[INFO] MERGE concluido: {SILVER_TABLE}")

job.commit()
print("[INFO] Job finalizado com sucesso.")
