"""
dag_carga_redshift.py
=====================
DAG de carga do Redshift Serverless.

Executa o COPY dos dados Gold (S3 Parquet) para o Redshift,
em ordem respeitando dependencias dimensoes → fatos.

Pode ser acionada:
  - Automaticamente pela dag_pipeline_diario (trigger ao final)
  - Manualmente via Airflow UI para reprocessamento

Estrategia de carga:
  - TRUNCATE + COPY (full refresh diario)
  - COPY usa IAM Role com acesso ao S3 Gold
  - Formato: PARQUET (nativo no Redshift Serverless)

── Reprocessamento ──────────────────────────────────────────────────────────────
Recebe reprocess_date via dag_run.conf (passado pela dag_pipeline_diario ou
via trigger manual). O TRUNCATE + COPY garante idempotência: pode ser executado
quantas vezes for necessário com o mesmo resultado.

Exemplo de trigger manual (só recarrega Redshift, sem rodar Glue):
  airflow dags trigger cc_carga_redshift \\
    --conf '{"reprocess_date": "2024-06-15"}'
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator

log = logging.getLogger(__name__)

# ─── Configuracoes ────────────────────────────────────────────────────────────

BUCKET          = Variable.get("BUCKET_NAME",       default_var="act-cc-dev-lakehouse")
IAM_ROLE        = Variable.get("REDSHIFT_IAM_ROLE", default_var="")
REDSHIFT_CONN   = "redshift_default"
GOLD_S3         = f"s3://{BUCKET}/gold"
SCHEMA          = "gold"

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
}


# ─── Helper: SQL de COPY ──────────────────────────────────────────────────────

def copy_sql(table: str, s3_prefix: str) -> str:
    """
    Gera o SQL de TRUNCATE + COPY para uma tabela Gold.

    Estrategia full-refresh: o TRUNCATE antes do COPY garante idempotência —
    executar novamente com os mesmos dados Gold no S3 sempre produz o mesmo
    resultado no Redshift, sem duplicatas.
    """
    return f"""
        TRUNCATE TABLE {SCHEMA}.{table};

        COPY {SCHEMA}.{table}
        FROM '{GOLD_S3}/{s3_prefix}/'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS PARQUET
        SERIALIZETOJSON;
    """


# ─── Mapa tabela → prefixo S3 ────────────────────────────────────────────────

DIMS = {
    "dim_data":               "dimensoes/dim_data",
    "dim_cliente":            "dimensoes/dim_cliente",
    "dim_operador":           "dimensoes/dim_operador",
    "dim_fila":               "dimensoes/dim_fila",
    "dim_canal":              "dimensoes/dim_canal",
    "dim_status_chamada":     "dimensoes/dim_status_chamada",
    "dim_status_ticket":      "dimensoes/dim_status_ticket",
    "dim_campanha":           "dimensoes/dim_campanha",
    "dim_categoria_ticket":   "dimensoes/dim_categoria_ticket",
    "dim_prioridade_ticket":  "dimensoes/dim_prioridade_ticket",
    "dim_skill":              "dimensoes/dim_skill",
}

FATOS = {
    "fato_chamada":               "fatos/fato_chamada",
    "fato_ticket":                "fatos/fato_ticket",
    "fato_discagem":              "fatos/fato_discagem",
    "fato_chat":                  "fatos/fato_chat",
    "fato_whatsapp":              "fatos/fato_whatsapp",
    "fato_ura_navegacao":         "fatos/fato_ura_navegacao",
    "fato_jornada_operador":      "fatos/fato_jornada_operador",
    "fato_qualidade":             "fatos/fato_qualidade",
    "fato_metricas_operacionais": "fatos/fato_metricas_operacionais",
    "fato_interacao_ticket":      "fatos/fato_interacao_ticket",
    "fato_mensagem_chat":         "fatos/fato_mensagem_chat",
}


# ─── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="cc_carga_redshift",
    description="Carga diaria Gold S3 → Redshift Serverless (TRUNCATE + COPY Parquet)",
    schedule_interval=None,  # Acionada pela dag_pipeline_diario ou manualmente
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=1),
    tags=["contact-center", "redshift", "carga"],
    doc_md="""
    ## Carga Redshift — Contact Center Data Lakehouse

    Realiza TRUNCATE + COPY de todas as tabelas Gold para o Redshift Serverless.

    **Ordem de execucao:**
    1. Log da data de reprocessamento
    2. Dimensoes (todas em paralelo — sem FK entre si)
    3. Fatos (todos em paralelo — apos todas as dimensoes)

    **Estrategia:** Full refresh com TRUNCATE + COPY.
    Idempotente: pode ser reexecutada quantas vezes for necessário.

    **Reprocessamento manual (só Redshift, sem Glue):**
    ```
    airflow dags trigger cc_carga_redshift \\
      --conf '{"reprocess_date": "2024-06-15"}'
    ```
    """,
) as dag:

    def _log_reprocess_date(**context) -> None:
        """Loga a data de reprocessamento para rastreabilidade."""
        conf = context.get("dag_run").conf or {}
        reprocess_date = conf.get("reprocess_date") or context["ds"]
        log.info(
            "Iniciando carga Redshift | reprocess_date=%s | dag_run_id=%s",
            reprocess_date,
            context["run_id"],
        )

    inicio_carga = EmptyOperator(task_id="inicio_carga_redshift")

    log_reprocess = PythonOperator(
        task_id="log_reprocess_date",
        python_callable=_log_reprocess_date,
    )

    dims_carregadas = EmptyOperator(task_id="dimensoes_carregadas")
    carga_concluida = EmptyOperator(task_id="carga_redshift_concluida")

    # ── Carga das Dimensoes (em paralelo) ────────────────────────────────────
    dim_tasks = []
    for table, s3_prefix in DIMS.items():
        task = RedshiftSQLOperator(
            task_id=f"copy_{table}",
            redshift_conn_id=REDSHIFT_CONN,
            sql=copy_sql(table, s3_prefix),
        )
        dim_tasks.append(task)

    # ── Carga dos Fatos (em paralelo, apos dimensoes) ────────────────────────
    fato_tasks = []
    for table, s3_prefix in FATOS.items():
        task = RedshiftSQLOperator(
            task_id=f"copy_{table}",
            redshift_conn_id=REDSHIFT_CONN,
            sql=copy_sql(table, s3_prefix),
        )
        fato_tasks.append(task)

    # ── Dependencias ─────────────────────────────────────────────────────────
    inicio_carga >> log_reprocess >> dim_tasks >> dims_carregadas >> fato_tasks >> carga_concluida
