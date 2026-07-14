"""
dag_pipeline_diario.py
======================
DAG principal do Contact Center Data Lakehouse.

Orquestra o pipeline completo diário:
  Bronze → Silver (18 jobs Glue em paralelo)
       ↓
  Silver → Gold Dimensoes (11 jobs em paralelo)
       ↓
  Silver → Gold Fatos Wave 1 (7 jobs em paralelo)
       ↓
  Silver → Gold Fatos Wave 2 (4 jobs em paralelo)
       ↓
  Carga Redshift (trigger da dag_carga_redshift)

Schedule: diário às 02:00 UTC
Timeout:  3 horas
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor

# ─── Configuracoes ────────────────────────────────────────────────────────────

BUCKET      = Variable.get("BUCKET_NAME",    default_var="act-cc-dev-lakehouse")
ENV         = Variable.get("ENV",            default_var="dev")
GLUE_ROLE   = Variable.get("GLUE_ROLE_ARN",  default_var="")
AWS_CONN    = "aws_default"
GLUE_REGION = "us-east-1"

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

GLUE_JOB_ARGS = {
    "--BUCKET_NAME": BUCKET,
    "--ENV":         ENV,
    "--enable-job-insights": "true",
}

# ─── Jobs por wave ───────────────────────────────────────────────────────────

BRONZE_JOBS = [
    "job-tb-avaliacao-qualidade-bronze-to-silver",
    "job-tb-campanha-bronze-to-silver",
    "job-tb-chamada-bronze-to-silver",
    "job-tb-chat-bronze-to-silver",
    "job-tb-cliente-bronze-to-silver",
    "job-tb-discagem-bronze-to-silver",
    "job-tb-endereco-cliente-bronze-to-silver",
    "job-tb-fila-atendimento-bronze-to-silver",
    "job-tb-gravacao-chamada-bronze-to-silver",
    "job-tb-interacao-ticket-bronze-to-silver",
    "job-tb-jornada-operador-bronze-to-silver",
    "job-tb-mensagem-chat-bronze-to-silver",
    "job-tb-metricas-operacionais-bronze-to-silver",
    "job-tb-operador-bronze-to-silver",
    "job-tb-skill-operador-bronze-to-silver",
    "job-tb-ticket-bronze-to-silver",
    "job-tb-ura-navegacao-bronze-to-silver",
    "job-tb-whatsapp-atendimento-bronze-to-silver",
]

GOLD_DIM_JOBS = [
    "job-dim-campanha-gold",
    "job-dim-canal-gold",
    "job-dim-categoria-ticket-gold",
    "job-dim-cliente-gold",
    "job-dim-data-gold",
    "job-dim-fila-gold",
    "job-dim-operador-gold",
    "job-dim-prioridade-ticket-gold",
    "job-dim-skill-gold",
    "job-dim-status-chamada-gold",
    "job-dim-status-ticket-gold",
]

GOLD_FATO_WAVE1_JOBS = [
    "job-fato-chamada-gold",
    "job-fato-chat-gold",
    "job-fato-discagem-gold",
    "job-fato-jornada-operador-gold",
    "job-fato-ticket-gold",
    "job-fato-ura-navegacao-gold",
    "job-fato-whatsapp-gold",
]

GOLD_FATO_WAVE2_JOBS = [
    "job-fato-interacao-ticket-gold",
    "job-fato-mensagem-chat-gold",
    "job-fato-metricas-operacionais-gold",
    "job-fato-qualidade-gold",
]


# ─── Helpers ─────────────────────────────────────────────────────────────────

def make_glue_task(dag: DAG, job_name: str, task_id: str | None = None) -> GlueJobOperator:
    """Cria um GlueJobOperator padrao para o job informado."""
    tid = task_id or f"run_{job_name.replace('-', '_')}"
    return GlueJobOperator(
        task_id=tid,
        job_name=job_name,
        script_args=GLUE_JOB_ARGS,
        aws_conn_id=AWS_CONN,
        region_name=GLUE_REGION,
        wait_for_completion=True,
        dag=dag,
    )


def make_sensor(dag: DAG, job_name: str, run_id_xcom_key: str) -> GlueJobSensor:
    """Cria um GlueJobSensor para aguardar conclusao assincrona (opcional)."""
    return GlueJobSensor(
        task_id=f"wait_{job_name.replace('-', '_')}",
        job_name=job_name,
        run_id="{{ task_instance.xcom_pull('" + run_id_xcom_key + "') }}",
        aws_conn_id=AWS_CONN,
        dag=dag,
    )


# ─── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="cc_pipeline_diario",
    description="Pipeline diario Contact Center: Bronze → Silver → Gold → Redshift",
    schedule_interval="0 2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=3),
    tags=["contact-center", "lakehouse", "glue", "producao"],
    doc_md="""
    ## Pipeline Diário — Contact Center Data Lakehouse

    Orquestra os 40 Glue Jobs em 4 waves sequenciais:

    | Wave | Jobs | Descricao |
    |------|------|-----------|
    | 1    | 18   | Bronze → Silver (todas as entidades) |
    | 2    | 11   | Silver → Gold Dimensoes |
    | 3    | 7    | Silver → Gold Fatos (independentes) |
    | 4    | 4    | Silver → Gold Fatos (dependem de fatos da wave 3) |

    Apos a wave 4, aciona a DAG `cc_carga_redshift` para carregar o Redshift.
    """,
) as dag:

    inicio = EmptyOperator(task_id="inicio_pipeline")
    fim_bronze = EmptyOperator(task_id="bronze_concluido")
    fim_dims = EmptyOperator(task_id="gold_dims_concluido")
    fim_wave1 = EmptyOperator(task_id="gold_fatos_wave1_concluido")
    fim_pipeline = EmptyOperator(task_id="pipeline_glue_concluido")

    # ── Wave 1: Bronze → Silver (todos em paralelo) ──────────────────────────
    bronze_tasks = [make_glue_task(dag, job) for job in BRONZE_JOBS]
    inicio >> bronze_tasks >> fim_bronze

    # ── Wave 2: Gold Dimensoes (todos em paralelo, apos bronze) ─────────────
    dim_tasks = [make_glue_task(dag, job) for job in GOLD_DIM_JOBS]
    fim_bronze >> dim_tasks >> fim_dims

    # ── Wave 3: Gold Fatos independentes (paralelo, apos dims) ──────────────
    wave1_tasks = [make_glue_task(dag, job) for job in GOLD_FATO_WAVE1_JOBS]
    fim_dims >> wave1_tasks >> fim_wave1

    # ── Wave 4: Gold Fatos dependentes (paralelo, apos wave 3) ──────────────
    wave2_tasks = [make_glue_task(dag, job) for job in GOLD_FATO_WAVE2_JOBS]
    fim_wave1 >> wave2_tasks >> fim_pipeline

    # ── Trigger carga Redshift ───────────────────────────────────────────────
    trigger_redshift = TriggerDagRunOperator(
        task_id="acionar_carga_redshift",
        trigger_dag_id="cc_carga_redshift",
        wait_for_completion=True,
        poke_interval=60,
    )
    fim_pipeline >> trigger_redshift
