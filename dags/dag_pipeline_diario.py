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

── Reprocessamento ──────────────────────────────────────────────────────────────
Para reprocessar sem buscar dados na origem, acione manualmente com:

  reprocess_date : data a reprocessar (YYYY-MM-DD). Vazio = usa data lógica.
  start_layer    : camada de entrada do reprocessamento.

    "bronze"   → pipeline completo (Bronze→Silver→Gold→Redshift)
    "silver"   → pula ingestão; reprocessa a partir dos dados já no Bronze S3
    "gold"     → pula Bronze e Silver; reprocessa só Gold→Redshift
    "redshift" → só recarrega o Redshift a partir dos dados Gold já no S3

Exemplo via CLI:
  airflow dags trigger cc_pipeline_diario \\
    --conf '{"reprocess_date": "2024-06-15", "start_layer": "silver"}'
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.utils.trigger_rule import TriggerRule

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
    """
    Cria um GlueJobOperator padrao para o job informado.

    Passa --REPROCESS_DATE para o script Glue, permitindo que o job filtre
    e reprocesse apenas a particao da data informada — sem buscar dados na origem.
    O valor é resolvido em tempo de execução via template Jinja:
      - se params.reprocess_date foi informado, usa esse valor;
      - caso contrário, usa {{ ds }} (data lógica da execução).
    """
    tid = task_id or f"run_{job_name.replace('-', '_')}"
    return GlueJobOperator(
        task_id=tid,
        job_name=job_name,
        script_args={
            "--BUCKET_NAME":          BUCKET,
            "--ENV":                  ENV,
            "--enable-job-insights":  "true",
            "--REPROCESS_DATE":       "{{ params.reprocess_date or ds }}",
        },
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


def _gate(layers: list[str], **context) -> bool:
    """
    Retorna True se a camada atual está inclusa nas camadas permitidas.
    Usado pelos ShortCircuitOperators para pular waves desnecessárias.
    """
    return context["params"]["start_layer"] in layers


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
    params={
        "reprocess_date": Param(
            default="",
            type="string",
            description=(
                "Data a reprocessar (YYYY-MM-DD). "
                "Vazio = usa a data lógica da execução ({{ ds }}). "
                "Os Glue Jobs usam esse valor para filtrar apenas a partição da data informada, "
                "sem buscar dados na origem."
            ),
        ),
        "start_layer": Param(
            default="bronze",
            type="string",
            enum=["bronze", "silver", "gold", "redshift"],
            description=(
                "Camada de entrada do pipeline:\n"
                "  bronze   → pipeline completo (Bronze→Silver→Gold→Redshift)\n"
                "  silver   → pula ingestão; usa dados já no Bronze S3\n"
                "  gold     → pula Bronze e Silver; usa dados já no Silver S3\n"
                "  redshift → só recarrega o Redshift; usa dados já no Gold S3"
            ),
        ),
    },
    doc_md="""
    ## Pipeline Diário — Contact Center Data Lakehouse

    Orquestra os 40 Glue Jobs em 4 waves sequenciais:

    | Wave | Jobs | Descricao |
    |------|------|-----------|
    | 1    | 18   | Bronze → Silver (todas as entidades) |
    | 2    | 11   | Silver → Gold Dimensoes |
    | 3    | 7    | Silver → Gold Fatos (independentes) |
    | 4    | 4    | Silver → Gold Fatos (dependem de fatos da wave 3) |

    Após a wave 4, aciona a DAG `cc_carga_redshift` para carregar o Redshift.

    ### Reprocessamento
    Acione manualmente com `reprocess_date` e `start_layer` para reprocessar
    uma data específica sem buscar dados na origem.
    """,
) as dag:

    inicio = EmptyOperator(task_id="inicio_pipeline")

    # ── Gate Bronze ──────────────────────────────────────────────────────────
    # Permite a wave Bronze→Silver apenas se start_layer="bronze".
    # ignore_downstream_trigger_rules=False: quando retorna False, marca apenas
    # os downstream com trigger_rule=ALL_SUCCESS como SKIPPED — os próximos gates
    # (com trigger_rule=ALL_DONE) continuam executando normalmente.
    gate_bronze = ShortCircuitOperator(
        task_id="gate_bronze",
        python_callable=_gate,
        op_kwargs={"layers": ["bronze"]},
        ignore_downstream_trigger_rules=False,
    )

    fim_bronze = EmptyOperator(task_id="bronze_concluido")

    # ── Gate Silver/Gold Dims ─────────────────────────────────────────────────
    # trigger_rule=ALL_DONE: executa mesmo que o gate_bronze tenha retornado False
    # (bronze tasks estarão SKIPPED, fim_bronze estará SKIPPED).
    gate_silver = ShortCircuitOperator(
        task_id="gate_silver",
        python_callable=_gate,
        op_kwargs={"layers": ["bronze", "silver"]},
        trigger_rule=TriggerRule.ALL_DONE,
        ignore_downstream_trigger_rules=False,
    )

    fim_dims = EmptyOperator(task_id="gold_dims_concluido")

    # ── Gate Gold Fatos ───────────────────────────────────────────────────────
    gate_gold = ShortCircuitOperator(
        task_id="gate_gold",
        python_callable=_gate,
        op_kwargs={"layers": ["bronze", "silver", "gold"]},
        trigger_rule=TriggerRule.ALL_DONE,
        ignore_downstream_trigger_rules=False,
    )

    fim_wave1   = EmptyOperator(task_id="gold_fatos_wave1_concluido")
    fim_pipeline = EmptyOperator(task_id="pipeline_glue_concluido")

    # ── Wave 1: Bronze → Silver (todos em paralelo) ──────────────────────────
    bronze_tasks = [make_glue_task(dag, job) for job in BRONZE_JOBS]

    # ── Wave 2: Gold Dimensoes (todos em paralelo) ───────────────────────────
    dim_tasks = [make_glue_task(dag, job) for job in GOLD_DIM_JOBS]

    # ── Wave 3: Gold Fatos independentes (paralelo) ──────────────────────────
    wave1_tasks = [make_glue_task(dag, job) for job in GOLD_FATO_WAVE1_JOBS]

    # ── Wave 4: Gold Fatos dependentes (paralelo) ────────────────────────────
    wave2_tasks = [make_glue_task(dag, job) for job in GOLD_FATO_WAVE2_JOBS]

    # ── Trigger carga Redshift ───────────────────────────────────────────────
    # trigger_rule=ALL_DONE: aciona mesmo que as waves Glue tenham sido puladas
    # (ex: start_layer="redshift"). Passa reprocess_date para a DAG filha.
    trigger_redshift = TriggerDagRunOperator(
        task_id="acionar_carga_redshift",
        trigger_dag_id="cc_carga_redshift",
        conf={"reprocess_date": "{{ params.reprocess_date or ds }}"},
        wait_for_completion=True,
        poke_interval=60,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── Dependencias ─────────────────────────────────────────────────────────
    #
    # Fluxo linear com gates:
    #
    #   inicio → gate_bronze → [bronze_tasks] → fim_bronze
    #                → gate_silver (ALL_DONE) → [dim_tasks] → fim_dims
    #                      → gate_gold (ALL_DONE) → [wave1_tasks] → fim_wave1
    #                                                  → [wave2_tasks] → fim_pipeline
    #                                                        → trigger_redshift (ALL_DONE)
    #
    # Os gates com trigger_rule=ALL_DONE executam mesmo quando a wave anterior
    # foi pulada (tasks com status SKIPPED contam como "done").

    inicio >> gate_bronze >> bronze_tasks >> fim_bronze
    fim_bronze >> gate_silver >> dim_tasks >> fim_dims
    fim_dims >> gate_gold >> wave1_tasks >> fim_wave1
    fim_wave1 >> wave2_tasks >> fim_pipeline
    fim_pipeline >> trigger_redshift
