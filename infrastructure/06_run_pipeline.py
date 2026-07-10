"""
06_run_pipeline.py
==================
Contact Center Data Lakehouse — Execução Sequencial do Pipeline Glue

Executa os 40 jobs Glue na ordem correta, respeitando dependências:

  Etapa 1 — Bronze → Silver  (18 jobs, lotes paralelos de MAX_PARALLEL)
  Etapa 2 — Silver → Gold Dims (11 jobs, paralelo total)
  Etapa 3 — Silver → Gold Fatos Wave 1 (8 jobs sem dep. de outros fatos)
  Etapa 4 — Silver → Gold Fatos Wave 2 (3 jobs que dependem de Wave 1)

Uso:
    python 06_run_pipeline.py
    python 06_run_pipeline.py --bucket-name act-cc-dev-lakehouse --env dev
    python 06_run_pipeline.py --only bronze         # só etapa Bronze→Silver
    python 06_run_pipeline.py --only dims           # só dimensões
    python 06_run_pipeline.py --only fatos          # só fatos (waves 1+2)
    python 06_run_pipeline.py --only gold           # dims + fatos
    python 06_run_pipeline.py --job job-tb-chamada-bronze-to-silver  # job único
    python 06_run_pipeline.py --max-parallel 3      # limita paralelismo
    python 06_run_pipeline.py --dry-run             # mostra o que seria executado

Requisitos:
    pip install boto3
    Credenciais AWS configuradas com permissão de execução em Glue
"""

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_BUCKET      = "act-cc-dev-lakehouse"
DEFAULT_REGION      = "us-east-1"
DEFAULT_ENV         = "dev"
DEFAULT_MAX_PARALLEL = 5
JOB_TIMEOUT_MINUTES = 45    # encerra polling se job demorar demais
POLL_INTERVAL_SEC   = 20    # intervalo entre verificações de estado

# ---------------------------------------------------------------------------
# Pipeline: definição das etapas e ordem de execução
# ---------------------------------------------------------------------------

BRONZE_TO_SILVER = [
    "job-tb-chamada-bronze-to-silver",
    "job-tb-ticket-bronze-to-silver",
    "job-tb-chat-bronze-to-silver",
    "job-tb-whatsapp-atendimento-bronze-to-silver",
    "job-tb-ura-navegacao-bronze-to-silver",
    "job-tb-discagem-bronze-to-silver",
    "job-tb-metricas-operacionais-bronze-to-silver",
    "job-tb-gravacao-chamada-bronze-to-silver",
    "job-tb-cliente-bronze-to-silver",
    "job-tb-endereco-cliente-bronze-to-silver",
    "job-tb-operador-bronze-to-silver",
    "job-tb-skill-operador-bronze-to-silver",
    "job-tb-fila-atendimento-bronze-to-silver",
    "job-tb-avaliacao-qualidade-bronze-to-silver",
    "job-tb-jornada-operador-bronze-to-silver",
    "job-tb-interacao-ticket-bronze-to-silver",
    "job-tb-campanha-bronze-to-silver",
    "job-tb-mensagem-chat-bronze-to-silver",
]

SILVER_TO_GOLD_DIMS = [
    "job-dim-data-gold",
    "job-dim-cliente-gold",
    "job-dim-operador-gold",
    "job-dim-campanha-gold",
    "job-dim-canal-gold",
    "job-dim-fila-gold",
    "job-dim-skill-gold",
    "job-dim-status-chamada-gold",
    "job-dim-status-ticket-gold",
    "job-dim-categoria-ticket-gold",
    "job-dim-prioridade-ticket-gold",
]

# Wave 1: fatos independentes (sem dependência de outros fatos Gold)
SILVER_TO_GOLD_FATOS_W1 = [
    "job-fato-chamada-gold",            # base — fato_ura_navegacao depende deste
    "job-fato-chat-gold",               # base — fato_mensagem_chat depende deste
    "job-fato-ticket-gold",             # base — fato_interacao_ticket depende deste
    "job-fato-whatsapp-gold",
    "job-fato-qualidade-gold",
    "job-fato-discagem-gold",           # deps: dim_campanha, dim_cliente, dim_data
    "job-fato-jornada-operador-gold",   # deps: dim_operador, dim_data
    "job-fato-metricas-operacionais-gold",  # deps: dim_fila, dim_data
]

# Wave 2: fatos que dependem de fatos da Wave 1
SILVER_TO_GOLD_FATOS_W2 = [
    "job-fato-ura-navegacao-gold",      # deps: fato_chamada, dim_data
    "job-fato-interacao-ticket-gold",   # deps: fato_ticket, dim_operador, dim_data
    "job-fato-mensagem-chat-gold",      # deps: fato_chat, dim_data
]

ALL_JOBS = (
    BRONZE_TO_SILVER
    + SILVER_TO_GOLD_DIMS
    + SILVER_TO_GOLD_FATOS_W1
    + SILVER_TO_GOLD_FATOS_W2
)


# ---------------------------------------------------------------------------
# Helpers de formatação
# ---------------------------------------------------------------------------

def now_str() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def elapsed(start: float) -> str:
    secs = int(time.time() - start)
    return str(timedelta(seconds=secs))


def _bar(total: int, done: int, width: int = 30) -> str:
    filled = int(width * done / max(total, 1))
    return "[" + "█" * filled + "░" * (width - filled) + f"] {done}/{total}"


def print_header(title: str) -> None:
    print(f"\n{'=' * 65}")
    print(f"  {title}")
    print(f"{'=' * 65}")


def print_stage(label: str, jobs: list, total_stages: int, stage_num: int) -> None:
    print(f"\n{'─' * 65}")
    print(f"  ETAPA {stage_num}/{total_stages} — {label}  ({len(jobs)} jobs)")
    print(f"{'─' * 65}")


# ---------------------------------------------------------------------------
# Execução de job individual
# ---------------------------------------------------------------------------

def start_job(glue_client, job_name: str, bucket: str, env: str) -> str:
    """Inicia um Glue Job e retorna o JobRunId."""
    try:
        resp = glue_client.start_job_run(
            JobName=job_name,
            Arguments={
                "--JOB_NAME":    job_name,
                "--BUCKET_NAME": bucket,
                "--ENV":         env,
            },
        )
        return resp["JobRunId"]
    except ClientError as e:
        raise RuntimeError(f"Falha ao iniciar '{job_name}': {e}") from e


def poll_job(
    glue_client,
    job_name: str,
    run_id: str,
    timeout_minutes: int = JOB_TIMEOUT_MINUTES,
) -> dict:
    """
    Aguarda o job terminar.
    Retorna dict com: job_name, run_id, state, duration_sec, message.
    """
    start = time.time()
    last_state = None

    while True:
        try:
            resp  = glue_client.get_job_run(JobName=job_name, RunId=run_id)
            run   = resp["JobRun"]
            state = run["JobRunState"]
        except ClientError as e:
            return {
                "job_name":    job_name,
                "run_id":      run_id,
                "state":       "ERROR",
                "duration_sec": int(time.time() - start),
                "message":     str(e),
            }

        if state != last_state:
            last_state = state

        terminal = {"SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR"}
        if state in terminal:
            duration = int(time.time() - start)
            message  = run.get("ErrorMessage", "")
            return {
                "job_name":     job_name,
                "run_id":       run_id,
                "state":        state,
                "duration_sec": duration,
                "message":      message,
            }

        elapsed_min = (time.time() - start) / 60
        if elapsed_min > timeout_minutes:
            return {
                "job_name":     job_name,
                "run_id":       run_id,
                "state":        "TIMEOUT_POLL",
                "duration_sec": int(time.time() - start),
                "message":      f"Polling timeout após {timeout_minutes}min",
            }

        time.sleep(POLL_INTERVAL_SEC)


def run_single_job(
    glue_client,
    job_name: str,
    bucket: str,
    env: str,
    dry_run: bool = False,
) -> dict:
    """Inicia e aguarda um job. Retorna resultado."""
    if dry_run:
        print(f"    [DRY-RUN] {job_name}")
        return {"job_name": job_name, "run_id": "dry-run", "state": "SUCCEEDED", "duration_sec": 0, "message": ""}

    t0 = time.time()
    try:
        run_id = start_job(glue_client, job_name, bucket, env)
        print(f"    [{now_str()}] INICIADO  {job_name}  (run: {run_id[:8]}…)")
        result = poll_job(glue_client, job_name, run_id)
        icon   = "✓" if result["state"] == "SUCCEEDED" else "✗"
        dur    = elapsed(t0)
        print(f"    [{now_str()}] {icon} {result['state']:<14} {job_name}  ({dur})")
        if result["message"]:
            print(f"             └─ {result['message'][:120]}")
        return result
    except RuntimeError as e:
        dur = elapsed(t0)
        print(f"    [{now_str()}] ✗ START_FAILED   {job_name}  ({dur})")
        print(f"             └─ {str(e)[:120]}")
        return {"job_name": job_name, "run_id": None, "state": "START_FAILED",
                "duration_sec": int(time.time() - t0), "message": str(e)}


# ---------------------------------------------------------------------------
# Execução em lotes paralelos
# ---------------------------------------------------------------------------

def run_batch(
    glue_client,
    jobs: list,
    bucket: str,
    env: str,
    max_parallel: int,
    fail_fast: bool,
    dry_run: bool,
) -> list:
    """
    Executa uma lista de jobs com paralelismo limitado a max_parallel.
    Retorna lista de resultados.
    """
    results  = []
    failed   = []
    total    = len(jobs)
    done     = 0

    # Quebra em lotes de max_parallel
    for batch_start in range(0, total, max_parallel):
        batch = jobs[batch_start : batch_start + max_parallel]

        if fail_fast and failed:
            print(f"\n  [ABORTANDO] fail_fast ativo — {len(failed)} falha(s) detectada(s).")
            # Marca os restantes como SKIPPED
            for jn in jobs[batch_start:]:
                results.append({"job_name": jn, "run_id": None, "state": "SKIPPED",
                                 "duration_sec": 0, "message": "fail_fast"})
            break

        print(f"\n  Lote {batch_start // max_parallel + 1} — {_bar(total, done)}")

        with ThreadPoolExecutor(max_workers=len(batch)) as executor:
            futures = {
                executor.submit(run_single_job, glue_client, jn, bucket, env, dry_run): jn
                for jn in batch
            }
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                done += 1
                if result["state"] != "SUCCEEDED":
                    failed.append(result["job_name"])

    return results


# ---------------------------------------------------------------------------
# Orquestração por etapas
# ---------------------------------------------------------------------------

def run_stage(
    label: str,
    jobs: list,
    glue_client,
    bucket: str,
    env: str,
    max_parallel: int,
    fail_fast: bool,
    dry_run: bool,
    stage_num: int,
    total_stages: int,
) -> list:
    """Executa uma etapa e retorna lista de resultados."""
    print_stage(label, jobs, total_stages, stage_num)
    t0 = time.time()
    results = run_batch(glue_client, jobs, bucket, env, max_parallel, fail_fast, dry_run)

    succeeded = [r for r in results if r["state"] == "SUCCEEDED"]
    failed    = [r for r in results if r["state"] not in ("SUCCEEDED", "SKIPPED")]

    print(f"\n  Etapa concluída em {elapsed(t0)}")
    print(f"  Resultado: {len(succeeded)} sucesso(s) | {len(failed)} falha(s) | {len(jobs)} total")

    if failed:
        print("  Jobs com falha:")
        for r in failed:
            print(f"    ✗ {r['job_name']} → {r['state']}")
            if r.get("message"):
                print(f"      {r['message'][:100]}")

    return results


# ---------------------------------------------------------------------------
# Relatório final
# ---------------------------------------------------------------------------

def print_report(all_results: list, pipeline_start: float) -> bool:
    """Imprime relatório final. Retorna True se pipeline foi bem-sucedido."""
    print_header("RELATÓRIO FINAL DO PIPELINE")

    succeeded = [r for r in all_results if r["state"] == "SUCCEEDED"]
    failed    = [r for r in all_results if r["state"] not in ("SUCCEEDED", "SKIPPED", "dry-run")]
    skipped   = [r for r in all_results if r["state"] == "SKIPPED"]
    total     = len(all_results)

    print(f"\n  Duração total : {elapsed(pipeline_start)}")
    print(f"  Jobs totais   : {total}")
    print(f"  ✓ Sucesso     : {len(succeeded)}")
    print(f"  ✗ Falha       : {len(failed)}")
    print(f"  ⊘ Pulados     : {len(skipped)}")

    if succeeded:
        print(f"\n  ── Sucessos ──")
        for r in succeeded:
            dur = str(timedelta(seconds=r["duration_sec"]))
            print(f"    ✓ {r['job_name']:<50} {dur}")

    if failed:
        print(f"\n  ── Falhas ──")
        for r in failed:
            print(f"    ✗ {r['job_name']}")
            print(f"      Estado  : {r['state']}")
            if r.get("message"):
                print(f"      Mensagem: {r['message'][:120]}")

    if skipped:
        print(f"\n  ── Pulados (fail_fast) ──")
        for r in skipped:
            print(f"    ⊘ {r['job_name']}")

    status = "SUCESSO" if not failed else "FALHA"
    print(f"\n{'=' * 65}")
    print(f"  Pipeline finalizado com {status}")
    print(f"{'=' * 65}\n")

    return len(failed) == 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Executa o pipeline Contact Center Data Lakehouse no Glue"
    )
    parser.add_argument("--bucket-name",   default=DEFAULT_BUCKET,
                        help=f"Bucket S3 (padrão: {DEFAULT_BUCKET})")
    parser.add_argument("--region",        default=DEFAULT_REGION,
                        help=f"Região AWS (padrão: {DEFAULT_REGION})")
    parser.add_argument("--env",           default=DEFAULT_ENV,
                        help=f"Ambiente: dev | prod (padrão: {DEFAULT_ENV})")
    parser.add_argument("--max-parallel",  type=int, default=DEFAULT_MAX_PARALLEL,
                        help=f"Máx. jobs simultâneos por lote (padrão: {DEFAULT_MAX_PARALLEL})")
    parser.add_argument(
        "--only",
        choices=["bronze", "dims", "fatos", "gold"],
        default=None,
        help=(
            "Executa apenas uma etapa:\n"
            "  bronze = Bronze→Silver\n"
            "  dims   = Silver→Gold Dimensões\n"
            "  fatos  = Silver→Gold Fatos (waves 1+2)\n"
            "  gold   = Dims + Fatos"
        ),
    )
    parser.add_argument("--job", default=None,
                        help="Executa apenas um job específico pelo nome")
    parser.add_argument("--fail-fast", action="store_true",
                        help="Aborta as etapas seguintes se houver falha")
    parser.add_argument("--dry-run", action="store_true",
                        help="Mostra o que seria executado sem submeter jobs")
    return parser.parse_args()


def main():
    args         = parse_args()
    bucket       = args.bucket_name
    region       = args.region
    env          = args.env
    max_parallel = args.max_parallel
    fail_fast    = args.fail_fast
    dry_run      = args.dry_run

    print_header("Contact Center Data Lakehouse — Pipeline Runner")
    print(f"  Bucket       : {bucket}")
    print(f"  Região       : {region}")
    print(f"  Ambiente     : {env}")
    print(f"  Max paralelo : {max_parallel}")
    print(f"  Fail fast    : {fail_fast}")
    print(f"  Dry run      : {dry_run}")
    if args.only:
        print(f"  Filtro       : --only {args.only}")
    if args.job:
        print(f"  Job único    : {args.job}")

    glue_client  = boto3.client("glue", region_name=region)
    pipeline_t0  = time.time()
    all_results  = []

    # ── Job único ──────────────────────────────────────────────────────────
    if args.job:
        if args.job not in ALL_JOBS:
            print(f"\n[ERRO] Job '{args.job}' não encontrado. Jobs disponíveis:")
            for j in ALL_JOBS:
                print(f"  {j}")
            sys.exit(1)
        result = run_single_job(glue_client, args.job, bucket, env, dry_run)
        all_results.append(result)
        ok = print_report(all_results, pipeline_t0)
        sys.exit(0 if ok else 1)

    # ── Definição das etapas a executar ────────────────────────────────────
    stages = []

    run_bronze = args.only in (None, "bronze")
    run_dims   = args.only in (None, "dims", "gold")
    run_fatos  = args.only in (None, "fatos", "gold")

    if run_bronze:
        stages.append(("Bronze → Silver (18 jobs)", BRONZE_TO_SILVER))
    if run_dims:
        stages.append(("Silver → Gold: Dimensões (11 jobs)", SILVER_TO_GOLD_DIMS))
    if run_fatos:
        stages.append(("Silver → Gold: Fatos Wave 1 (8 jobs)", SILVER_TO_GOLD_FATOS_W1))
        stages.append(("Silver → Gold: Fatos Wave 2 — deps de Wave 1 (3 jobs)", SILVER_TO_GOLD_FATOS_W2))

    total_stages = len(stages)

    if dry_run:
        print(f"\n  [DRY-RUN] Jobs que seriam executados ({sum(len(s[1]) for s in stages)} total):")
        for i, (label, jobs) in enumerate(stages, 1):
            print(f"\n  Etapa {i}: {label}")
            for j in jobs:
                print(f"    → {j}")
        print()

    # ── Execução das etapas ─────────────────────────────────────────────────
    abort = False
    for i, (label, jobs) in enumerate(stages, 1):
        if abort:
            print(f"\n  [ABORTANDO] Etapa '{label}' pulada devido a falhas anteriores.")
            for j in jobs:
                all_results.append({
                    "job_name": j, "run_id": None, "state": "SKIPPED",
                    "duration_sec": 0, "message": "etapa anterior falhou",
                })
            continue

        results = run_stage(
            label=label,
            jobs=jobs,
            glue_client=glue_client,
            bucket=bucket,
            env=env,
            max_parallel=max_parallel,
            fail_fast=fail_fast,
            dry_run=dry_run,
            stage_num=i,
            total_stages=total_stages,
        )
        all_results.extend(results)

        # Se fail_fast e houve falha na etapa → aborta próximas
        if fail_fast:
            stage_failed = [r for r in results if r["state"] not in ("SUCCEEDED", "SKIPPED")]
            if stage_failed:
                abort = True

    ok = print_report(all_results, pipeline_t0)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
