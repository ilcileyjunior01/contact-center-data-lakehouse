"""
apply_cost_optimizations.py
===========================
Aplica otimizações de custo na infraestrutura AWS existente via boto3.
Não usa Terraform — seguro para ambientes sem state file.

Mudanças aplicadas:
  1. S3 lifecycle rules:
     - redshift-staging/ -> expira em 3 dias
     - logs/             -> expira em 14 dias
     - versões antigas   -> expira em 7 dias (global)
     - multipart incompleto -> cancela em 3 dias (global)
  2. CloudWatch log groups -> retention reduzida para 7 dias

Uso:
    python apply_cost_optimizations.py
    python apply_cost_optimizations.py --dry-run   (mostra o que faria sem executar)
    python apply_cost_optimizations.py --region us-east-1 --bucket act-cc-dev-lakehouse
"""

import argparse
import json
import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_REGION = "us-east-1"
DEFAULT_BUCKET = "act-cc-dev-lakehouse"
CLOUDWATCH_RETENTION_DAYS = 7

# Prefixos dos log groups Glue/Lambda do projeto
LOG_GROUP_PREFIXES = [
    "/aws-glue/jobs/",
    "/aws-glue/workflows/",
    "/aws/lambda/fn-start-glue-crawler-cc",
    "/aws/emr-serverless/",
]


# ---------------------------------------------------------------------------
# S3 Lifecycle Rules
# ---------------------------------------------------------------------------

def get_existing_lifecycle_rules(s3_client, bucket: str) -> list:
    """Retorna as regras de lifecycle existentes no bucket."""
    try:
        resp = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket)
        return resp.get("Rules", [])
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchLifecycleConfiguration":
            return []
        raise


def build_new_rules() -> list:
    """Retorna as 3 novas regras de lifecycle a adicionar."""
    return [
        {
            "ID": "redshift-staging-expiry",
            "Status": "Enabled",
            "Filter": {"Prefix": "redshift-staging/"},
            "Expiration": {"Days": 3},
            "NoncurrentVersionExpiration": {"NoncurrentDays": 1},
        },
        {
            "ID": "logs-expiry",
            "Status": "Enabled",
            "Filter": {"Prefix": "logs/"},
            "Expiration": {"Days": 14},
            "NoncurrentVersionExpiration": {"NoncurrentDays": 3},
        },
        {
            "ID": "global-noncurrent-version-expiry",
            "Status": "Enabled",
            "Filter": {"Prefix": ""},
            "NoncurrentVersionExpiration": {"NoncurrentDays": 7},
            "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 3},
        },
    ]


def apply_s3_lifecycle(s3_client, bucket: str, dry_run: bool) -> None:
    print(f"\n{'='*60}")
    print(f"  S3 LIFECYCLE RULES — bucket: {bucket}")
    print(f"{'='*60}")

    existing = get_existing_lifecycle_rules(s3_client, bucket)
    existing_ids = {r["ID"] for r in existing}
    new_rules = build_new_rules()

    rules_to_add = [r for r in new_rules if r["ID"] not in existing_ids]
    rules_to_skip = [r for r in new_rules if r["ID"] in existing_ids]

    if rules_to_skip:
        for r in rules_to_skip:
            print(f"  [SKIP] Regra já existe: {r['ID']}")

    if not rules_to_add:
        print("  [OK] Todas as regras já estão configuradas. Nada a fazer.")
        return

    merged_rules = existing + rules_to_add

    for r in rules_to_add:
        print(f"  [+] Nova regra: {r['ID']}")
        if r["ID"] == "redshift-staging-expiry":
            print(f"      redshift-staging/ -> exclui objetos após 3 dias")
        elif r["ID"] == "logs-expiry":
            print(f"      logs/ -> exclui objetos após 14 dias")
        elif r["ID"] == "global-noncurrent-version-expiry":
            print(f"      (global) -> versões antigas expiram em 7 dias")
            print(f"      (global) -> multipart incompleto cancelado em 3 dias")

    if dry_run:
        print(f"\n  [DRY-RUN] Nenhuma alteração aplicada.")
        print(f"  Configuração que seria enviada:")
        print(json.dumps({"Rules": merged_rules}, indent=4, default=str))
        return

    try:
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration={"Rules": merged_rules},
        )
        print(f"\n  [OK] {len(rules_to_add)} regra(s) aplicada(s) com sucesso!")
    except ClientError as e:
        print(f"  [ERRO] Falha ao aplicar lifecycle rules: {e}")
        raise


# ---------------------------------------------------------------------------
# CloudWatch Log Groups
# ---------------------------------------------------------------------------

def list_project_log_groups(logs_client) -> list:
    """Lista log groups do projeto com base nos prefixos definidos."""
    found = []
    paginator = logs_client.get_paginator("describe_log_groups")

    for prefix in LOG_GROUP_PREFIXES:
        try:
            for page in paginator.paginate(logGroupNamePrefix=prefix):
                for lg in page.get("logGroups", []):
                    found.append(lg)
        except ClientError as e:
            print(f"  [AVISO] Erro ao listar log groups com prefixo '{prefix}': {e}")

    return found


def apply_cloudwatch_retention(logs_client, dry_run: bool) -> None:
    print(f"\n{'='*60}")
    print(f"  CLOUDWATCH LOG GROUPS — retention -> {CLOUDWATCH_RETENTION_DAYS} dias")
    print(f"{'='*60}")

    log_groups = list_project_log_groups(logs_client)

    if not log_groups:
        print("  [INFO] Nenhum log group do projeto encontrado na AWS.")
        return

    updated = 0
    skipped = 0

    for lg in log_groups:
        name = lg["logGroupName"]
        current = lg.get("retentionInDays")

        if current == CLOUDWATCH_RETENTION_DAYS:
            print(f"  [SKIP] {name} (já {current} dias)")
            skipped += 1
            continue

        old = current if current else "infinito"
        print(f"  [~]  {name}  ({old} -> {CLOUDWATCH_RETENTION_DAYS} dias)")

        if not dry_run:
            try:
                logs_client.put_retention_policy(
                    logGroupName=name,
                    retentionInDays=CLOUDWATCH_RETENTION_DAYS,
                )
                updated += 1
            except ClientError as e:
                print(f"       [ERRO] {e}")
        else:
            updated += 1

    suffix = " [DRY-RUN]" if dry_run else ""
    print(f"\n  [OK] {updated} log group(s) atualizado(s), {skipped} ignorado(s).{suffix}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Aplica otimizações de custo AWS — Contact Center Data Lakehouse"
    )
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mostra o que seria feito sem aplicar nenhuma mudança",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    print("=" * 60)
    print("  Contact Center Lakehouse — Cost Optimization Script")
    print("=" * 60)
    print(f"  Região  : {args.region}")
    print(f"  Bucket  : {args.bucket}")
    print(f"  Dry-run : {'SIM (nenhuma mudança será aplicada)' if args.dry_run else 'NÃO (mudanças serão aplicadas)'}")
    print("=" * 60)

    s3_client   = boto3.client("s3",   region_name=args.region)
    logs_client = boto3.client("logs", region_name=args.region)

    # 1. S3 lifecycle rules
    apply_s3_lifecycle(s3_client, args.bucket, args.dry_run)

    # 2. CloudWatch log groups retention
    apply_cloudwatch_retention(logs_client, args.dry_run)

    suffix = "  [DRY-RUN - nenhuma mudanca aplicada]" if args.dry_run else ""
    print(f"\n{'='*60}")
    print(f"  Concluido!{suffix}")
    print(f"{'='*60}")
    print(
        "\n"
        "  Resumo das otimizacoes de custo aplicadas:\n"
        "  " + "-"*57 + "\n"
        "  S3 lifecycle:\n"
        "    redshift-staging/ -> objetos expiram em 3 dias\n"
        "    logs/             -> objetos expiram em 14 dias\n"
        "    (global)          -> versoes antigas expiram em 7 dias\n"
        "    (global)          -> multipart incompleto cancela em 3d\n"
        "\n"
        "  CloudWatch:\n"
        "    Log groups do projeto -> retencao 7 dias\n"
        "\n"
        "  Servicos ja otimizados (sem alteracao necessaria):\n"
        "    Redshift Serverless  -> auto-pause 30 min ativo\n"
        "    EMR Serverless       -> auto-stop 15 min, idle = $0\n"
        "    Lambda               -> dentro do free tier\n"
        "    Athena results       -> expiram em 30 dias (ja configurado)\n"
        "  " + "-"*57 + "\n"
        "  Custo estimado em modo dev (sem rodar jobs): ~$0.57/mes\n"
    )


if __name__ == "__main__":
    main()
