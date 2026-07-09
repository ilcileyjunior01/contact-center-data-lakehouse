"""
01_setup_s3.py
==============
Contact Center Data Lakehouse - S3 Infrastructure Setup

Cria o bucket S3 principal com todas as pastas, subpastas por domínio/tabela,
versionamento, lifecycle rules e EventBridge notifications.

Uso:
    python 01_setup_s3.py
    python 01_setup_s3.py --bucket-name meu-bucket --region us-east-1

Requisitos:
    pip install boto3
    Credenciais AWS configuradas (aws configure ou variáveis de ambiente)
"""

import argparse
import json
import sys
import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_BUCKET = "act-cc-dev-lakehouse"
DEFAULT_REGION = "sa-east-1"

# ---------------------------------------------------------------------------
# Prefixes a serem criados
# ---------------------------------------------------------------------------
TOP_LEVEL_PREFIXES = [
    "bronze/",
    "silver/",
    "gold/",
    "checkpoints/",
    "quarantine/",
    "logs/",
    "scripts/",
    "athena-results/",
    "temp/",
]

# 18 tabelas organizadas por domínio dentro de bronze/
BRONZE_TABLE_PREFIXES = [
    # Domínio: operacao (8 tabelas)
    "bronze/operacao/chamada/",
    "bronze/operacao/ticket/",
    "bronze/operacao/chat/",
    "bronze/operacao/whatsapp/",
    "bronze/operacao/ura/",
    "bronze/operacao/discagem/",
    "bronze/operacao/metricas/",
    "bronze/operacao/gravacao/",
    # Domínio: cadastro (5 tabelas)
    "bronze/cadastro/cliente/",
    "bronze/cadastro/endereco/",
    "bronze/cadastro/operador/",
    "bronze/cadastro/skill/",
    "bronze/cadastro/fila/",
    # Domínio: qualidade (3 tabelas)
    "bronze/qualidade/avaliacao/",
    "bronze/qualidade/jornada/",
    "bronze/qualidade/ticket-interacao/",
    # Domínio: marketing (1 tabela)
    "bronze/marketing/campanha/",
    # Domínio: suporte (1 tabela)
    "bronze/suporte/mensagem-chat/",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_account_id() -> str:
    """Retorna o AWS Account ID via STS."""
    sts = boto3.client("sts")
    identity = sts.get_caller_identity()
    account_id = identity["Account"]
    print(f"  [STS] Account ID identificado: {account_id}")
    return account_id


def bucket_exists(s3_client, bucket_name: str) -> bool:
    """Retorna True se o bucket já existe e é acessível."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in ("404", "NoSuchBucket"):
            return False
        if error_code == "403":
            print(f"  [AVISO] Bucket '{bucket_name}' existe, mas sem permissão de acesso (403).")
            sys.exit(1)
        raise


def create_bucket(s3_client, bucket_name: str, region: str) -> None:
    """Cria o bucket S3 na região especificada."""
    if bucket_exists(s3_client, bucket_name):
        print(f"  [OK] Bucket '{bucket_name}' já existe — pulando criação.")
        return

    print(f"  [INFO] Criando bucket '{bucket_name}' na região '{region}'...")
    try:
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        print(f"  [OK] Bucket '{bucket_name}' criado com sucesso.")
    except ClientError as e:
        print(f"  [ERRO] Falha ao criar bucket: {e}")
        sys.exit(1)


def enable_versioning(s3_client, bucket_name: str) -> None:
    """Ativa o versionamento no bucket."""
    print(f"  [INFO] Ativando versionamento em '{bucket_name}'...")
    try:
        s3_client.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={"Status": "Enabled"},
        )
        print("  [OK] Versionamento ativado.")
    except ClientError as e:
        print(f"  [ERRO] Falha ao ativar versionamento: {e}")
        raise


def block_public_access(s3_client, bucket_name: str) -> None:
    """Bloqueia todo acesso público ao bucket (boa prática de segurança)."""
    print(f"  [INFO] Bloqueando acesso público ao bucket '{bucket_name}'...")
    try:
        s3_client.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            },
        )
        print("  [OK] Acesso público bloqueado.")
    except ClientError as e:
        print(f"  [ERRO] Falha ao bloquear acesso público: {e}")
        raise


def create_prefixes(s3_client, bucket_name: str, prefixes: list) -> None:
    """Cria os prefixes (pastas) colocando objetos vazios com key terminando em '/'."""
    print(f"\n  [INFO] Criando {len(prefixes)} prefixes no bucket '{bucket_name}'...")
    created = 0
    skipped = 0
    errors = 0

    for prefix in prefixes:
        try:
            # Verifica se o prefix já existe
            response = s3_client.list_objects_v2(
                Bucket=bucket_name, Prefix=prefix, MaxKeys=1
            )
            if response.get("KeyCount", 0) > 0:
                print(f"    [OK] Prefix já existe: {prefix}")
                skipped += 1
                continue

            # Cria o prefix com um objeto vazio
            s3_client.put_object(Bucket=bucket_name, Key=prefix, Body=b"")
            print(f"    [+] Criado: {prefix}")
            created += 1
        except ClientError as e:
            print(f"    [ERRO] Falha ao criar prefix '{prefix}': {e}")
            errors += 1

    print(
        f"  [RESUMO PREFIXES] Criados: {created} | Já existiam: {skipped} | Erros: {errors}"
    )


def configure_lifecycle(s3_client, bucket_name: str) -> None:
    """
    Configura lifecycle rule:
      - Expirar objetos em quarantine/ após 90 dias
      - Expirar marcadores de delete em bronze/ após 180 dias (manutenção de versões)
    """
    print("\n  [INFO] Configurando lifecycle rules...")
    lifecycle_config = {
        "Rules": [
            {
                "ID": "expire-quarantine-90d",
                "Status": "Enabled",
                "Filter": {"Prefix": "quarantine/"},
                "Expiration": {"Days": 90},
                "NoncurrentVersionExpiration": {"NoncurrentDays": 30},
            },
            {
                "ID": "expire-temp-7d",
                "Status": "Enabled",
                "Filter": {"Prefix": "temp/"},
                "Expiration": {"Days": 7},
            },
            {
                "ID": "transition-bronze-ia-90d",
                "Status": "Enabled",
                "Filter": {"Prefix": "bronze/"},
                "Transitions": [
                    {"Days": 90, "StorageClass": "STANDARD_IA"},
                    {"Days": 365, "StorageClass": "GLACIER"},
                ],
                "NoncurrentVersionTransitions": [
                    {"NoncurrentDays": 30, "StorageClass": "STANDARD_IA"},
                ],
                "NoncurrentVersionExpiration": {"NoncurrentDays": 180},
            },
            {
                "ID": "expire-athena-results-30d",
                "Status": "Enabled",
                "Filter": {"Prefix": "athena-results/"},
                "Expiration": {"Days": 30},
            },
        ]
    }

    try:
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_config,
        )
        print("  [OK] Lifecycle rules configuradas:")
        print("       - quarantine/: expirar após 90 dias")
        print("       - temp/: expirar após 7 dias")
        print("       - bronze/: mover para IA após 90d, Glacier após 365d")
        print("       - athena-results/: expirar após 30 dias")
    except ClientError as e:
        print(f"  [ERRO] Falha ao configurar lifecycle: {e}")
        raise


def configure_eventbridge_notification(
    s3_client, bucket_name: str, account_id: str
) -> None:
    """
    Configura S3 Event Notification para encaminhar eventos PutObject
    do prefix bronze/ para o EventBridge default event bus.
    """
    print("\n  [INFO] Configurando S3 Event Notification → EventBridge...")
    try:
        # Primeiro habilita EventBridge notifications no bucket
        s3_client.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={
                "EventBridgeConfiguration": {},  # Envia TODOS os eventos para EventBridge
            },
        )
        print(
            "  [OK] S3 Event Notification configurado para EventBridge."
        )
        print(
            "  [INFO] Todos os eventos do bucket serão enviados ao default event bus."
        )
        print(
            "  [INFO] Crie EventBridge rules com filtro em 'source': 'aws.s3'"
        )
        print(
            f"         e 'detail.bucket.name': '{bucket_name}', 'detail.object.key' prefix: 'bronze/'"
        )
    except ClientError as e:
        print(f"  [ERRO] Falha ao configurar EventBridge notification: {e}")
        raise


def configure_server_side_encryption(s3_client, bucket_name: str) -> None:
    """Habilita criptografia SSE-S3 (AES256) por padrão no bucket."""
    print("\n  [INFO] Configurando criptografia padrão SSE-S3...")
    try:
        s3_client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "AES256"
                        },
                        "BucketKeyEnabled": True,
                    }
                ]
            },
        )
        print("  [OK] Criptografia SSE-S3 (AES256) habilitada.")
    except ClientError as e:
        print(f"  [ERRO] Falha ao configurar criptografia: {e}")
        raise


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Setup S3 bucket para Contact Center Data Lakehouse"
    )
    parser.add_argument(
        "--bucket-name",
        default=DEFAULT_BUCKET,
        help=f"Nome do bucket S3 (default: {DEFAULT_BUCKET})",
    )
    parser.add_argument(
        "--region",
        default=DEFAULT_REGION,
        help=f"Região AWS (default: {DEFAULT_REGION})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simula as operações sem executar chamadas AWS",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    bucket_name = args.bucket_name
    region = args.region

    print("=" * 65)
    print("  Contact Center Data Lakehouse — Setup S3")
    print("=" * 65)
    print(f"  Bucket  : {bucket_name}")
    print(f"  Região  : {region}")
    print(f"  Dry-run : {args.dry_run}")
    print("=" * 65)

    if args.dry_run:
        print("\n[DRY-RUN] Prefixes que seriam criados:")
        for p in TOP_LEVEL_PREFIXES + BRONZE_TABLE_PREFIXES:
            print(f"  s3://{bucket_name}/{p}")
        print("\n[DRY-RUN] Nenhuma chamada AWS foi feita.")
        return

    # Clientes AWS
    s3_client = boto3.client("s3", region_name=region)

    print("\n[ETAPA 1] Identificando conta AWS...")
    account_id = get_account_id()

    print("\n[ETAPA 2] Criando/verificando bucket S3...")
    create_bucket(s3_client, bucket_name, region)

    print("\n[ETAPA 3] Bloqueando acesso público...")
    block_public_access(s3_client, bucket_name)

    print("\n[ETAPA 4] Ativando versionamento...")
    enable_versioning(s3_client, bucket_name)

    print("\n[ETAPA 5] Configurando criptografia padrão...")
    configure_server_side_encryption(s3_client, bucket_name)

    print("\n[ETAPA 6] Criando prefixes top-level...")
    create_prefixes(s3_client, bucket_name, TOP_LEVEL_PREFIXES)

    print("\n[ETAPA 7] Criando subpastas das 18 tabelas em bronze/...")
    create_prefixes(s3_client, bucket_name, BRONZE_TABLE_PREFIXES)

    print("\n[ETAPA 8] Configurando lifecycle rules...")
    configure_lifecycle(s3_client, bucket_name)

    print("\n[ETAPA 9] Configurando S3 Event Notification → EventBridge...")
    configure_eventbridge_notification(s3_client, bucket_name, account_id)

    # ---------------------------------------------------------------------------
    # Resumo Final
    # ---------------------------------------------------------------------------
    total_prefixes = len(TOP_LEVEL_PREFIXES) + len(BRONZE_TABLE_PREFIXES)
    print("\n" + "=" * 65)
    print("  RESUMO FINAL - S3 Setup Concluído")
    print("=" * 65)
    print(f"  Conta AWS      : {account_id}")
    print(f"  Bucket         : s3://{bucket_name}/")
    print(f"  Região         : {region}")
    print(f"  Total prefixes : {total_prefixes}")
    print(f"  Versionamento  : Ativado")
    print(f"  Criptografia   : SSE-S3 (AES256)")
    print(f"  Acesso público : Bloqueado")
    print(f"  Lifecycle      : quarantine/→90d | temp/→7d | bronze/→IA/Glacier")
    print(f"  EventBridge    : Habilitado (todos os eventos do bucket)")
    print()
    print("  Prefixes top-level:")
    for p in TOP_LEVEL_PREFIXES:
        print(f"    s3://{bucket_name}/{p}")
    print()
    print("  Tabelas bronze/ (18 tabelas):")
    for p in BRONZE_TABLE_PREFIXES:
        print(f"    s3://{bucket_name}/{p}")
    print("=" * 65)
    print("  Setup S3 finalizado com sucesso!")
    print("=" * 65)


if __name__ == "__main__":
    main()
