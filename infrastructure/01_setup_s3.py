"""
01_setup_s3.py
==============
Contact Center Data Lakehouse - S3 Infrastructure Setup

Cria o bucket S3 com:
  - Block public access habilitado
  - Versionamento habilitado
  - Estrutura de prefixos (bronze / silver / gold / quarantine /
    checkpoints / scripts / logs)
  - Lifecycle rule: move objetos de quarantine para Glacier após 90 dias

Uso:
    python 01_setup_s3.py
    python 01_setup_s3.py --bucket-name meu-bucket --region us-east-1

Requisitos:
    pip install boto3
    Credenciais AWS configuradas com permissão em S3
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
DEFAULT_REGION = "us-east-1"

# Prefixos que serão criados com um objeto marcador (.keep)
# S3 não tem "pastas" reais — o marcador torna o prefixo visível no console.
PREFIXES = [
    # Camada Bronze — dados brutos por domínio
    "bronze/operacao/chamada/",
    "bronze/operacao/ticket/",
    "bronze/operacao/chat/",
    "bronze/operacao/whatsapp/",
    "bronze/operacao/ura/",
    "bronze/operacao/discagem/",
    "bronze/operacao/metricas/",
    "bronze/operacao/gravacao/",
    "bronze/cadastro/cliente/",
    "bronze/cadastro/endereco/",
    "bronze/cadastro/operador/",
    "bronze/cadastro/skill/",
    "bronze/cadastro/fila/",
    "bronze/qualidade/avaliacao/",
    "bronze/qualidade/jornada/",
    "bronze/qualidade/ticket-interacao/",
    "bronze/marketing/campanha/",
    "bronze/suporte/mensagem-chat/",
    # Camada Silver — Iceberg / dados tratados
    "silver/operacao/chamada/",
    "silver/operacao/ticket/",
    "silver/operacao/chat/",
    "silver/canais/whatsapp/",
    "silver/operacao/ura/",
    "silver/comercial/discagem/",
    "silver/operacao/metricas/",
    "silver/operacao/gravacao/",
    "silver/cadastro/cliente/",
    "silver/cadastro/endereco/",
    "silver/cadastro/operador/",
    "silver/cadastro/skill/",
    "silver/cadastro/fila/",
    "silver/qualidade/avaliacao/",
    "silver/qualidade/jornada/",
    "silver/qualidade/ticket-interacao/",
    "silver/comercial/campanha/",
    "silver/canais/mensagem_chat/",
    # Camada Gold — Star Schema Iceberg
    "gold/dimensoes/dim_data/",
    "gold/dimensoes/dim_cliente/",
    "gold/dimensoes/dim_operador/",
    "gold/dimensoes/dim_campanha/",
    "gold/dimensoes/dim_canal/",
    "gold/dimensoes/dim_fila/",
    "gold/dimensoes/dim_skill/",
    "gold/dimensoes/dim_status_chamada/",
    "gold/dimensoes/dim_status_ticket/",
    "gold/dimensoes/dim_categoria_ticket/",
    "gold/dimensoes/dim_prioridade_ticket/",
    "gold/fatos/fato_chamada/",
    "gold/fatos/fato_ticket/",
    "gold/fatos/fato_chat/",
    "gold/fatos/fato_whatsapp/",
    "gold/fatos/fato_ura_navegacao/",
    "gold/fatos/fato_discagem/",
    "gold/fatos/fato_metricas_operacionais/",
    "gold/fatos/fato_mensagem_chat/",
    "gold/fatos/fato_interacao_ticket/",
    "gold/fatos/fato_qualidade/",
    "gold/fatos/fato_jornada_operador/",
    # Quarentena
    "quarantine/",
    # Watermark / checkpoints
    "checkpoints/",
    # Scripts PySpark dos jobs Glue
    "scripts/bronze_to_silver/",
    "scripts/silver_to_gold/",
    # Logs de jobs (Glue, EMR Serverless)
    "logs/glue/",
    "logs/emr-serverless/",
    # Temporários / staging
    "tmp/",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def create_bucket(s3_client, bucket_name: str, region: str) -> None:
    """Cria o bucket S3 se não existir."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"  [OK] Bucket '{bucket_name}' já existe — pulando criação.")
        return
    except ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code != 404:
            print(f"  [ERRO] Falha ao verificar bucket: {e}")
            raise

    print(f"  [INFO] Criando bucket '{bucket_name}' em '{region}'...")
    try:
        if region == "us-east-1":
            # us-east-1 não aceita LocationConstraint
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        print(f"  [+] Bucket criado: s3://{bucket_name}/")
    except ClientError as e:
        print(f"  [ERRO] Falha ao criar bucket: {e}")
        raise


def block_public_access(s3_client, bucket_name: str) -> None:
    """Bloqueia todo acesso público ao bucket."""
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
        print("  [+] Block public access habilitado.")
    except ClientError as e:
        print(f"  [ERRO] Falha ao bloquear acesso público: {e}")
        raise


def enable_versioning(s3_client, bucket_name: str) -> None:
    """Habilita versionamento no bucket."""
    try:
        s3_client.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={"Status": "Enabled"},
        )
        print("  [+] Versionamento habilitado.")
    except ClientError as e:
        print(f"  [ERRO] Falha ao habilitar versionamento: {e}")
        raise


def apply_lifecycle_policy(s3_client, bucket_name: str) -> None:
    """
    Aplica política de lifecycle:
      - quarantine/: move para Glacier Instant Retrieval após 90 dias,
        expira após 365 dias
      - logs/:       expira após 30 dias
      - tmp/:        expira após 7 dias
    """
    lifecycle_config = {
        "Rules": [
            {
                "ID": "quarantine-to-glacier",
                "Status": "Enabled",
                "Filter": {"Prefix": "quarantine/"},
                "Transitions": [
                    {
                        "Days": 90,
                        "StorageClass": "GLACIER_IR",
                    }
                ],
                "Expiration": {"Days": 365},
                "NoncurrentVersionExpiration": {"NoncurrentDays": 30},
            },
            {
                "ID": "logs-expiration",
                "Status": "Enabled",
                "Filter": {"Prefix": "logs/"},
                "Expiration": {"Days": 30},
                "NoncurrentVersionExpiration": {"NoncurrentDays": 7},
            },
            {
                "ID": "tmp-expiration",
                "Status": "Enabled",
                "Filter": {"Prefix": "tmp/"},
                "Expiration": {"Days": 7},
                "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 2},
            },
        ]
    }

    try:
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_config,
        )
        print("  [+] Lifecycle policy aplicada (quarantine→Glacier/90d, logs/30d, tmp/7d).")
    except ClientError as e:
        print(f"  [ERRO] Falha ao aplicar lifecycle policy: {e}")
        raise


def apply_bucket_tags(s3_client, bucket_name: str) -> None:
    """Aplica tags de projeto ao bucket."""
    try:
        s3_client.put_bucket_tagging(
            Bucket=bucket_name,
            Tagging={
                "TagSet": [
                    {"Key": "project",     "Value": "contact-center-data-lakehouse"},
                    {"Key": "layer",       "Value": "all"},
                    {"Key": "environment", "Value": "dev"},
                    {"Key": "created_by",  "Value": "01_setup_s3.py"},
                ]
            },
        )
        print("  [+] Tags aplicadas ao bucket.")
    except ClientError as e:
        print(f"  [ERRO] Falha ao aplicar tags: {e}")
        raise


def create_prefix_structure(s3_client, bucket_name: str) -> None:
    """
    Cria os prefixos do Data Lakehouse via objetos marcadores (.keep).
    Isso torna a estrutura visível no Console S3 e evita erros
    de 'prefix not found' em leituras durante testes.
    """
    print(f"\n  Criando {len(PREFIXES)} prefixos...")
    created = 0
    skipped = 0
    for prefix in PREFIXES:
        key = f"{prefix}.keep"
        try:
            s3_client.head_object(Bucket=bucket_name, Key=key)
            skipped += 1
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=b"",
                    ContentType="application/octet-stream",
                )
                created += 1
            else:
                raise
    print(f"  [+] Prefixos criados: {created} | Já existiam: {skipped}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Setup S3 para Contact Center Data Lakehouse"
    )
    parser.add_argument("--bucket-name", default=DEFAULT_BUCKET,
                        help=f"Nome do bucket S3 (padrão: {DEFAULT_BUCKET})")
    parser.add_argument("--region", default=DEFAULT_REGION,
                        help=f"Região AWS (padrão: {DEFAULT_REGION})")
    parser.add_argument("--skip-versioning", action="store_true",
                        help="Não habilita versionamento (útil para dev com custo reduzido)")
    return parser.parse_args()


def main():
    args = parse_args()
    bucket_name = args.bucket_name
    region      = args.region

    print("=" * 65)
    print("  Contact Center Data Lakehouse — Setup S3")
    print("=" * 65)
    print(f"  Bucket : {bucket_name}")
    print(f"  Região : {region}")
    print("=" * 65)

    s3_client = boto3.client("s3", region_name=region)

    # -----------------------------------------------------------------------
    print("\n[ETAPA 1] Criando bucket S3...")
    create_bucket(s3_client, bucket_name, region)

    # -----------------------------------------------------------------------
    print("\n[ETAPA 2] Bloqueando acesso público...")
    block_public_access(s3_client, bucket_name)

    # -----------------------------------------------------------------------
    if not args.skip_versioning:
        print("\n[ETAPA 3] Habilitando versionamento...")
        enable_versioning(s3_client, bucket_name)
    else:
        print("\n[ETAPA 3] Versionamento ignorado (--skip-versioning).")

    # -----------------------------------------------------------------------
    print("\n[ETAPA 4] Aplicando lifecycle policy...")
    apply_lifecycle_policy(s3_client, bucket_name)

    # -----------------------------------------------------------------------
    print("\n[ETAPA 5] Aplicando tags ao bucket...")
    apply_bucket_tags(s3_client, bucket_name)

    # -----------------------------------------------------------------------
    print("\n[ETAPA 6] Criando estrutura de prefixos...")
    create_prefix_structure(s3_client, bucket_name)

    # -----------------------------------------------------------------------
    print("\n" + "=" * 65)
    print("  RESUMO FINAL - S3 Setup Concluído")
    print("=" * 65)
    print(f"  Bucket             : s3://{bucket_name}/")
    print(f"  Região             : {region}")
    print(f"  Block Public Access: Habilitado")
    print(f"  Versionamento      : {'Desabilitado (--skip-versioning)' if args.skip_versioning else 'Habilitado'}")
    print(f"  Lifecycle policy   : quarantine→Glacier/90d | logs/30d | tmp/7d")
    print(f"  Prefixos criados   : {len(PREFIXES)}")
    print()
    print("  PRÓXIMOS PASSOS:")
    print("    python 02_setup_glue.py   # Databases, Crawlers, Role IAM e Jobs Glue")
    print("    python 03_setup_lambda.py # Lambda + EventBridge (pipeline event-driven)")
    print(f"    python src/ingestion/s3_data_loader.py --bucket-name {bucket_name}")
    print("=" * 65)
    print("  Setup S3 finalizado com sucesso!")
    print("=" * 65)


if __name__ == "__main__":
    main()
