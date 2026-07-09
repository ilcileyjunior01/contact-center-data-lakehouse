"""
02_setup_glue.py
================
Contact Center Data Lakehouse - Glue Infrastructure Setup

Cria Glue Databases, Crawlers (um por tabela), Workflow e documenta
os jobs que devem ser registrados.

Uso:
    python 02_setup_glue.py
    python 02_setup_glue.py --region us-east-1 --bucket-name act-cc-dev-lakehouse

Requisitos:
    pip install boto3
    Credenciais AWS configuradas com permissão em Glue e IAM
"""

import argparse
import sys
import time
import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_BUCKET = "act-cc-dev-lakehouse"
DEFAULT_REGION = "us-east-1"

GLUE_DATABASES = ["db_bronze", "db_silver", "db_gold"]

WORKFLOW_NAME = "wf-cc-pipeline-diario"

# Mapeamento: nome_curto -> path S3 (prefixo relativo ao bucket)
# 18 tabelas conforme especificação
TABLES = {
    # Domínio: operacao
    "operacao_chamada":         "bronze/operacao/chamada/",
    "operacao_ticket":          "bronze/operacao/ticket/",
    "operacao_chat":            "bronze/operacao/chat/",
    "operacao_whatsapp":        "bronze/operacao/whatsapp/",
    "operacao_ura":             "bronze/operacao/ura/",
    "operacao_discagem":        "bronze/operacao/discagem/",
    "operacao_metricas":        "bronze/operacao/metricas/",
    "operacao_gravacao":        "bronze/operacao/gravacao/",
    # Domínio: cadastro
    "cadastro_cliente":         "bronze/cadastro/cliente/",
    "cadastro_endereco":        "bronze/cadastro/endereco/",
    "cadastro_operador":        "bronze/cadastro/operador/",
    "cadastro_skill":           "bronze/cadastro/skill/",
    "cadastro_fila":            "bronze/cadastro/fila/",
    # Domínio: qualidade
    "qualidade_avaliacao":      "bronze/qualidade/avaliacao/",
    "qualidade_jornada":        "bronze/qualidade/jornada/",
    "qualidade_ticket_interacao":"bronze/qualidade/ticket-interacao/",
    # Domínio: marketing
    "marketing_campanha":       "bronze/marketing/campanha/",
    # Domínio: suporte
    "suporte_mensagem_chat":    "bronze/suporte/mensagem-chat/",
}

# Jobs que serão registrados (documentação — não criamos os jobs aqui)
GLUE_JOBS = [
    {
        "job_name": "job-bronze-to-silver-chamada",
        "script":   "scripts/bronze_to_silver_chamada.py",
        "source":   "bronze/operacao/chamada/",
        "target":   "silver/operacao/chamada/",
    },
    {
        "job_name": "job-bronze-to-silver-ticket",
        "script":   "scripts/bronze_to_silver_ticket.py",
        "source":   "bronze/operacao/ticket/",
        "target":   "silver/operacao/ticket/",
    },
    {
        "job_name": "job-bronze-to-silver-chat",
        "script":   "scripts/bronze_to_silver_chat.py",
        "source":   "bronze/operacao/chat/",
        "target":   "silver/operacao/chat/",
    },
    {
        "job_name": "job-bronze-to-silver-whatsapp",
        "script":   "scripts/bronze_to_silver_whatsapp.py",
        "source":   "bronze/operacao/whatsapp/",
        "target":   "silver/operacao/whatsapp/",
    },
    {
        "job_name": "job-bronze-to-silver-ura",
        "script":   "scripts/bronze_to_silver_ura.py",
        "source":   "bronze/operacao/ura/",
        "target":   "silver/operacao/ura/",
    },
    {
        "job_name": "job-bronze-to-silver-cadastro",
        "script":   "scripts/bronze_to_silver_cadastro.py",
        "source":   "bronze/cadastro/",
        "target":   "silver/cadastro/",
    },
    {
        "job_name": "job-bronze-to-silver-qualidade",
        "script":   "scripts/bronze_to_silver_qualidade.py",
        "source":   "bronze/qualidade/",
        "target":   "silver/qualidade/",
    },
    {
        "job_name": "job-silver-to-gold-kpis",
        "script":   "scripts/silver_to_gold_kpis.py",
        "source":   "silver/",
        "target":   "gold/",
    },
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_account_id() -> str:
    sts = boto3.client("sts")
    identity = sts.get_caller_identity()
    account_id = identity["Account"]
    print(f"  [STS] Account ID: {account_id}")
    return account_id


def create_glue_database(glue_client, db_name: str) -> None:
    """Cria Glue Database se não existir."""
    try:
        glue_client.get_database(Name=db_name)
        print(f"    [OK] Database '{db_name}' já existe — pulando.")
    except glue_client.exceptions.EntityNotFoundException:
        try:
            glue_client.create_database(
                DatabaseInput={
                    "Name": db_name,
                    "Description": f"Contact Center Data Lakehouse — camada {db_name.split('_')[1].upper()}",
                    "Parameters": {
                        "project": "contact-center-data-lakehouse",
                        "layer": db_name.split("_")[1],
                        "created_by": "02_setup_glue.py",
                    },
                }
            )
            print(f"    [+] Database criado: {db_name}")
        except ClientError as e:
            print(f"    [ERRO] Falha ao criar database '{db_name}': {e}")
            raise


def create_glue_crawler(
    glue_client,
    table_name: str,
    s3_path: str,
    account_id: str,
    region: str,
) -> None:
    """
    Cria Glue Crawler para uma tabela específica.
    - Role: GlueExecutionRole (deve ser criada previamente ou via Terraform)
    - Target: s3_path no bucket
    - Database: db_bronze
    - Recrawl: CRAWL_NEW_FOLDERS_ONLY
    """
    crawler_name = f"crawler-{table_name.replace('_', '-')}"
    role_arn = f"arn:aws:iam::{account_id}:role/GlueExecutionRole"

    try:
        glue_client.get_crawler(Name=crawler_name)
        print(f"    [OK] Crawler '{crawler_name}' já existe — pulando.")
        return
    except glue_client.exceptions.EntityNotFoundException:
        pass

    try:
        glue_client.create_crawler(
            Name=crawler_name,
            Role=role_arn,
            DatabaseName="db_bronze",
            Description=f"Crawler para tabela bronze: {table_name}",
            Targets={
                "S3Targets": [
                    {
                        "Path": s3_path,
                        "Exclusions": ["_temporary/**", "_spark_metadata/**"],
                    }
                ]
            },
            SchemaChangePolicy={
                "UpdateBehavior": "LOG",
                "DeleteBehavior": "LOG",
            },
            RecrawlPolicy={
                "RecrawlBehavior": "CRAWL_NEW_FOLDERS_ONLY",
            },
            LineageConfiguration={
                "CrawlerLineageSettings": "ENABLE",
            },
            Configuration=json_config(),
            Tags={
                "project": "contact-center-data-lakehouse",
                "table": table_name,
                "layer": "bronze",
                "created_by": "02_setup_glue.py",
            },
        )
        print(f"    [+] Crawler criado: {crawler_name}")
        print(f"        Role   : {role_arn}")
        print(f"        Target : {s3_path}")
        print(f"        DB     : db_bronze")
    except ClientError as e:
        print(f"    [ERRO] Falha ao criar crawler '{crawler_name}': {e}")
        raise


def json_config() -> str:
    """Retorna configuração JSON do crawler para inferência de tipos e agrupamento."""
    import json
    config = {
        "Version": 1.0,
        "CrawlerOutput": {
            "Partitions": {"AddOrUpdateBehavior": "InheritFromTable"},
            "Tables": {"AddOrUpdateBehavior": "MergeNewColumns"},
        },
        "Grouping": {
            "TableGroupingPolicy": "CombineCompatibleSchemas",
            "TableLevelConfiguration": 4,
        },
    }
    return json.dumps(config)


def create_glue_workflow(glue_client) -> None:
    """Cria o Glue Workflow principal do pipeline diário."""
    try:
        glue_client.get_workflow(Name=WORKFLOW_NAME)
        print(f"  [OK] Workflow '{WORKFLOW_NAME}' já existe — pulando.")
        return
    except glue_client.exceptions.EntityNotFoundException:
        pass

    try:
        glue_client.create_workflow(
            Name=WORKFLOW_NAME,
            Description=(
                "Pipeline diário Contact Center Data Lakehouse. "
                "Orquestra crawlers e jobs Bronze->Silver->Gold."
            ),
            DefaultRunProperties={
                "project": "contact-center-data-lakehouse",
                "environment": "dev",
            },
            Tags={
                "project": "contact-center-data-lakehouse",
                "created_by": "02_setup_glue.py",
            },
            MaxConcurrentRuns=1,
        )
        print(f"  [+] Workflow criado: {WORKFLOW_NAME}")
    except ClientError as e:
        print(f"  [ERRO] Falha ao criar workflow '{WORKFLOW_NAME}': {e}")
        raise


def print_job_upload_guide(bucket_name: str) -> None:
    """Imprime guia de como fazer upload dos scripts de job para o S3."""
    print("\n" + "=" * 65)
    print("  GUIA: Upload de Scripts Glue Job")
    print("=" * 65)
    print(
        "  Os jobs abaixo devem ser registrados manualmente após upload dos\n"
        "  scripts PySpark para o S3. Siga os passos:\n"
    )
    print("  1. Coloque os scripts PySpark em pipeline/glue_jobs/")
    print(
        f"  2. Faça upload para s3://{bucket_name}/scripts/\n"
        f"     aws s3 sync pipeline/glue_jobs/ s3://{bucket_name}/scripts/\n"
    )
    print("  3. Registre cada job no Glue Console ou via CLI:\n")

    for job in GLUE_JOBS:
        script_s3 = f"s3://{bucket_name}/{job['script']}"
        print(f"  Job: {job['job_name']}")
        print(f"    Script  : {script_s3}")
        print(f"    Source  : s3://{bucket_name}/{job['source']}")
        print(f"    Target  : s3://{bucket_name}/{job['target']}")
        print(
            f"    Exemplo CLI:\n"
            f"      aws glue create-job \\\n"
            f"        --name {job['job_name']} \\\n"
            f"        --role GlueExecutionRole \\\n"
            f"        --command Name=glueetl,ScriptLocation={script_s3},PythonVersion=3 \\\n"
            f"        --glue-version 4.0 \\\n"
            f"        --number-of-workers 2 \\\n"
            f"        --worker-type G.1X\n"
        )

    print("  4. Adicione os jobs ao workflow via Glue Console -> Triggers")
    print("=" * 65)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Setup Glue para Contact Center Data Lakehouse"
    )
    parser.add_argument("--bucket-name", default=DEFAULT_BUCKET)
    parser.add_argument("--region", default=DEFAULT_REGION)
    return parser.parse_args()


def main():
    # Importação aqui para evitar erro caso o módulo seja importado sem argparse
    import json  # noqa — usado em json_config()

    args = parse_args()
    bucket_name = args.bucket_name
    region = args.region

    print("=" * 65)
    print("  Contact Center Data Lakehouse — Setup Glue")
    print("=" * 65)
    print(f"  Bucket : {bucket_name}")
    print(f"  Região : {region}")
    print("=" * 65)

    glue_client = boto3.client("glue", region_name=region)

    print("\n[ETAPA 1] Identificando conta AWS...")
    account_id = get_account_id()

    # Monta o S3 path completo com o bucket para cada tabela
    tables_with_full_path = {
        table: f"s3://{bucket_name}/{prefix}"
        for table, prefix in TABLES.items()
    }

    # -----------------------------------------------------------------------
    print("\n[ETAPA 2] Criando Glue Databases...")
    for db_name in GLUE_DATABASES:
        print(f"\n  Database: {db_name}")
        create_glue_database(glue_client, db_name)

    # -----------------------------------------------------------------------
    print("\n[ETAPA 3] Criando 18 Glue Crawlers (um por tabela)...")
    crawler_count = 0
    for table_name, s3_path in tables_with_full_path.items():
        print(f"\n  Tabela: {table_name}")
        create_glue_crawler(
            glue_client,
            table_name=table_name,
            s3_path=s3_path,
            account_id=account_id,
            region=region,
        )
        crawler_count += 1

    # -----------------------------------------------------------------------
    print(f"\n[ETAPA 4] Criando Glue Workflow '{WORKFLOW_NAME}'...")
    create_glue_workflow(glue_client)

    # -----------------------------------------------------------------------
    print("\n[ETAPA 5] Guia de upload dos Glue Job scripts...")
    print_job_upload_guide(bucket_name)

    # -----------------------------------------------------------------------
    # Resumo Final
    # -----------------------------------------------------------------------
    print("\n" + "=" * 65)
    print("  RESUMO FINAL - Glue Setup Concluído")
    print("=" * 65)
    print(f"  Conta AWS          : {account_id}")
    print(f"  Região             : {region}")
    print(f"  Databases criados  : {len(GLUE_DATABASES)} ({', '.join(GLUE_DATABASES)})")
    print(f"  Crawlers criados   : {crawler_count}")
    print(f"  Workflow           : {WORKFLOW_NAME}")
    print(f"  Jobs a registrar   : {len(GLUE_JOBS)} (veja guia acima)")
    print()
    print("  IMPORTANTE:")
    print(f"  Certifique-se de que a role GlueExecutionRole existe:")
    print(f"  arn:aws:iam::{account_id}:role/GlueExecutionRole")
    print("  e possui policies: AWSGlueServiceRole, S3 read/write no bucket.")
    print("=" * 65)
    print("  Setup Glue finalizado com sucesso!")
    print("=" * 65)


if __name__ == "__main__":
    import json  # necessário para json_config()
    main()
