"""
02_setup_glue.py
================
Contact Center Data Lakehouse - Glue Infrastructure Setup

Cria:
  1. IAM Role GlueExecutionRole (se não existir)
  2. Glue Databases: db_bronze, db_silver, db_gold
  3. 18 Glue Crawlers (um por tabela Bronze)
  4. Glue Workflow wf-cc-pipeline-diario
  5. Upload dos scripts PySpark para S3
  6. Registro dos 40 Glue Jobs (18 Bronze→Silver + 11 Dims + 11 Fatos)

Uso:
    python 02_setup_glue.py
    python 02_setup_glue.py --region us-east-1 --bucket-name act-cc-dev-lakehouse
    python 02_setup_glue.py --skip-upload  # pula o sync de scripts para S3

Requisitos:
    pip install boto3
    Credenciais AWS com permissões em Glue, IAM e S3
    aws s3 sync disponível no PATH (para upload dos scripts)
"""

import argparse
import json
import subprocess
import sys
import time
import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_BUCKET = "act-cc-dev-lakehouse"
DEFAULT_REGION = "us-east-1"

GLUE_DATABASES    = ["db_bronze", "db_silver", "db_gold"]
WORKFLOW_NAME     = "wf-cc-pipeline-diario"
GLUE_ROLE_NAME    = "GlueExecutionRole"
GLUE_VERSION      = "4.0"
WORKER_TYPE       = "G.1X"
NUM_WORKERS       = 2

# Mapeamento: nome_curto → prefixo S3 Bronze
TABLES = {
    "operacao_chamada":          "bronze/operacao/chamada/",
    "operacao_ticket":           "bronze/operacao/ticket/",
    "operacao_chat":             "bronze/operacao/chat/",
    "operacao_whatsapp":         "bronze/operacao/whatsapp/",
    "operacao_ura":              "bronze/operacao/ura/",
    "operacao_discagem":         "bronze/operacao/discagem/",
    "operacao_metricas":         "bronze/operacao/metricas/",
    "operacao_gravacao":         "bronze/operacao/gravacao/",
    "cadastro_cliente":          "bronze/cadastro/cliente/",
    "cadastro_endereco":         "bronze/cadastro/endereco/",
    "cadastro_operador":         "bronze/cadastro/operador/",
    "cadastro_skill":            "bronze/cadastro/skill/",
    "cadastro_fila":             "bronze/cadastro/fila/",
    "qualidade_avaliacao":       "bronze/qualidade/avaliacao/",
    "qualidade_jornada":         "bronze/qualidade/jornada/",
    "qualidade_ticket_interacao":"bronze/qualidade/ticket-interacao/",
    "marketing_campanha":        "bronze/marketing/campanha/",
    "suporte_mensagem_chat":     "bronze/suporte/mensagem-chat/",
}

# ---------------------------------------------------------------------------
# 40 Glue Jobs: 18 Bronze→Silver + 11 Dims Gold + 11 Fatos Gold
# script: caminho relativo dentro de s3://{BUCKET}/scripts/
# ---------------------------------------------------------------------------
GLUE_JOBS = [
    # ── Bronze → Silver (18 jobs) ──────────────────────────────────────────
    {
        "job_name": "job-tb-chamada-bronze-to-silver",
        "script":   "scripts/bronze_to_silver/job_tb_chamada_bronze_to_silver.py",
    },
    {
        "job_name": "job-tb-ticket-bronze-to-silver",
        "script":   "scripts/bronze_to_silver/job_tb_ticket_bronze_to_silver.py",
    },
    {
        "job_name": "job-tb-chat-bronze-to-silver",
        "script":   "scripts/bronze_to_silver/job_tb_chat_bronze_to_silver.py",
    },
    {
        "job_name": "job-tb-whatsapp-atendimento-bronze-to-silver",
        "script":   "scripts/bronze_to_silver/job_tb_whatsapp_atendimento_bronze_to_silver.py",
    },
    {
        "job_name": "job-tb-ura-navegacao-bronze-to-silver",
        "script":   "scripts/bronze_to_silver/job_tb_ura_navegacao_bronze_to_silver.py",
    },
    {
        "job_name": "job-tb-discagem-bronze-to-silver",
        "script":   "scripts/bronze_to_silver/job_tb_discagem_bronze_to_silver.py",
    },
    {
        "job_name": "job-tb-metricas-operacionais-bronze-to-silver",
        "script":   "scripts/bronze_to_silver/job_tb_metricas_operacionais_bronze_to_silver.py",
    },
    {
        "job_name": "job-tb-gravacao-chamada-bronze-to-silver",
        "script":   "scripts/bronze_to_silver/job_tb_gravacao_chamada_bronze_to_silver.py",
    },
    {
        "job_name": "job-tb-cliente-bronze-to-silver",
        "script":   "scripts/bronze_to_silver/job_tb_cliente_bronze_to_silver.py",
    },
    {
        "job_name": "job-tb-endereco-cliente-bronze-to-silver",
        "script":   "scripts/bronze_to_silver/job_tb_endereco_cliente_bronze_to_silver.py",
    },
    {
        "job_name": "job-tb-operador-bronze-to-silver",
        "script":   "scripts/bronze_to_silver/job_tb_operador_bronze_to_silver.py",
    },
    {
        "job_name": "job-tb-skill-operador-bronze-to-silver",
        "script":   "scripts/bronze_to_silver/job_tb_skill_operador_bronze_to_silver.py",
    },
    {
        "job_name": "job-tb-fila-atendimento-bronze-to-silver",
        "script":   "scripts/bronze_to_silver/job_tb_fila_atendimento_bronze_to_silver.py",
    },
    {
        "job_name": "job-tb-avaliacao-qualidade-bronze-to-silver",
        "script":   "scripts/bronze_to_silver/job_tb_avaliacao_qualidade_bronze_to_silver.py",
    },
    {
        "job_name": "job-tb-jornada-operador-bronze-to-silver",
        "script":   "scripts/bronze_to_silver/job_tb_jornada_operador_bronze_to_silver.py",
    },
    {
        "job_name": "job-tb-interacao-ticket-bronze-to-silver",
        "script":   "scripts/bronze_to_silver/job_tb_interacao_ticket_bronze_to_silver.py",
    },
    {
        "job_name": "job-tb-campanha-bronze-to-silver",
        "script":   "scripts/bronze_to_silver/job_tb_campanha_bronze_to_silver.py",
    },
    {
        "job_name": "job-tb-mensagem-chat-bronze-to-silver",
        "script":   "scripts/bronze_to_silver/job_tb_mensagem_chat_bronze_to_silver.py",
    },
    # ── Silver → Gold: Dimensões (11 jobs) ────────────────────────────────
    {
        "job_name": "job-dim-data-gold",
        "script":   "scripts/silver_to_gold/job_dim_data_gold.py",
    },
    {
        "job_name": "job-dim-cliente-gold",
        "script":   "scripts/silver_to_gold/job_dim_cliente_gold.py",
    },
    {
        "job_name": "job-dim-operador-gold",
        "script":   "scripts/silver_to_gold/job_dim_operador_gold.py",
    },
    {
        "job_name": "job-dim-campanha-gold",
        "script":   "scripts/silver_to_gold/job_dim_campanha_gold.py",
    },
    {
        "job_name": "job-dim-canal-gold",
        "script":   "scripts/silver_to_gold/job_dim_canal_gold.py",
    },
    {
        "job_name": "job-dim-fila-gold",
        "script":   "scripts/silver_to_gold/job_dim_fila_gold.py",
    },
    {
        "job_name": "job-dim-skill-gold",
        "script":   "scripts/silver_to_gold/job_dim_skill_gold.py",
    },
    {
        "job_name": "job-dim-status-chamada-gold",
        "script":   "scripts/silver_to_gold/job_dim_status_chamada_gold.py",
    },
    {
        "job_name": "job-dim-status-ticket-gold",
        "script":   "scripts/silver_to_gold/job_dim_status_ticket_gold.py",
    },
    {
        "job_name": "job-dim-categoria-ticket-gold",
        "script":   "scripts/silver_to_gold/job_dim_categoria_ticket_gold.py",
    },
    {
        "job_name": "job-dim-prioridade-ticket-gold",
        "script":   "scripts/silver_to_gold/job_dim_prioridade_ticket_gold.py",
    },
    # ── Silver → Gold: Fatos (11 jobs) ────────────────────────────────────
    {
        "job_name": "job-fato-chamada-gold",
        "script":   "scripts/silver_to_gold/job_fato_chamada_gold.py",
    },
    {
        "job_name": "job-fato-ticket-gold",
        "script":   "scripts/silver_to_gold/job_fato_ticket_gold.py",
    },
    {
        "job_name": "job-fato-chat-gold",
        "script":   "scripts/silver_to_gold/job_fato_chat_gold.py",
    },
    {
        "job_name": "job-fato-whatsapp-gold",
        "script":   "scripts/silver_to_gold/job_fato_whatsapp_gold.py",
    },
    {
        "job_name": "job-fato-ura-navegacao-gold",
        "script":   "scripts/silver_to_gold/job_fato_ura_navegacao_gold.py",
    },
    {
        "job_name": "job-fato-discagem-gold",
        "script":   "scripts/silver_to_gold/job_fato_discagem_gold.py",
    },
    {
        "job_name": "job-fato-metricas-operacionais-gold",
        "script":   "scripts/silver_to_gold/job_fato_metricas_operacionais_gold.py",
    },
    {
        "job_name": "job-fato-mensagem-chat-gold",
        "script":   "scripts/silver_to_gold/job_fato_mensagem_chat_gold.py",
    },
    {
        "job_name": "job-fato-interacao-ticket-gold",
        "script":   "scripts/silver_to_gold/job_fato_interacao_ticket_gold.py",
    },
    {
        "job_name": "job-fato-qualidade-gold",
        "script":   "scripts/silver_to_gold/job_fato_qualidade_gold.py",
    },
    {
        "job_name": "job-fato-jornada-operador-gold",
        "script":   "scripts/silver_to_gold/job_fato_jornada_operador_gold.py",
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


# ---------------------------------------------------------------------------
# IAM Role
# ---------------------------------------------------------------------------

def create_glue_execution_role(
    iam_client,
    account_id: str,
    region: str,
    bucket_name: str,
) -> str:
    """
    Cria a IAM role GlueExecutionRole com permissões para:
      - AWSGlueServiceRole (managed): Glue, S3 padrão, CloudWatch
      - Inline policy S3: leitura e escrita no bucket do projeto
      - Inline policy Lake Formation: GetDataAccess (para tabelas Iceberg)
    Retorna o ARN da role.
    """
    role_arn = f"arn:aws:iam::{account_id}:role/{GLUE_ROLE_NAME}"

    try:
        response = iam_client.get_role(RoleName=GLUE_ROLE_NAME)
        print(f"  [OK] Role '{GLUE_ROLE_NAME}' já existe: {role_arn}")
        return response["Role"]["Arn"]
    except iam_client.exceptions.NoSuchEntityException:
        pass

    assume_role_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "glue.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    })

    print(f"  [INFO] Criando role '{GLUE_ROLE_NAME}'...")
    try:
        response = iam_client.create_role(
            RoleName=GLUE_ROLE_NAME,
            AssumeRolePolicyDocument=assume_role_policy,
            Description="Role de execução Glue ETL — Contact Center Data Lakehouse",
            Tags=[
                {"Key": "project",     "Value": "contact-center-data-lakehouse"},
                {"Key": "created_by",  "Value": "02_setup_glue.py"},
            ],
        )
        role_arn = response["Role"]["Arn"]
        print(f"  [+] Role criada: {role_arn}")
    except ClientError as e:
        print(f"  [ERRO] Falha ao criar role: {e}")
        raise

    # Managed policy: cobertura base do Glue (CloudWatch, S3 padrão, Glue Data Catalog)
    try:
        iam_client.attach_role_policy(
            RoleName=GLUE_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
        )
        print("  [+] Managed policy AWSGlueServiceRole anexada.")
    except ClientError as e:
        print(f"  [ERRO] Falha ao anexar managed policy: {e}")
        raise

    # Inline policy: acesso completo ao bucket do projeto
    s3_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "ProjectBucketAccess",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket",
                    "s3:GetBucketLocation",
                    "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts",
                    "s3:GetObjectVersion",
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket_name}",
                    f"arn:aws:s3:::{bucket_name}/*",
                ],
            }
        ],
    })

    try:
        iam_client.put_role_policy(
            RoleName=GLUE_ROLE_NAME,
            PolicyName="GlueProjectBucketAccess",
            PolicyDocument=s3_policy,
        )
        print("  [+] Inline policy S3 (bucket do projeto) anexada.")
    except ClientError as e:
        print(f"  [ERRO] Falha ao adicionar inline policy S3: {e}")
        raise

    # Inline policy: Lake Formation GetDataAccess para tabelas Iceberg
    lf_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "LakeFormationDataAccess",
                "Effect": "Allow",
                "Action": ["lakeformation:GetDataAccess"],
                "Resource": "*",
            }
        ],
    })

    try:
        iam_client.put_role_policy(
            RoleName=GLUE_ROLE_NAME,
            PolicyName="GlueLakeFormationAccess",
            PolicyDocument=lf_policy,
        )
        print("  [+] Inline policy Lake Formation anexada.")
    except ClientError as e:
        print(f"  [ERRO] Falha ao adicionar inline policy Lake Formation: {e}")
        raise

    print("  [INFO] Aguardando propagação da role IAM (15s)...")
    time.sleep(15)

    return role_arn


# ---------------------------------------------------------------------------
# Glue Databases
# ---------------------------------------------------------------------------

def create_glue_database(glue_client, db_name: str) -> None:
    try:
        glue_client.get_database(Name=db_name)
        print(f"    [OK] Database '{db_name}' já existe — pulando.")
    except glue_client.exceptions.EntityNotFoundException:
        try:
            glue_client.create_database(
                DatabaseInput={
                    "Name": db_name,
                    "Description": (
                        f"Contact Center Data Lakehouse — "
                        f"camada {db_name.split('_')[1].upper()}"
                    ),
                    "Parameters": {
                        "project":    "contact-center-data-lakehouse",
                        "layer":      db_name.split("_")[1],
                        "created_by": "02_setup_glue.py",
                    },
                }
            )
            print(f"    [+] Database criado: {db_name}")
        except ClientError as e:
            print(f"    [ERRO] Falha ao criar database '{db_name}': {e}")
            raise


# ---------------------------------------------------------------------------
# Glue Crawlers
# ---------------------------------------------------------------------------

def json_config() -> str:
    config = {
        "Version": 1.0,
        "CrawlerOutput": {
            "Partitions": {"AddOrUpdateBehavior": "InheritFromTable"},
            "Tables":     {"AddOrUpdateBehavior": "MergeNewColumns"},
        },
        "Grouping": {
            "TableGroupingPolicy":    "CombineCompatibleSchemas",
            "TableLevelConfiguration": 4,
        },
    }
    return json.dumps(config)


def create_glue_crawler(
    glue_client,
    table_name: str,
    s3_path: str,
    role_arn: str,
) -> None:
    crawler_name = f"crawler-{table_name.replace('_', '-')}"

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
            Description=f"Crawler Bronze: {table_name}",
            Targets={
                "S3Targets": [
                    {
                        "Path":       s3_path,
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
                "project":    "contact-center-data-lakehouse",
                "table":      table_name,
                "layer":      "bronze",
                "created_by": "02_setup_glue.py",
            },
        )
        print(f"    [+] Crawler criado: {crawler_name}  →  {s3_path}")
    except ClientError as e:
        print(f"    [ERRO] Falha ao criar crawler '{crawler_name}': {e}")
        raise


# ---------------------------------------------------------------------------
# Glue Workflow
# ---------------------------------------------------------------------------

def create_glue_workflow(glue_client) -> None:
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
                "Orquestra crawlers e jobs Bronze→Silver→Gold."
            ),
            DefaultRunProperties={
                "project":     "contact-center-data-lakehouse",
                "environment": "dev",
            },
            Tags={
                "project":    "contact-center-data-lakehouse",
                "created_by": "02_setup_glue.py",
            },
            MaxConcurrentRuns=1,
        )
        print(f"  [+] Workflow criado: {WORKFLOW_NAME}")
    except ClientError as e:
        print(f"  [ERRO] Falha ao criar workflow '{WORKFLOW_NAME}': {e}")
        raise


# ---------------------------------------------------------------------------
# Upload de scripts para S3
# ---------------------------------------------------------------------------

def upload_scripts(bucket_name: str, region: str) -> None:
    """
    Faz sync dos scripts PySpark locais para s3://{bucket}/scripts/
    usando a AWS CLI. Requer que a CLI esteja instalada e autenticada.
    """
    import os
    # Detecta raiz do projeto (dois níveis acima de infrastructure/)
    script_dir   = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    pipeline_dir = os.path.join(project_root, "pipeline")

    if not os.path.isdir(pipeline_dir):
        print(f"  [AVISO] Diretório pipeline/ não encontrado em: {pipeline_dir}")
        print("  Pulando upload de scripts.")
        return

    dest = f"s3://{bucket_name}/scripts/"
    cmd  = [
        "aws", "s3", "sync",
        pipeline_dir, dest,
        "--exclude", "*.pyc",
        "--exclude", "__pycache__/*",
        "--exclude", "README.md",
        "--region", region,
    ]

    print(f"  [INFO] Enviando pipeline/ → {dest}")
    print(f"         Comando: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"  [+] Upload concluído.")
            if result.stdout:
                for line in result.stdout.strip().splitlines()[:10]:
                    print(f"      {line}")
        else:
            print(f"  [AVISO] aws s3 sync retornou código {result.returncode}:")
            print(f"  {result.stderr}")
    except FileNotFoundError:
        print(
            "  [AVISO] AWS CLI não encontrada. Faça upload manual:\n"
            f"    aws s3 sync pipeline/ {dest}"
        )


# ---------------------------------------------------------------------------
# Registro dos 40 Glue Jobs
# ---------------------------------------------------------------------------

def register_glue_job(
    glue_client,
    job_name: str,
    script_s3: str,
    role_arn: str,
    bucket_name: str,
    region: str,
) -> None:
    """Cria um Glue Job se não existir."""
    try:
        glue_client.get_job(JobName=job_name)
        print(f"    [OK] Job '{job_name}' já existe — pulando.")
        return
    except glue_client.exceptions.EntityNotFoundException:
        pass

    # Argumentos padrão passados a todos os jobs
    default_args = {
        "--JOB_NAME":              job_name,
        "--BUCKET_NAME":           bucket_name,
        "--ENV":                   "dev",
        "--enable-metrics":        "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--enable-job-insights":   "true",
        "--datalake-formats":      "iceberg",            # habilita Iceberg no Glue 4.0
        "--TempDir":               f"s3://{bucket_name}/tmp/glue/{job_name}/",
        "--enable-glue-datacatalog": "true",
    }

    try:
        glue_client.create_job(
            Name=job_name,
            Role=role_arn,
            Command={
                "Name":           "glueetl",
                "ScriptLocation": script_s3,
                "PythonVersion":  "3",
            },
            DefaultArguments=default_args,
            GlueVersion=GLUE_VERSION,
            WorkerType=WORKER_TYPE,
            NumberOfWorkers=NUM_WORKERS,
            Timeout=60,              # minutos — encerra job travado
            MaxRetries=0,
            ExecutionProperty={"MaxConcurrentRuns": 1},
            Description=f"Contact Center Data Lakehouse — {job_name}",
            Tags={
                "project":    "contact-center-data-lakehouse",
                "created_by": "02_setup_glue.py",
            },
        )
        print(f"    [+] Job registrado: {job_name}")
    except ClientError as e:
        print(f"    [ERRO] Falha ao registrar job '{job_name}': {e}")
        raise


def register_all_jobs(
    glue_client,
    role_arn: str,
    bucket_name: str,
    region: str,
) -> None:
    total   = len(GLUE_JOBS)
    created = 0
    skipped = 0

    for job in GLUE_JOBS:
        script_s3 = f"s3://{bucket_name}/{job['script']}"
        try:
            glue_client.get_job(JobName=job["job_name"])
            print(f"    [OK] Job '{job['job_name']}' já existe — pulando.")
            skipped += 1
        except glue_client.exceptions.EntityNotFoundException:
            register_glue_job(
                glue_client,
                job_name=job["job_name"],
                script_s3=script_s3,
                role_arn=role_arn,
                bucket_name=bucket_name,
                region=region,
            )
            created += 1

    print(f"\n  Jobs registrados: {created} criados | {skipped} já existiam | {total} total")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Setup Glue para Contact Center Data Lakehouse"
    )
    parser.add_argument("--bucket-name",   default=DEFAULT_BUCKET)
    parser.add_argument("--region",        default=DEFAULT_REGION)
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Não faz upload dos scripts para S3 (execute manualmente antes)",
    )
    return parser.parse_args()


def main():
    args        = parse_args()
    bucket_name = args.bucket_name
    region      = args.region

    print("=" * 65)
    print("  Contact Center Data Lakehouse — Setup Glue")
    print("=" * 65)
    print(f"  Bucket : {bucket_name}")
    print(f"  Região : {region}")
    print("=" * 65)

    iam_client  = boto3.client("iam",  region_name=region)
    glue_client = boto3.client("glue", region_name=region)

    # -----------------------------------------------------------------------
    print("\n[ETAPA 1] Identificando conta AWS...")
    account_id = get_account_id()

    # -----------------------------------------------------------------------
    print(f"\n[ETAPA 2] Criando IAM Role '{GLUE_ROLE_NAME}'...")
    role_arn = create_glue_execution_role(
        iam_client,
        account_id=account_id,
        region=region,
        bucket_name=bucket_name,
    )

    # -----------------------------------------------------------------------
    print("\n[ETAPA 3] Criando Glue Databases...")
    for db_name in GLUE_DATABASES:
        print(f"\n  Database: {db_name}")
        create_glue_database(glue_client, db_name)

    # -----------------------------------------------------------------------
    print("\n[ETAPA 4] Criando 18 Glue Crawlers (um por tabela Bronze)...")
    for table_name, prefix in TABLES.items():
        s3_path = f"s3://{bucket_name}/{prefix}"
        print(f"\n  Tabela: {table_name}")
        create_glue_crawler(glue_client, table_name, s3_path, role_arn)

    # -----------------------------------------------------------------------
    print(f"\n[ETAPA 5] Criando Glue Workflow '{WORKFLOW_NAME}'...")
    create_glue_workflow(glue_client)

    # -----------------------------------------------------------------------
    if not args.skip_upload:
        print("\n[ETAPA 6] Fazendo upload dos scripts PySpark para S3...")
        upload_scripts(bucket_name, region)
    else:
        print("\n[ETAPA 6] Upload de scripts ignorado (--skip-upload).")
        print(
            f"  Execute manualmente:\n"
            f"    aws s3 sync pipeline/ s3://{bucket_name}/scripts/ --region {region}"
        )

    # -----------------------------------------------------------------------
    print(f"\n[ETAPA 7] Registrando {len(GLUE_JOBS)} Glue Jobs...")
    register_all_jobs(glue_client, role_arn, bucket_name, region)

    # -----------------------------------------------------------------------
    print("\n" + "=" * 65)
    print("  RESUMO FINAL - Glue Setup Concluído")
    print("=" * 65)
    print(f"  Conta AWS          : {account_id}")
    print(f"  Região             : {region}")
    print(f"  Role IAM           : {role_arn}")
    print(f"  Databases criados  : {len(GLUE_DATABASES)} ({', '.join(GLUE_DATABASES)})")
    print(f"  Crawlers criados   : {len(TABLES)}")
    print(f"  Workflow           : {WORKFLOW_NAME}")
    print(f"  Jobs registrados   : {len(GLUE_JOBS)} (18 Bronze→Silver + 22 Silver→Gold)")
    print()
    print("  PRÓXIMOS PASSOS:")
    print("    1. Carregue os dados sintéticos:")
    print(f"       python pipeline/ingestion/s3_data_loader.py --bucket-name {bucket_name}")
    print("    2. Execute os crawlers Bronze (no Console Glue ou via CLI):")
    print(f"       aws glue start-crawler --name crawler-operacao-chamada --region {region}")
    print("    3. Execute os jobs Bronze→Silver sequencialmente:")
    print(f"       aws glue start-job-run --job-name job-tb-chamada-bronze-to-silver")
    print("    4. Execute os jobs Silver→Gold (Dims antes de Fatos).")
    print("=" * 65)
    print("  Setup Glue finalizado com sucesso!")
    print("=" * 65)


if __name__ == "__main__":
    main()
