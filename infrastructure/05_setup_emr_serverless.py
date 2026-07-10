"""
05_setup_emr_serverless.py
==========================
Contact Center Data Lakehouse - EMR Serverless Setup

Cria a aplicação EMR Serverless Spark, a IAM role de execução com
permissões S3/Glue/CloudWatch, e disponibiliza uma função auxiliar
submit_spark_job() para submeter jobs PySpark.

Uso:
    python 05_setup_emr_serverless.py
    python 05_setup_emr_serverless.py --region us-east-1 --bucket-name act-cc-dev-lakehouse
    python 05_setup_emr_serverless.py --submit-example  # submete job de exemplo

Requisitos:
    pip install boto3
    Credenciais AWS com permissões em EMR Serverless, IAM, S3

IMPORTANTE:
    Preencha PLACEHOLDER_SUBNET_IDS e PLACEHOLDER_SECURITY_GROUP_IDS
    com os valores reais da sua VPC antes de executar.
"""

import argparse
import json
import sys
import time
import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_REGION = "us-east-1"
DEFAULT_BUCKET = "act-cc-dev-lakehouse"
APP_NAME = "app-cc-spark"
EMR_RELEASE = "emr-6.15.0"
EXECUTION_ROLE_NAME = "EMRServerlessExecutionRole"

# PREENCHA com os valores reais da sua VPC:
PLACEHOLDER_SUBNET_IDS = [
    "subnet-XXXXXXXXXXXXXXXXX",   # <-- subnet privada (NAT Gateway obrigatório para EMR)
    "subnet-YYYYYYYYYYYYYYYYY",
]
PLACEHOLDER_SECURITY_GROUP_IDS = [
    "sg-XXXXXXXXXXXXXXXXX",       # <-- Security Group com saída para internet/S3
]

# Configuração de capacidade
INITIAL_CAPACITY = {
    "DRIVER": {
        "workerCount": 1,
        "workerConfiguration": {
            "cpu": "2vCPU",
            "memory": "4GB",
            "disk": "20GB",
        },
    },
    "EXECUTOR": {
        "workerCount": 2,
        "workerConfiguration": {
            "cpu": "4vCPU",
            "memory": "8GB",
            "disk": "20GB",
        },
    },
}

MAXIMUM_CAPACITY = {
    "cpu": "16vCPU",   # máximo 4 executores × 4 vCPU
    "memory": "64GB",  # máximo para jobs de transformação Silver/Gold
    "disk": "100GB",
}

# Auto-stop: encerra workers ociosos após 15 minutos (economiza custo)
AUTO_STOP_IDLE_TIMEOUT_MINUTES = 15


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_account_id() -> str:
    sts = boto3.client("sts")
    identity = sts.get_caller_identity()
    account_id = identity["Account"]
    print(f"  [STS] Account ID: {account_id}")
    return account_id


def find_application_by_name(emr_client, app_name: str) -> dict | None:
    """Busca uma aplicação EMR Serverless pelo nome. Retorna None se não encontrar."""
    try:
        paginator = emr_client.get_paginator("list_applications")
        for page in paginator.paginate(states=["CREATED", "STARTED", "STOPPED"]):
            for app in page.get("applications", []):
                if app["name"] == app_name:
                    # Busca detalhes completos
                    detail = emr_client.get_application(applicationId=app["id"])
                    return detail["application"]
    except ClientError as e:
        print(f"  [ERRO] Falha ao listar aplicações EMR: {e}")
        raise
    return None


# ---------------------------------------------------------------------------
# IAM Role
# ---------------------------------------------------------------------------

def create_emr_execution_role(
    iam_client,
    account_id: str,
    region: str,
    bucket_name: str,
) -> str:
    """
    Cria a IAM role EMRServerlessExecutionRole com permissões para:
    - S3: leitura e escrita no bucket do projeto
    - Glue: GetDatabase, GetTable, GetPartition, CreateTable, UpdateTable
    - CloudWatch Logs: criar grupos e streams de log
    Retorna o ARN da role.
    """
    role_arn = f"arn:aws:iam::{account_id}:role/{EXECUTION_ROLE_NAME}"

    # Verifica se já existe
    try:
        response = iam_client.get_role(RoleName=EXECUTION_ROLE_NAME)
        print(f"  [OK] Role '{EXECUTION_ROLE_NAME}' já existe: {role_arn}")
        return response["Role"]["Arn"]
    except iam_client.exceptions.NoSuchEntityException:
        pass

    # Trust policy para EMR Serverless
    assume_role_policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "emr-serverless.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                    "Condition": {
                        "StringEquals": {
                            "aws:SourceAccount": account_id,
                        },
                        "ArnLike": {
                            "aws:SourceArn": f"arn:aws:emr-serverless:{region}:{account_id}:/applications/*",
                        },
                    },
                }
            ],
        }
    )

    print(f"  [INFO] Criando role '{EXECUTION_ROLE_NAME}'...")
    try:
        response = iam_client.create_role(
            RoleName=EXECUTION_ROLE_NAME,
            AssumeRolePolicyDocument=assume_role_policy,
            Description="Role de execução EMR Serverless — Contact Center Data Lakehouse",
            Tags=[
                {"Key": "project", "Value": "contact-center-data-lakehouse"},
                {"Key": "created_by", "Value": "05_setup_emr_serverless.py"},
            ],
        )
        role_arn = response["Role"]["Arn"]
        print(f"  [+] Role criada: {role_arn}")
    except ClientError as e:
        print(f"  [ERRO] Falha ao criar role: {e}")
        raise

    # Inline policy consolidada
    inline_policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                # S3: leitura e escrita no bucket do projeto
                {
                    "Sid": "S3BucketAccess",
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:ListBucket",
                        "s3:GetBucketLocation",
                        "s3:AbortMultipartUpload",
                        "s3:ListMultipartUploadParts",
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{bucket_name}",
                        f"arn:aws:s3:::{bucket_name}/*",
                    ],
                },
                # Glue Data Catalog
                {
                    "Sid": "GlueDataCatalog",
                    "Effect": "Allow",
                    "Action": [
                        "glue:GetDatabase",
                        "glue:GetDatabases",
                        "glue:GetTable",
                        "glue:GetTables",
                        "glue:GetPartition",
                        "glue:GetPartitions",
                        "glue:BatchGetPartition",
                        "glue:CreateTable",
                        "glue:UpdateTable",
                        "glue:DeleteTable",
                        "glue:CreatePartition",
                        "glue:BatchCreatePartition",
                        "glue:UpdatePartition",
                    ],
                    "Resource": [
                        f"arn:aws:glue:{region}:{account_id}:catalog",
                        f"arn:aws:glue:{region}:{account_id}:database/*",
                        f"arn:aws:glue:{region}:{account_id}:table/*",
                    ],
                },
                # CloudWatch Logs
                {
                    "Sid": "CloudWatchLogs",
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents",
                        "logs:DescribeLogGroups",
                        "logs:DescribeLogStreams",
                    ],
                    "Resource": f"arn:aws:logs:{region}:{account_id}:log-group:/aws/emr-serverless/*",
                },
                # EC2 network interface (necessário para VPC mode)
                {
                    "Sid": "EC2NetworkAccess",
                    "Effect": "Allow",
                    "Action": [
                        "ec2:CreateNetworkInterface",
                        "ec2:DeleteNetworkInterface",
                        "ec2:DescribeNetworkInterfaces",
                        "ec2:DescribeSecurityGroups",
                        "ec2:DescribeSubnets",
                        "ec2:DescribeVpcs",
                    ],
                    "Resource": "*",
                },
            ],
        }
    )

    try:
        iam_client.put_role_policy(
            RoleName=EXECUTION_ROLE_NAME,
            PolicyName="EMRServerlessExecutionInlinePolicy",
            PolicyDocument=inline_policy,
        )
        print("  [+] Inline policy anexada à role.")
    except ClientError as e:
        print(f"  [ERRO] Falha ao anexar inline policy: {e}")
        raise

    print("  [INFO] Aguardando propagação da role IAM (15s)...")
    time.sleep(15)

    return role_arn


# ---------------------------------------------------------------------------
# EMR Serverless Application
# ---------------------------------------------------------------------------

def create_emr_application(
    emr_client,
    app_name: str,
    subnet_ids: list,
    security_group_ids: list,
    region: str,
) -> dict:
    """
    Cria a aplicação EMR Serverless Spark.
    Retorna o dicionário da aplicação (inclui applicationId).
    """
    # Verifica se já existe
    existing = find_application_by_name(emr_client, app_name)
    if existing:
        print(f"  [OK] Aplicação '{app_name}' já existe.")
        print(f"       ID     : {existing['applicationId']}")
        print(f"       Status : {existing['state']}")
        return existing

    # Valida placeholders de VPC
    use_vpc = not any("XXXXXXX" in s for s in subnet_ids)

    network_config = {}
    if use_vpc:
        network_config = {
            "subnetIds": subnet_ids,
            "securityGroupIds": security_group_ids,
        }
        print(f"  [INFO] Usando VPC config: subnets={subnet_ids}")
    else:
        print(
            "  [AVISO] Subnets são placeholders — criando aplicação SEM VPC config.\n"
            "          Para produção, preencha PLACEHOLDER_SUBNET_IDS com subnets reais.\n"
            "          Sem VPC, o acesso S3 será via endpoint público."
        )

    print(f"  [INFO] Criando aplicação EMR Serverless '{app_name}'...")
    print(f"         Release : {EMR_RELEASE}")
    print(f"         Tipo    : SPARK")
    print(f"         AutoStop: {AUTO_STOP_IDLE_TIMEOUT_MINUTES} minutos de idle")

    create_kwargs = {
        "name": app_name,
        "type": "SPARK",
        "releaseLabel": EMR_RELEASE,
        "initialCapacity": INITIAL_CAPACITY,
        "maximumCapacity": MAXIMUM_CAPACITY,
        "autoStartConfiguration": {"enabled": True},
        "autoStopConfiguration": {
            "enabled": True,
            "idleTimeoutMinutes": AUTO_STOP_IDLE_TIMEOUT_MINUTES,
        },
        "tags": {
            "project": "contact-center-data-lakehouse",
            "created_by": "05_setup_emr_serverless.py",
        },
    }

    if network_config:
        create_kwargs["networkConfiguration"] = network_config

    try:
        response = emr_client.create_application(**create_kwargs)
        app_id = response["applicationId"]
        app_arn = response["arn"]
        print(f"  [+] Aplicação criada:")
        print(f"      ID  : {app_id}")
        print(f"      ARN : {app_arn}")

        # Busca detalhes completos
        detail = emr_client.get_application(applicationId=app_id)
        return detail["application"]

    except ClientError as e:
        print(f"  [ERRO] Falha ao criar aplicação EMR: {e}")
        raise


def wait_application_created(emr_client, app_id: str, timeout_minutes: int = 5) -> None:
    """Aguarda a aplicação ficar no estado CREATED ou STARTED."""
    print(f"  [INFO] Aguardando aplicação '{app_id}' ficar disponível...")
    start = time.time()
    while True:
        try:
            response = emr_client.get_application(applicationId=app_id)
            state = response["application"]["state"]
            print(f"    Estado: {state}")

            if state in ("CREATED", "STARTED"):
                print(f"  [OK] Aplicação pronta (estado: {state}).")
                return
            elif state in ("TERMINATED", "TERMINATING"):
                print(f"  [ERRO] Aplicação em estado inesperado: {state}")
                sys.exit(1)

            elapsed = (time.time() - start) / 60
            if elapsed > timeout_minutes:
                print(f"  [AVISO] Timeout ({timeout_minutes}min) aguardando aplicação.")
                return

            time.sleep(10)
        except ClientError as e:
            print(f"  [ERRO] Erro ao verificar aplicação: {e}")
            raise


# ---------------------------------------------------------------------------
# Job Submission
# ---------------------------------------------------------------------------

def submit_spark_job(
    emr_client,
    app_id: str,
    execution_role_arn: str,
    job_name: str,
    script_s3_path: str,
    bucket_name: str,
    region: str,
    script_args: list = None,
    spark_conf: dict = None,
) -> dict:
    """
    Submete um job PySpark para a aplicação EMR Serverless.

    Args:
        emr_client           : cliente boto3 do EMR Serverless
        app_id               : ID da aplicação EMR Serverless
        execution_role_arn   : ARN da role de execução
        job_name             : nome descritivo do job
        script_s3_path       : caminho S3 do script PySpark (.py)
        bucket_name          : bucket para logs e saída
        region               : região AWS
        script_args          : lista de argumentos para o script (opcional)
        spark_conf           : dict com configurações Spark adicionais (opcional)

    Returns:
        dict com jobRunId e arn do job submetido
    """
    log_uri = f"s3://{bucket_name}/logs/emr-serverless/{job_name}/"

    # Configurações Spark padrão — Iceberg + Glue Data Catalog
    default_spark_conf = {
        "spark.executor.cores": "2",
        "spark.executor.memory": "4g",
        "spark.executor.memoryOverhead": "1g",
        "spark.driver.cores": "1",
        "spark.driver.memory": "2g",
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.minExecutors": "1",
        "spark.dynamicAllocation.maxExecutors": "4",
        # Iceberg extensions
        "spark.sql.extensions":
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        # Glue catalog como catálogo Iceberg
        "spark.sql.catalog.glue_catalog":
            "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.glue_catalog.catalog-impl":
            "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.glue_catalog.io-impl":
            "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.glue_catalog.warehouse":
            f"s3://{bucket_name}/",
        # Adaptive Query Execution
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        # Glue Data Catalog como Hive metastore
        "spark.hadoop.hive.metastore.client.factory.class":
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
    }

    if spark_conf:
        default_spark_conf.update(spark_conf)

    job_driver = {
        "sparkSubmit": {
            "entryPoint": script_s3_path,
            "entryPointArguments": script_args or [],
            "sparkSubmitParameters": " ".join(
                f"--conf {k}={v}" for k, v in default_spark_conf.items()
            ),
        }
    }

    monitoring_config = {
        "s3MonitoringConfiguration": {
            "logUri": log_uri,
        },
        "cloudWatchLoggingConfiguration": {
            "enabled": True,
            "logGroupName": f"/aws/emr-serverless/{job_name}",
            "logStreamNamePrefix": "spark",
        },
    }

    print(f"  [INFO] Submetendo job '{job_name}'...")
    print(f"         Script : {script_s3_path}")
    print(f"         Logs   : {log_uri}")

    try:
        response = emr_client.start_job_run(
            applicationId=app_id,
            executionRoleArn=execution_role_arn,
            name=job_name,
            jobDriver=job_driver,
            configurationOverrides={"monitoringConfiguration": monitoring_config},
            tags={
                "project": "contact-center-data-lakehouse",
                "job_name": job_name,
            },
        )
        job_run_id = response["jobRunId"]
        job_arn = response["arn"]
        print(f"  [+] Job submetido com sucesso!")
        print(f"      JobRunId : {job_run_id}")
        print(f"      ARN      : {job_arn}")
        print(f"      Logs     : {log_uri}")
        print(
            f"\n  Para monitorar:\n"
            f"    aws emr-serverless get-job-run \\\n"
            f"      --application-id {app_id} \\\n"
            f"      --job-run-id {job_run_id} \\\n"
            f"      --region {region}"
        )
        return {"jobRunId": job_run_id, "arn": job_arn}
    except ClientError as e:
        print(f"  [ERRO] Falha ao submeter job: {e}")
        raise


def wait_job_completion(
    emr_client,
    app_id: str,
    job_run_id: str,
    timeout_minutes: int = 60,
) -> str:
    """
    Aguarda a conclusão de um job EMR Serverless.
    Retorna o estado final: SUCCESS, FAILED, CANCELLED.
    """
    print(f"\n  [INFO] Aguardando job '{job_run_id}' concluir (timeout: {timeout_minutes}min)...")
    start = time.time()
    last_state = None

    while True:
        try:
            response = emr_client.get_job_run(
                applicationId=app_id,
                jobRunId=job_run_id,
            )
            job_run = response["jobRun"]
            state = job_run["state"]
            state_details = job_run.get("stateDetails", "")

            if state != last_state:
                print(f"    Estado: {state}" + (f" — {state_details}" if state_details else ""))
                last_state = state

            terminal_states = {"SUCCESS", "FAILED", "CANCELLED", "CANCELLING"}
            if state in terminal_states:
                if state == "SUCCESS":
                    print(f"  [OK] Job concluído com SUCESSO!")
                else:
                    print(f"  [ERRO] Job finalizado com estado: {state}")
                    if state_details:
                        print(f"         Detalhes: {state_details}")
                return state

            elapsed = (time.time() - start) / 60
            if elapsed > timeout_minutes:
                print(f"  [AVISO] Timeout ({timeout_minutes}min). Job ainda em execução.")
                return state

            time.sleep(30)
        except ClientError as e:
            print(f"  [ERRO] Erro ao verificar job: {e}")
            raise


def print_cost_estimate(bucket_name: str) -> None:
    """Imprime estimativa de custo por job no EMR Serverless."""
    print("\n" + "=" * 65)
    print("  CUSTO ESTIMADO POR JOB (EMR Serverless - us-east-1)")
    print("=" * 65)
    print("""
  Configuração típica para jobs Bronze->Silver:
  ─────────────────────────────────────────────
  Driver  : 2 vCPU × 4 GB por 10 minutos
  Executor: 2 workers × 4 vCPU × 8 GB por 10 minutos

  Preços EMR Serverless (us-east-1, 2024):
  • vCPU-hora : ~$0.052632
  • GB-hora   : ~$0.005789

  Cálculo por job de 10 minutos:
  • Driver   : 2 vCPU × (10/60)h × $0.052632 = ~$0.018
              + 4 GB × (10/60)h × $0.005789  = ~$0.004
  • Executors: 2 × [4 vCPU × (10/60)h × $0.052632 = ~$0.035
                    + 8 GB × (10/60)h × $0.005789  = ~$0.008]

  TOTAL ESTIMADO POR JOB (~10 min): ~$0.08 - $0.15

  Para 18 tabelas × 1 job/dia = ~$1.44 - $2.70/dia
  Para 22 dias úteis           = ~$32 - $60/mês

  OTIMIZAÇÕES:
  • Use auto-stop (configurado: 15min idle) para não desperdiçar
  • Prefira Parquet + Snappy para reduzir I/O e tempo de execução
  • Use particionamento por data para evitar full table scans
  • Monitore via EMR Serverless Console -> Applications -> Job Runs

  ALTERNATIVA MAIS ECONÔMICA PARA DEV:
  • AWS Glue ETL: cobra por DPU-hora (~$0.44/DPU-hora)
  • Glue tem 1 milhão de DPU-segundos FREE/mês
  • Use EMR Serverless apenas para jobs com volume > 10GB

  Logs de cada job em:
  s3://""" + bucket_name + """/logs/emr-serverless/<job_name>/
""")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Setup EMR Serverless para Contact Center Data Lakehouse"
    )
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--bucket-name", default=DEFAULT_BUCKET)
    parser.add_argument(
        "--subnet-ids",
        nargs="+",
        default=PLACEHOLDER_SUBNET_IDS,
        help="IDs das subnets VPC (privadas com NAT Gateway)",
    )
    parser.add_argument(
        "--security-group-ids",
        nargs="+",
        default=PLACEHOLDER_SECURITY_GROUP_IDS,
        help="IDs dos Security Groups para a aplicação",
    )
    parser.add_argument(
        "--submit-example",
        action="store_true",
        help="Submete um job de exemplo Bronze->Silver após criar a aplicação",
    )
    parser.add_argument(
        "--wait-job",
        action="store_true",
        help="Aguarda a conclusão do job de exemplo (requer --submit-example)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    region = args.region
    bucket_name = args.bucket_name

    print("=" * 65)
    print("  Contact Center Data Lakehouse — Setup EMR Serverless")
    print("=" * 65)
    print(f"  Aplicação  : {APP_NAME}")
    print(f"  Release    : {EMR_RELEASE}")
    print(f"  Região     : {region}")
    print(f"  Bucket     : {bucket_name}")
    print(f"  AutoStop   : {AUTO_STOP_IDLE_TIMEOUT_MINUTES} min idle")
    print("=" * 65)

    iam_client = boto3.client("iam", region_name=region)
    emr_client = boto3.client("emr-serverless", region_name=region)

    print("\n[ETAPA 1] Identificando conta AWS...")
    account_id = get_account_id()
    execution_role_arn = f"arn:aws:iam::{account_id}:role/{EXECUTION_ROLE_NAME}"

    print(f"\n[ETAPA 2] Criando IAM Role '{EXECUTION_ROLE_NAME}'...")
    execution_role_arn = create_emr_execution_role(
        iam_client,
        account_id=account_id,
        region=region,
        bucket_name=bucket_name,
    )

    print(f"\n[ETAPA 3] Criando aplicação EMR Serverless '{APP_NAME}'...")
    application = create_emr_application(
        emr_client,
        app_name=APP_NAME,
        subnet_ids=args.subnet_ids,
        security_group_ids=args.security_group_ids,
        region=region,
    )
    app_id = application["applicationId"]

    print(f"\n[ETAPA 4] Aguardando aplicação ficar pronta...")
    wait_application_created(emr_client, app_id)

    # -----------------------------------------------------------------------
    # Exemplo de submissão de job
    # -----------------------------------------------------------------------
    if args.submit_example:
        print("\n[ETAPA 5] Submetendo job de exemplo Bronze->Silver (chamada)...")

        example_script = (
            f"s3://{bucket_name}/scripts/bronze_to_silver/"
            "job_tb_chamada_bronze_to_silver.py"
        )
        example_args = [
            "--JOB_NAME",    "job-tb-chamada-bronze-to-silver",
            "--BUCKET_NAME", bucket_name,
            "--ENV",         "dev",
        ]

        try:
            job_result = submit_spark_job(
                emr_client,
                app_id=app_id,
                execution_role_arn=execution_role_arn,
                job_name="bronze-to-silver-chamada",
                script_s3_path=example_script,
                bucket_name=bucket_name,
                region=region,
                script_args=example_args,
                spark_conf={
                    "spark.executor.instances": "2",
                    "spark.executor.cores": "2",
                    "spark.executor.memory": "4g",
                },
            )

            if args.wait_job:
                final_state = wait_job_completion(
                    emr_client,
                    app_id=app_id,
                    job_run_id=job_result["jobRunId"],
                    timeout_minutes=30,
                )
                print(f"\n  Estado final do job: {final_state}")

        except ClientError as e:
            print(
                f"\n  [AVISO] Não foi possível submeter o job de exemplo: {e}\n"
                f"  Certifique-se de que o script existe em: {example_script}"
            )
    else:
        print(
            "\n[ETAPA 5] Exemplo de submissão de job (--submit-example não informado)."
        )
        print(
            "  Para submeter um job de exemplo, execute:\n"
            f"    python 05_setup_emr_serverless.py --submit-example\n"
            "\n  Ou use a função submit_spark_job() programaticamente:\n"
            "\n    from infrastructure.05_setup_emr_serverless import submit_spark_job"
            "\n    import boto3"
            f"\n    emr = boto3.client('emr-serverless', region_name='{region}')"
            f"\n    result = submit_spark_job("
            f"\n        emr_client=emr,"
            f"\n        app_id='{app_id}',"
            f"\n        execution_role_arn='{execution_role_arn}',"
            f"\n        job_name='job-tb-chamada-bronze-to-silver',"
            f"\n        script_s3_path='s3://{bucket_name}/scripts/bronze_to_silver/job_tb_chamada_bronze_to_silver.py',"
            f"\n        bucket_name='{bucket_name}',"
            f"\n        region='{region}',"
            f"\n    )"
        )

    # -----------------------------------------------------------------------
    # Estimativa de custo
    # -----------------------------------------------------------------------
    print_cost_estimate(bucket_name)

    # -----------------------------------------------------------------------
    # Resumo Final
    # -----------------------------------------------------------------------
    print("\n" + "=" * 65)
    print("  RESUMO FINAL - EMR Serverless Setup Concluído")
    print("=" * 65)
    print(f"  Conta AWS           : {account_id}")
    print(f"  Região              : {region}")
    print(f"  Aplicação ID        : {app_id}")
    print(f"  Aplicação Nome      : {APP_NAME}")
    print(f"  Release             : {EMR_RELEASE}")
    print(f"  Tipo                : SPARK")
    print(f"  Auto-start          : Habilitado")
    print(f"  Auto-stop idle      : {AUTO_STOP_IDLE_TIMEOUT_MINUTES} minutos")
    print(f"  Capacidade inicial  : 1 Driver + 2 Executors")
    print(f"  Capacidade máxima   : 16 vCPU / 64 GB / 100 GB disk")
    print(f"  Execution Role      : {execution_role_arn}")
    print(f"  Logs dos jobs       : s3://{bucket_name}/logs/emr-serverless/")
    print()
    print("  PROXIMOS PASSOS:")
    print(f"  1. Faça upload dos scripts PySpark:")
    print(f"     aws s3 sync pipeline/ s3://{bucket_name}/scripts/")
    print(f"  2. Submeta jobs com: python 05_setup_emr_serverless.py --submit-example")
    print(f"  3. Monitore em: AWS Console -> EMR Serverless -> {APP_NAME}")
    print()
    print("  PARA DELETAR A APLICAÇÃO (economizar custo):")
    print(
        f"    aws emr-serverless delete-application \\\n"
        f"      --application-id {app_id} \\\n"
        f"      --region {region}"
    )
    print("=" * 65)
    print("  Setup EMR Serverless finalizado com sucesso!")
    print("=" * 65)


if __name__ == "__main__":
    main()
