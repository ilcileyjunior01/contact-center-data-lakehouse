"""
03_setup_lambda.py
==================
Contact Center Data Lakehouse - Lambda + EventBridge Setup

Cria a IAM Role, empacota o código da Lambda, cria a função, cria a
EventBridge rule para capturar eventos S3 do bucket bronze/ e vincula
a Lambda como target.

Uso:
    python 03_setup_lambda.py
    python 03_setup_lambda.py --region sa-east-1 --bucket-name act-cc-dev-lakehouse

Requisitos:
    pip install boto3
    Credenciais AWS com permissões em IAM, Lambda e EventBridge
    Arquivo lambda/fn_start_glue_crawler/lambda_function.py deve existir
"""

import argparse
import io
import json
import os
import sys
import time
import zipfile
import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_BUCKET = "act-cc-dev-lakehouse"
DEFAULT_REGION = "sa-east-1"
LAMBDA_NAME = "fn-start-glue-crawler"
LAMBDA_ROLE_NAME = "LambdaGlueCrawlerRole"
EVENTBRIDGE_RULE_NAME = "rule-s3-bronze-to-lambda"

# Path relativo ao diretório raiz do projeto
LAMBDA_SOURCE_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "lambda",
    "fn_start_glue_crawler",
)


# ---------------------------------------------------------------------------
# IAM Role
# ---------------------------------------------------------------------------

def get_account_id() -> str:
    sts = boto3.client("sts")
    identity = sts.get_caller_identity()
    account_id = identity["Account"]
    print(f"  [STS] Account ID: {account_id}")
    return account_id


def create_lambda_role(iam_client, account_id: str, region: str) -> str:
    """
    Cria a IAM role para a Lambda com:
    - Trust policy: lambda.amazonaws.com
    - Inline policy: logs, glue:StartCrawler, glue:GetCrawler
    Retorna o ARN da role.
    """
    role_arn = f"arn:aws:iam::{account_id}:role/{LAMBDA_ROLE_NAME}"

    # Verifica se a role já existe
    try:
        response = iam_client.get_role(RoleName=LAMBDA_ROLE_NAME)
        print(f"  [OK] Role '{LAMBDA_ROLE_NAME}' já existe: {role_arn}")
        return response["Role"]["Arn"]
    except iam_client.exceptions.NoSuchEntityException:
        pass

    # Trust policy
    assume_role_policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )

    print(f"  [INFO] Criando role '{LAMBDA_ROLE_NAME}'...")
    try:
        response = iam_client.create_role(
            RoleName=LAMBDA_ROLE_NAME,
            AssumeRolePolicyDocument=assume_role_policy,
            Description="Role para Lambda fn-start-glue-crawler (Contact Center Lakehouse)",
            Tags=[
                {"Key": "project", "Value": "contact-center-data-lakehouse"},
                {"Key": "created_by", "Value": "03_setup_lambda.py"},
            ],
        )
        role_arn = response["Role"]["Arn"]
        print(f"  [+] Role criada: {role_arn}")
    except ClientError as e:
        print(f"  [ERRO] Falha ao criar role: {e}")
        raise

    # Inline policy
    inline_policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "CloudWatchLogs",
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents",
                    ],
                    "Resource": f"arn:aws:logs:{region}:{account_id}:log-group:/aws/lambda/{LAMBDA_NAME}:*",
                },
                {
                    "Sid": "GlueCrawlerControl",
                    "Effect": "Allow",
                    "Action": [
                        "glue:StartCrawler",
                        "glue:GetCrawler",
                        "glue:GetCrawlerMetrics",
                        "glue:StopCrawler",
                    ],
                    "Resource": f"arn:aws:glue:{region}:{account_id}:crawler/*",
                },
            ],
        }
    )

    try:
        iam_client.put_role_policy(
            RoleName=LAMBDA_ROLE_NAME,
            PolicyName="LambdaGlueCrawlerInlinePolicy",
            PolicyDocument=inline_policy,
        )
        print("  [+] Inline policy anexada à role.")
    except ClientError as e:
        print(f"  [ERRO] Falha ao anexar inline policy: {e}")
        raise

    # Aguarda propagação da role na IAM (pode levar alguns segundos)
    print("  [INFO] Aguardando propagação da role IAM (15s)...")
    time.sleep(15)

    return role_arn


# ---------------------------------------------------------------------------
# Lambda Package
# ---------------------------------------------------------------------------

def zip_lambda_code(source_dir: str) -> bytes:
    """
    Empacota o diretório da Lambda em um ZIP em memória.
    Retorna os bytes do ZIP.
    """
    source_dir = os.path.abspath(source_dir)

    if not os.path.isdir(source_dir):
        print(f"  [ERRO] Diretório da Lambda não encontrado: {source_dir}")
        sys.exit(1)

    print(f"  [INFO] Empacotando Lambda de: {source_dir}")

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(source_dir):
            # Exclui __pycache__ e .pyc
            dirs[:] = [d for d in dirs if d != "__pycache__"]
            for file in files:
                if file.endswith(".pyc"):
                    continue
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, source_dir)
                zf.write(file_path, arcname)
                print(f"    [ZIP] Adicionado: {arcname}")

    zip_bytes = zip_buffer.getvalue()
    print(f"  [OK] ZIP gerado: {len(zip_bytes):,} bytes")
    return zip_bytes


# ---------------------------------------------------------------------------
# Lambda Function
# ---------------------------------------------------------------------------

def create_or_update_lambda(
    lambda_client,
    role_arn: str,
    zip_bytes: bytes,
    region: str,
    bucket_name: str,
) -> str:
    """
    Cria ou atualiza a função Lambda.
    Retorna o ARN da função.
    """
    env_vars = {
        "REGION": region,
        "BUCKET_NAME": bucket_name,
        "LOG_LEVEL": "INFO",
    }

    # Verifica se a função já existe
    try:
        response = lambda_client.get_function(FunctionName=LAMBDA_NAME)
        function_arn = response["Configuration"]["FunctionArn"]
        print(f"  [OK] Lambda '{LAMBDA_NAME}' já existe — atualizando código...")

        lambda_client.update_function_code(
            FunctionName=LAMBDA_NAME,
            ZipFile=zip_bytes,
        )
        # Aguarda update concluir
        waiter = lambda_client.get_waiter("function_updated")
        waiter.wait(FunctionName=LAMBDA_NAME)

        lambda_client.update_function_configuration(
            FunctionName=LAMBDA_NAME,
            Environment={"Variables": env_vars},
            Timeout=30,
            MemorySize=128,
        )
        print(f"  [OK] Lambda código e configuração atualizados.")
        return function_arn

    except lambda_client.exceptions.ResourceNotFoundException:
        pass

    print(f"  [INFO] Criando Lambda '{LAMBDA_NAME}'...")
    try:
        response = lambda_client.create_function(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.9",
            Role=role_arn,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zip_bytes},
            Description=(
                "Inicia o Glue Crawler correspondente quando um arquivo "
                "é depositado em bronze/ no bucket S3 do Contact Center Lakehouse."
            ),
            Timeout=30,
            MemorySize=128,
            Environment={"Variables": env_vars},
            Tags={
                "project": "contact-center-data-lakehouse",
                "created_by": "03_setup_lambda.py",
            },
            Architectures=["x86_64"],
        )
        function_arn = response["FunctionArn"]
        print(f"  [+] Lambda criada: {function_arn}")

        # Aguarda função ficar ativa
        print("  [INFO] Aguardando Lambda ficar ativa...")
        waiter = lambda_client.get_waiter("function_active")
        waiter.wait(FunctionName=LAMBDA_NAME)
        print("  [OK] Lambda ativa.")

        return function_arn

    except ClientError as e:
        print(f"  [ERRO] Falha ao criar Lambda: {e}")
        raise


# ---------------------------------------------------------------------------
# EventBridge Rule
# ---------------------------------------------------------------------------

def create_eventbridge_rule(
    events_client,
    bucket_name: str,
    account_id: str,
    region: str,
) -> str:
    """
    Cria a EventBridge rule para capturar eventos S3 PutObject
    do bucket com prefix bronze/.
    Retorna o ARN da rule.
    """
    # Event pattern para S3 Object Created no bucket com prefix bronze/
    event_pattern = json.dumps(
        {
            "source": ["aws.s3"],
            "detail-type": ["Object Created"],
            "detail": {
                "bucket": {"name": [bucket_name]},
                "object": {"key": [{"prefix": "bronze/"}]},
            },
        }
    )

    print(f"  [INFO] Criando/atualizando EventBridge rule '{EVENTBRIDGE_RULE_NAME}'...")
    try:
        response = events_client.put_rule(
            Name=EVENTBRIDGE_RULE_NAME,
            EventPattern=event_pattern,
            State="ENABLED",
            Description=(
                f"Captura S3 PutObject em bronze/ do bucket {bucket_name} "
                "e dispara a Lambda fn-start-glue-crawler."
            ),
            Tags=[
                {"Key": "project", "Value": "contact-center-data-lakehouse"},
                {"Key": "created_by", "Value": "03_setup_lambda.py"},
            ],
        )
        rule_arn = response["RuleArn"]
        print(f"  [+] EventBridge rule: {rule_arn}")
        return rule_arn
    except ClientError as e:
        print(f"  [ERRO] Falha ao criar EventBridge rule: {e}")
        raise


def add_lambda_as_target(
    events_client,
    lambda_client,
    rule_name: str,
    function_arn: str,
    account_id: str,
    region: str,
) -> None:
    """
    Adiciona a Lambda como target da EventBridge rule e concede
    permissão de invocação ao EventBridge.
    """
    print(f"  [INFO] Adicionando Lambda como target da rule '{rule_name}'...")
    try:
        events_client.put_targets(
            Rule=rule_name,
            Targets=[
                {
                    "Id": "TargetLambdaGlueCrawler",
                    "Arn": function_arn,
                }
            ],
        )
        print(f"  [+] Lambda adicionada como target da rule.")
    except ClientError as e:
        print(f"  [ERRO] Falha ao adicionar target: {e}")
        raise

    # Concede permissão de invoke ao EventBridge
    statement_id = "AllowEventBridgeInvoke"
    print(f"  [INFO] Concedendo permissão de invoke ao EventBridge...")
    try:
        lambda_client.add_permission(
            FunctionName=LAMBDA_NAME,
            StatementId=statement_id,
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{region}:{account_id}:rule/{rule_name}",
        )
        print(f"  [+] Permissão concedida: EventBridge → Lambda.")
    except lambda_client.exceptions.ResourceConflictException:
        print(f"  [OK] Permissão '{statement_id}' já existia — pulando.")
    except ClientError as e:
        print(f"  [ERRO] Falha ao conceder permissão: {e}")
        raise


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Setup Lambda + EventBridge para Contact Center Data Lakehouse"
    )
    parser.add_argument("--bucket-name", default=DEFAULT_BUCKET)
    parser.add_argument("--region", default=DEFAULT_REGION)
    return parser.parse_args()


def main():
    args = parse_args()
    bucket_name = args.bucket_name
    region = args.region

    print("=" * 65)
    print("  Contact Center Data Lakehouse — Setup Lambda + EventBridge")
    print("=" * 65)
    print(f"  Bucket       : {bucket_name}")
    print(f"  Região       : {region}")
    print(f"  Lambda       : {LAMBDA_NAME}")
    print(f"  Role IAM     : {LAMBDA_ROLE_NAME}")
    print(f"  EB Rule      : {EVENTBRIDGE_RULE_NAME}")
    print("=" * 65)

    iam_client = boto3.client("iam", region_name=region)
    lambda_client = boto3.client("lambda", region_name=region)
    events_client = boto3.client("events", region_name=region)

    print("\n[ETAPA 1] Identificando conta AWS...")
    account_id = get_account_id()

    print("\n[ETAPA 2] Criando IAM Role para Lambda...")
    role_arn = create_lambda_role(iam_client, account_id, region)

    print("\n[ETAPA 3] Empacotando código da Lambda...")
    zip_bytes = zip_lambda_code(LAMBDA_SOURCE_DIR)

    print("\n[ETAPA 4] Criando/atualizando função Lambda...")
    function_arn = create_or_update_lambda(
        lambda_client,
        role_arn=role_arn,
        zip_bytes=zip_bytes,
        region=region,
        bucket_name=bucket_name,
    )

    print("\n[ETAPA 5] Criando EventBridge rule...")
    rule_arn = create_eventbridge_rule(
        events_client,
        bucket_name=bucket_name,
        account_id=account_id,
        region=region,
    )

    print("\n[ETAPA 6] Configurando Lambda como target do EventBridge...")
    add_lambda_as_target(
        events_client,
        lambda_client,
        rule_name=EVENTBRIDGE_RULE_NAME,
        function_arn=function_arn,
        account_id=account_id,
        region=region,
    )

    # -----------------------------------------------------------------------
    # Resumo Final
    # -----------------------------------------------------------------------
    print("\n" + "=" * 65)
    print("  RESUMO FINAL - Lambda + EventBridge Setup Concluído")
    print("=" * 65)
    print(f"  Conta AWS       : {account_id}")
    print(f"  Região          : {region}")
    print(f"  Lambda ARN      : {function_arn}")
    print(f"  IAM Role        : arn:aws:iam::{account_id}:role/{LAMBDA_ROLE_NAME}")
    print(f"  EventBridge Rule: {rule_arn}")
    print()
    print("  Fluxo configurado:")
    print(f"    S3 {bucket_name}/bronze/**")
    print(f"    → EventBridge (rule: {EVENTBRIDGE_RULE_NAME})")
    print(f"    → Lambda {LAMBDA_NAME}")
    print(f"    → Glue StartCrawler (crawler correspondente à tabela)")
    print()
    print("  Variáveis de ambiente da Lambda:")
    print(f"    REGION      = {region}")
    print(f"    BUCKET_NAME = {bucket_name}")
    print(f"    LOG_LEVEL   = INFO")
    print()
    print("  Logs disponíveis em:")
    print(f"    CloudWatch Logs → /aws/lambda/{LAMBDA_NAME}")
    print("=" * 65)
    print("  Setup Lambda finalizado com sucesso!")
    print("=" * 65)


if __name__ == "__main__":
    main()
