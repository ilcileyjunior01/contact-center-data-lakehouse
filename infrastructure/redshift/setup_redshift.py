"""
setup_redshift.py
=================
Provisiona o Redshift Serverless para o Contact Center Data Lakehouse.

Cria:
  - Namespace: cc-lakehouse-namespace
  - Workgroup:  cc-lakehouse-workgroup
  - IAM Role para COPY do S3

Uso:
    python setup_redshift.py --aws-account-id 123456789012
    python setup_redshift.py --dry-run

Pre-requisitos:
    pip install boto3
    AWS credentials configuradas (aws configure ou variaveis de ambiente)
"""

import argparse
import json
import sys
import time

import boto3
from botocore.exceptions import ClientError

# ─── Configuracoes ────────────────────────────────────────────────────────────

NAMESPACE_NAME  = "cc-lakehouse-namespace"
WORKGROUP_NAME  = "cc-lakehouse-workgroup"
DB_NAME         = "dev"
ADMIN_USER      = "admin"
ROLE_NAME       = "RedshiftS3CopyRole-ContactCenter"
BASE_CAPACITY   = 8   # RPUs (minimo Redshift Serverless)

TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "redshift.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }
    ],
}

S3_COPY_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "S3GoldRead",
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:ListBucket"],
            "Resource": [
                "arn:aws:s3:::act-cc-dev-lakehouse",
                "arn:aws:s3:::act-cc-dev-lakehouse/gold/*",
            ],
        },
        {
            "Sid": "GlueCatalogRead",
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:GetTable",
                "glue:GetPartitions",
            ],
            "Resource": "*",
        },
    ],
}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def get_or_create_iam_role(iam, account_id: str, dry_run: bool) -> str:
    """Retorna o ARN da IAM Role de COPY S3, criando se nao existir."""
    role_arn = f"arn:aws:iam::{account_id}:role/{ROLE_NAME}"

    try:
        resp = iam.get_role(RoleName=ROLE_NAME)
        print(f"  [OK] IAM Role ja existe: {resp['Role']['Arn']}")
        return resp["Role"]["Arn"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            raise

    print(f"  [+] Criando IAM Role: {ROLE_NAME}")
    if dry_run:
        print("      [DRY-RUN] Pulando criacao real.")
        return role_arn

    resp = iam.create_role(
        RoleName=ROLE_NAME,
        AssumeRolePolicyDocument=json.dumps(TRUST_POLICY),
        Description="Permite Redshift Serverless copiar dados do S3 Gold",
        Tags=[
            {"Key": "Project", "Value": "contact-center-data-lakehouse"},
            {"Key": "ManagedBy", "Value": "setup_redshift.py"},
        ],
    )
    iam.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName="S3CopyPolicy",
        PolicyDocument=json.dumps(S3_COPY_POLICY),
    )
    print(f"  [OK] IAM Role criada: {resp['Role']['Arn']}")
    return resp["Role"]["Arn"]


def ensure_namespace(client, role_arn: str, admin_password: str, dry_run: bool):
    """Cria o namespace se nao existir."""
    try:
        resp = client.get_namespace(namespaceName=NAMESPACE_NAME)
        ns = resp["namespace"]
        print(f"  [OK] Namespace '{NAMESPACE_NAME}' ja existe (status: {ns['status']})")
        return ns
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    print(f"  [+] Criando namespace: {NAMESPACE_NAME}")
    if dry_run:
        print("      [DRY-RUN] Pulando criacao real.")
        return {}

    resp = client.create_namespace(
        namespaceName=NAMESPACE_NAME,
        dbName=DB_NAME,
        adminUsername=ADMIN_USER,
        adminUserPassword=admin_password,
        iamRoles=[role_arn],
        tags=[
            {"key": "Project", "value": "contact-center-data-lakehouse"},
            {"key": "ManagedBy", "value": "setup_redshift.py"},
        ],
    )
    print(f"  [OK] Namespace criado.")
    return resp["namespace"]


def ensure_workgroup(client, dry_run: bool):
    """Cria o workgroup se nao existir."""
    try:
        resp = client.get_workgroup(workgroupName=WORKGROUP_NAME)
        wg = resp["workgroup"]
        print(f"  [OK] Workgroup '{WORKGROUP_NAME}' ja existe (status: {wg['status']})")
        return wg
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    print(f"  [+] Criando workgroup: {WORKGROUP_NAME}")
    if dry_run:
        print("      [DRY-RUN] Pulando criacao real.")
        return {}

    resp = client.create_workgroup(
        workgroupName=WORKGROUP_NAME,
        namespaceName=NAMESPACE_NAME,
        baseCapacity=BASE_CAPACITY,
        publiclyAccessible=False,
        tags=[
            {"key": "Project", "value": "contact-center-data-lakehouse"},
            {"key": "ManagedBy", "value": "setup_redshift.py"},
        ],
    )
    print(f"  [OK] Workgroup criado (pode levar ~5 min para ficar AVAILABLE).")
    return resp["workgroup"]


def wait_workgroup_available(client, timeout_minutes: int = 10):
    """Aguarda o workgroup ficar AVAILABLE."""
    print(f"  [..] Aguardando workgroup AVAILABLE (timeout: {timeout_minutes} min)...")
    deadline = time.time() + timeout_minutes * 60
    while time.time() < deadline:
        resp = client.get_workgroup(workgroupName=WORKGROUP_NAME)
        status = resp["workgroup"]["status"]
        if status == "AVAILABLE":
            endpoint = resp["workgroup"].get("endpoint", {})
            host = endpoint.get("address", "N/A")
            port = endpoint.get("port", 5439)
            print(f"  [OK] Workgroup AVAILABLE!")
            print(f"       Host: {host}")
            print(f"       Port: {port}")
            return resp["workgroup"]
        print(f"       Status: {status} — aguardando...")
        time.sleep(30)
    print("  [WARN] Timeout atingido. Verifique o console AWS.")
    return None


def print_checklist(workgroup, namespace):
    """Exibe checklist final com proximos passos."""
    endpoint = workgroup.get("endpoint", {}) if workgroup else {}
    host = endpoint.get("address", "<seu-endpoint>")
    port = endpoint.get("port", 5439)

    print("\n" + "=" * 60)
    print("  REDSHIFT SERVERLESS — CHECKLIST")
    print("=" * 60)
    print(f"  Namespace : {NAMESPACE_NAME}")
    print(f"  Workgroup : {WORKGROUP_NAME}")
    print(f"  Database  : {DB_NAME}")
    print(f"  Host      : {host}")
    print(f"  Port      : {port}")
    print()
    print("  PROXIMOS PASSOS:")
    print("  1. Execute o DDL:")
    print("     psql -h <host> -U admin -d dev -f infrastructure/redshift/ddl/create_tables.sql")
    print("     psql -h <host> -U admin -d dev -f infrastructure/redshift/ddl/create_views.sql")
    print()
    print("  2. Atualize o .env do Airflow com o host acima")
    print()
    print("  3. Suba o Airflow:")
    print("     cd infrastructure/airflow")
    print("     docker compose up -d")
    print()
    print("  4. Ative a DAG 'cc_pipeline_diario' no Airflow UI (localhost:8080)")
    print("=" * 60)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Provisiona Redshift Serverless para o Contact Center Lakehouse")
    parser.add_argument("--aws-account-id", required=True, help="ID da conta AWS (12 digitos)")
    parser.add_argument("--region", default="us-east-1", help="Regiao AWS (default: us-east-1)")
    parser.add_argument("--admin-password", default=None, help="Senha do admin (ou defina REDSHIFT_ADMIN_PASSWORD)")
    parser.add_argument("--dry-run", action="store_true", help="Simula sem criar recursos reais")
    parser.add_argument("--wait", action="store_true", help="Aguarda workgroup ficar AVAILABLE")
    args = parser.parse_args()

    import os
    admin_password = args.admin_password or os.environ.get("REDSHIFT_ADMIN_PASSWORD", "Admin123!")
    if admin_password == "Admin123!" and not args.dry_run:
        print("[WARN] Usando senha padrao. Defina --admin-password ou REDSHIFT_ADMIN_PASSWORD.")

    print("\n=== Contact Center Data Lakehouse — Redshift Setup ===")
    print(f"  Account  : {args.aws_account_id}")
    print(f"  Region   : {args.region}")
    print(f"  Dry-run  : {args.dry_run}")
    print()

    iam = boto3.client("iam", region_name=args.region)
    rs  = boto3.client("redshift-serverless", region_name=args.region)

    print("[1/3] IAM Role")
    role_arn = get_or_create_iam_role(iam, args.aws_account_id, args.dry_run)

    print("\n[2/3] Namespace")
    namespace = ensure_namespace(rs, role_arn, admin_password, args.dry_run)

    print("\n[3/3] Workgroup")
    workgroup = ensure_workgroup(rs, args.dry_run)

    if args.wait and not args.dry_run:
        workgroup = wait_workgroup_available(rs) or workgroup

    print_checklist(workgroup, namespace)


if __name__ == "__main__":
    main()
