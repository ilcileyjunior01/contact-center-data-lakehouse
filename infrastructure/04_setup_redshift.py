"""
04_setup_redshift.py
====================
Contact Center Data Lakehouse - Redshift Serverless Setup

Cria o Namespace e Workgroup do Redshift Serverless, configura o
external schema ext_gold via Redshift Spectrum apontando para o
Glue Data Catalog (db_gold), e imprime orientações de uso e custo.

Uso:
    python 04_setup_redshift.py
    python 04_setup_redshift.py --region us-east-1 --admin-password MinhaSenha123!

IMPORTANTE:
    Antes de executar, preencha as variáveis SUBNET_IDS e SECURITY_GROUP_IDS
    com os valores reais da sua VPC, ou passe via argparse.

Requisitos:
    pip install boto3
    Credenciais AWS com permissões em Redshift Serverless, IAM
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
NAMESPACE_NAME = "ns-cc-lakehouse"
WORKGROUP_NAME = "wg-cc-analytics"
ADMIN_USERNAME = "admin"

# PREENCHA ANTES DE EXECUTAR:
# Você pode descobrir esses valores via:
#   aws ec2 describe-subnets --region us-east-1
#   aws ec2 describe-security-groups --region us-east-1
PLACEHOLDER_SUBNET_IDS = [
    "subnet-XXXXXXXXXXXXXXXXX",   # <-- substitua pelos IDs reais das suas subnets privadas
    "subnet-YYYYYYYYYYYYYYYYY",   # Recomendado: ao menos 2 subnets em AZs diferentes
]
PLACEHOLDER_SECURITY_GROUP_IDS = [
    "sg-XXXXXXXXXXXXXXXXX",       # <-- substitua pelo ID do seu Security Group
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


def namespace_exists(rs_client, namespace_name: str) -> bool:
    """Retorna True se o namespace já existe."""
    try:
        rs_client.get_namespace(namespaceName=namespace_name)
        return True
    except rs_client.exceptions.ResourceNotFoundException:
        return False
    except ClientError as e:
        print(f"  [ERRO] Erro ao verificar namespace: {e}")
        raise


def workgroup_exists(rs_client, workgroup_name: str) -> bool:
    """Retorna True se o workgroup já existe."""
    try:
        rs_client.get_workgroup(workgroupName=workgroup_name)
        return True
    except rs_client.exceptions.ResourceNotFoundException:
        return False
    except ClientError as e:
        print(f"  [ERRO] Erro ao verificar workgroup: {e}")
        raise


def create_namespace(
    rs_client,
    namespace_name: str,
    admin_username: str,
    admin_password: str,
    account_id: str,
    region: str,
) -> dict:
    """
    Cria o Redshift Serverless Namespace.
    O namespace é o container lógico para o banco de dados.
    """
    if namespace_exists(rs_client, namespace_name):
        print(f"  [OK] Namespace '{namespace_name}' já existe — pulando criação.")
        response = rs_client.get_namespace(namespaceName=namespace_name)
        return response["namespace"]

    # IAM role para Spectrum (acesso ao Glue e S3)
    iam_roles = [
        f"arn:aws:iam::{account_id}:role/RedshiftSpectrumRole",
    ]

    print(f"  [INFO] Criando Namespace '{namespace_name}'...")
    try:
        response = rs_client.create_namespace(
            namespaceName=namespace_name,
            adminUsername=admin_username,
            adminUserPassword=admin_password,
            dbName="cc_analytics",
            iamRoles=iam_roles,
            logExports=["useractivitylog", "userlog", "connectionlog"],
            tags=[
                {"key": "project", "value": "contact-center-data-lakehouse"},
                {"key": "created_by", "value": "04_setup_redshift.py"},
            ],
        )
        namespace = response["namespace"]
        print(f"  [+] Namespace criado:")
        print(f"      Name   : {namespace['namespaceName']}")
        print(f"      ARN    : {namespace['namespaceArn']}")
        print(f"      Status : {namespace['status']}")
        return namespace
    except ClientError as e:
        print(f"  [ERRO] Falha ao criar namespace: {e}")
        raise


def create_workgroup(
    rs_client,
    workgroup_name: str,
    namespace_name: str,
    subnet_ids: list,
    security_group_ids: list,
) -> dict:
    """
    Cria o Redshift Serverless Workgroup.
    base_capacity: 8 RPUs (mínimo para us-east-1).
    """
    if workgroup_exists(rs_client, workgroup_name):
        print(f"  [OK] Workgroup '{workgroup_name}' já existe — pulando criação.")
        response = rs_client.get_workgroup(workgroupName=workgroup_name)
        return response["workgroup"]

    # Valida placeholders
    if any("XXXXXXX" in sid for sid in subnet_ids):
        print(
            "\n  [AVISO] Os SUBNET_IDS ainda são placeholders!\n"
            "  Edite o arquivo 04_setup_redshift.py e preencha PLACEHOLDER_SUBNET_IDS\n"
            "  com os IDs reais das suas subnets privadas, ou use --subnet-ids.\n"
        )
        sys.exit(1)

    if any("XXXXXXX" in sg for sg in security_group_ids):
        print(
            "\n  [AVISO] Os SECURITY_GROUP_IDS ainda são placeholders!\n"
            "  Edite o arquivo 04_setup_redshift.py e preencha PLACEHOLDER_SECURITY_GROUP_IDS\n"
            "  com os IDs reais dos seus Security Groups, ou use --security-group-ids.\n"
        )
        sys.exit(1)

    config_parameters = [
        {"parameterKey": "enable_user_activity_logging", "parameterValue": "true"},
        {"parameterKey": "auto_mv",                       "parameterValue": "true"},
        {"parameterKey": "datestyle",                     "parameterValue": "ISO, MDY"},
        {"parameterKey": "search_path",                   "parameterValue": "$user, public"},
    ]

    print(f"  [INFO] Criando Workgroup '{workgroup_name}'...")
    print(f"         RPUs       : 8 (mínimo)")
    print(f"         Subnets    : {subnet_ids}")
    print(f"         Sec Groups : {security_group_ids}")

    try:
        response = rs_client.create_workgroup(
            workgroupName=workgroup_name,
            namespaceName=namespace_name,
            baseCapacity=8,                  # 8 RPUs — mínimo disponível
            subnetIds=subnet_ids,
            securityGroupIds=security_group_ids,
            publiclyAccessible=False,        # Acesso apenas via VPC (segurança)
            enhancedVpcRouting=True,
            configParameters=config_parameters,
            tags=[
                {"key": "project", "value": "contact-center-data-lakehouse"},
                {"key": "created_by", "value": "04_setup_redshift.py"},
            ],
        )
        workgroup = response["workgroup"]
        print(f"  [+] Workgroup criado:")
        print(f"      Name      : {workgroup['workgroupName']}")
        print(f"      ARN       : {workgroup['workgroupArn']}")
        print(f"      Status    : {workgroup['status']}")
        print(f"      RPUs      : {workgroup.get('baseCapacity', 8)}")
        return workgroup
    except ClientError as e:
        print(f"  [ERRO] Falha ao criar workgroup: {e}")
        raise


def wait_workgroup_available(rs_client, workgroup_name: str, timeout_minutes: int = 10) -> None:
    """Aguarda o workgroup ficar no status AVAILABLE."""
    print(f"  [INFO] Aguardando workgroup '{workgroup_name}' ficar AVAILABLE...")
    start = time.time()
    while True:
        try:
            response = rs_client.get_workgroup(workgroupName=workgroup_name)
            status = response["workgroup"]["status"]
            print(f"    Status: {status}")

            if status == "AVAILABLE":
                print(f"  [OK] Workgroup disponível.")
                return
            elif status in ("FAILED", "DELETING"):
                print(f"  [ERRO] Workgroup em status inesperado: {status}")
                sys.exit(1)

            elapsed = (time.time() - start) / 60
            if elapsed > timeout_minutes:
                print(f"  [AVISO] Timeout ({timeout_minutes}min) aguardando workgroup.")
                return

            time.sleep(15)
        except ClientError as e:
            print(f"  [ERRO] Erro ao verificar workgroup: {e}")
            raise


def print_external_schema_sql(account_id: str, region: str) -> None:
    """
    Imprime o SQL para criar o external schema ext_gold no Redshift
    apontando para o Glue Data Catalog (database db_gold).
    Este SQL deve ser executado no Redshift Query Editor v2 ou via psycopg2.
    """
    spectrum_role_arn = f"arn:aws:iam::{account_id}:role/RedshiftSpectrumRole"

    sql = f"""
-- ============================================================
--  EXECUTE NO REDSHIFT QUERY EDITOR V2 OU VIA PSYCOPG2
--  Conecte-se ao banco 'cc_analytics' do workgroup wg-cc-analytics
-- ============================================================

-- 1. Criar external schema apontando para Glue db_gold
CREATE EXTERNAL SCHEMA IF NOT EXISTS ext_gold
FROM DATA CATALOG
DATABASE 'db_gold'
IAM_ROLE '{spectrum_role_arn}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- 2. Verificar tabelas disponíveis via Spectrum
SELECT schemaname, tablename, location, parameters
FROM SVV_EXTERNAL_TABLES
WHERE schemaname = 'ext_gold'
ORDER BY tablename;

-- 3. Exemplo de query via Spectrum (sem carregar dados no Redshift)
SELECT
    chamada_id,
    data_chamada,
    duracao_segundos,
    status_chamada,
    operador_id
FROM ext_gold.fato_chamada
WHERE data_chamada >= CURRENT_DATE - INTERVAL '30 days'
LIMIT 100;

-- 4. Exemplo de query com CTAS (materializar resultado no Redshift)
CREATE TABLE public.relatorio_mensal_chamadas AS
SELECT
    DATE_TRUNC('month', data_chamada) AS mes,
    COUNT(*)                          AS total_chamadas,
    AVG(duracao_segundos)             AS duracao_media,
    SUM(CASE WHEN status = 'resolvido' THEN 1 ELSE 0 END) AS resolvidas
FROM ext_gold.fato_chamada
GROUP BY 1
ORDER BY 1 DESC;

-- ============================================================
--  ORIENTAÇÕES ADICIONAIS
-- ============================================================
-- • A role '{spectrum_role_arn}'
--   deve ter policies:
--     - AmazonS3ReadOnlyAccess (ou acesso ao bucket específico)
--     - AWSGlueConsoleFullAccess (ou GetDatabase/GetTable no Glue)
-- • Para performance: use particionamento por data nas tabelas Gold
-- • Para custo: o Spectrum cobra por TB escaneado (~$5/TB)
--   Prefira tabelas particionadas e formato Parquet com Snappy
"""
    print("\n" + "=" * 65)
    print("  SQL: Criar External Schema ext_gold (Redshift Spectrum)")
    print("=" * 65)
    print(sql)


def print_pause_resume_guide(workgroup_name: str, region: str) -> None:
    """Imprime orientações para pausar/retomar o workgroup e economizar custos."""
    print("=" * 65)
    print("  GUIA: Pausar e Retomar o Workgroup (Economia de Custos)")
    print("=" * 65)
    print("""
  O Redshift Serverless é cobrado pelo RPU-hora enquanto o workgroup
  está ACTIVE. Para economizar em ambientes de dev/teste:

  PAUSAR o workgroup (para de cobrar RPUs):
  ─────────────────────────────────────────
  Via AWS CLI:
    aws redshift-serverless update-workgroup \\
      --workgroup-name """ + workgroup_name + """ \\
      --region """ + region + """ \\
      --base-capacity 8  # já existia, apenas confirma

  Nota: Redshift Serverless NÃO tem pause/resume nativo como
  Redshift provisioned. Para pausar, delete o workgroup e recrie
  quando necessário, OU use auto-pause via CloudWatch Alarm.

  CUSTO ESTIMADO (us-east-1, 2024):
  ──────────────────────────────────
  • 8 RPUs × $0.36/RPU-hora = $2.88/hora
  • 8h/dia × 22 dias úteis  = ~$506/mês
  • Recomendação: use apenas durante horário de trabalho
    e delete o workgroup fora do horário de uso em dev.

  AUTOMAÇÃO (Lambda + CloudWatch Events):
  ────────────────────────────────────────
  Crie uma Lambda que deleta/recria o workgroup:
    - Segunda a Sexta: criar às 08h, deletar às 19h (horário de Brasília)
    - Fins de semana: não criar

  Exemplo de delete via CLI:
    aws redshift-serverless delete-workgroup \\
      --workgroup-name """ + workgroup_name + """ \\
      --region """ + region + """

  Exemplo de recriação: execute novamente 04_setup_redshift.py

  ALTERNATIVA DE CUSTO ZERO PARA DEV:
  ─────────────────────────────────────
  Use apenas o Glue Data Catalog + Athena para queries ad-hoc.
  O Athena cobra $5/TB escaneado e tem free tier de 1TB/mês.
""")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Setup Redshift Serverless para Contact Center Data Lakehouse"
    )
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--bucket-name", default=DEFAULT_BUCKET)
    parser.add_argument(
        "--admin-password",
        default=None,
        help="Senha do admin do Redshift (mín. 8 chars, maiúscula, minúscula, número, especial)",
    )
    parser.add_argument(
        "--subnet-ids",
        nargs="+",
        default=PLACEHOLDER_SUBNET_IDS,
        help="IDs das subnets VPC para o workgroup",
    )
    parser.add_argument(
        "--security-group-ids",
        nargs="+",
        default=PLACEHOLDER_SECURITY_GROUP_IDS,
        help="IDs dos Security Groups para o workgroup",
    )
    parser.add_argument(
        "--skip-workgroup",
        action="store_true",
        help="Pula criação do workgroup (usa apenas namespace e imprime guias)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    region = args.region
    bucket_name = args.bucket_name

    print("=" * 65)
    print("  Contact Center Data Lakehouse — Setup Redshift Serverless")
    print("=" * 65)
    print(f"  Namespace  : {NAMESPACE_NAME}")
    print(f"  Workgroup  : {WORKGROUP_NAME}")
    print(f"  Região     : {region}")
    print(f"  Bucket     : {bucket_name}")
    print("=" * 65)

    # Solicita senha se não fornecida
    admin_password = args.admin_password
    if not admin_password:
        import getpass
        print(
            "\n  [INFO] Informe a senha do admin do Redshift."
            "\n         Requisitos: mín. 8 chars, ao menos 1 maiúscula,"
            "\n         1 minúscula, 1 número e 1 caractere especial."
        )
        admin_password = getpass.getpass("  Admin password: ")
        if not admin_password:
            print("  [ERRO] Senha não pode ser vazia.")
            sys.exit(1)

    rs_client = boto3.client("redshift-serverless", region_name=region)

    print("\n[ETAPA 1] Identificando conta AWS...")
    account_id = get_account_id()

    print(f"\n[ETAPA 2] Criando Namespace '{NAMESPACE_NAME}'...")
    namespace = create_namespace(
        rs_client,
        namespace_name=NAMESPACE_NAME,
        admin_username=ADMIN_USERNAME,
        admin_password=admin_password,
        account_id=account_id,
        region=region,
    )

    if args.skip_workgroup:
        print("\n  [INFO] --skip-workgroup: pulando criação do workgroup.")
    else:
        print(f"\n[ETAPA 3] Criando Workgroup '{WORKGROUP_NAME}'...")
        workgroup = create_workgroup(
            rs_client,
            workgroup_name=WORKGROUP_NAME,
            namespace_name=NAMESPACE_NAME,
            subnet_ids=args.subnet_ids,
            security_group_ids=args.security_group_ids,
        )

        print(f"\n[ETAPA 4] Aguardando Workgroup ficar disponível...")
        wait_workgroup_available(rs_client, WORKGROUP_NAME)

    print("\n[ETAPA 5] SQL para criar External Schema (Redshift Spectrum)...")
    print_external_schema_sql(account_id, region)

    print("\n[ETAPA 6] Guia de custo e pausa do Workgroup...")
    print_pause_resume_guide(WORKGROUP_NAME, region)

    # -----------------------------------------------------------------------
    # Resumo Final
    # -----------------------------------------------------------------------
    print("\n" + "=" * 65)
    print("  RESUMO FINAL - Redshift Serverless Setup Concluído")
    print("=" * 65)
    print(f"  Conta AWS       : {account_id}")
    print(f"  Região          : {region}")
    print(f"  Namespace       : {NAMESPACE_NAME}")
    print(f"  Workgroup       : {WORKGROUP_NAME}")
    print(f"  Base Capacity   : 8 RPUs")
    print(f"  Database        : cc_analytics")
    print(f"  Admin User      : {ADMIN_USERNAME}")
    print(f"  Acesso público  : Desabilitado (VPC only)")
    print(f"  Logs            : useractivitylog, userlog, connectionlog")
    print()
    print("  PROXIMOS PASSOS:")
    print("  1. Conecte via Redshift Query Editor v2 ao workgroup wg-cc-analytics")
    print("  2. Execute o SQL do external schema ext_gold (veja acima)")
    print("  3. Crie a role RedshiftSpectrumRole com acesso S3 + Glue")
    print("  4. Valide com: SELECT * FROM SVV_EXTERNAL_TABLES")
    print()
    print("  AVISO DE CUSTO:")
    print("  8 RPUs × ~$0.36/RPU-hora = ~$2.88/hora")
    print("  Pause/delete o workgroup fora do horário de uso em dev!")
    print("=" * 65)
    print("  Setup Redshift finalizado com sucesso!")
    print("=" * 65)


if __name__ == "__main__":
    main()
