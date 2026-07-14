"""
01_setup_quicksight.py
======================
Contact Center Data Lakehouse - QuickSight Setup via Boto3

Etapas automatizadas:
  1. Cria/valida IAM Role para QuickSight acessar Athena + S3
  2. Cria Athena Data Source no QuickSight
  3. Cria 5 SPICE Datasets (um por página do dashboard)
  4. Imprime checklist de passos manuais (visuais no console)

Datasets criados:
  - ds_chamadas          -> KPI 01 (Volume e Desempenho de Chamadas)
  - ds_operadores        -> KPI 02 + 03 (Performance e Qualidade Operadores)
  - ds_tickets           -> KPI 04 + 05 (Volume e Eficiência Tickets)
  - ds_digital           -> KPI 06 + 07 (Chat e WhatsApp)
  - ds_campanhas_ura     -> KPI 08 + 09 + 12 (Campanhas e URA)

Uso:
    python 01_setup_quicksight.py
    python 01_setup_quicksight.py --aws-account-id 123456789012
    python 01_setup_quicksight.py --region us-east-1 --dry-run

Pré-requisitos:
    pip install boto3
    Credenciais AWS configuradas (aws configure)
    QuickSight habilitado na conta (ver guia: docs/quicksight_guide.md)
    aws quicksight describe-account-settings --aws-account-id <ID> --namespace default

Custo QuickSight:
    - Autor: $18/mês  |  Leitor: $5/mês  |  Free trial: 30 dias
    - SPICE: 10 GB grátis por autor
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
DEFAULT_REGION          = "us-east-1"
DEFAULT_BUCKET          = "act-cc-dev-lakehouse"
DEFAULT_ATHENA_WG       = "primary"
DEFAULT_ATHENA_OUTPUT   = "s3://act-cc-dev-lakehouse/athena-results/"
DEFAULT_GLUE_DB         = "db_gold"
QUICKSIGHT_ROLE_NAME    = "QuickSightServiceRole-ContactCenter"
DATASOURCE_ID           = "athena-contact-center"
DATASOURCE_NAME         = "Contact Center - Athena (db_gold)"
NAMESPACE               = "default"

# ---------------------------------------------------------------------------
# Queries SQL para cada dataset (baseadas nos KPIs existentes)
# ---------------------------------------------------------------------------
DATASETS: list[dict] = [
    {
        "id":   "ds-chamadas",
        "name": "CC - Chamadas Volume e Desempenho",
        "description": "KPI 01 - Volume diário, TMA, taxa atendimento e abandono por canal/fila",
        "sql": """
SELECT
    dd.dt_completa,
    dd.nr_ano,
    dd.nr_mes,
    dd.nr_dia,
    dd.ds_dia_semana,
    dd.fl_fim_de_semana,
    dc.nm_canal                                                           AS ds_canal,
    df.nm_fila                                                            AS ds_fila,
    dsc.ds_status                                                         AS ds_status_chamada,
    COUNT(fc.sk_chamada)                                                  AS nr_total_chamadas,
    SUM(CASE WHEN dsc.ds_status IN ('ATENDIDA','COMPLETADA','TRANSFERIDA') THEN 1 ELSE 0 END)
                                                                          AS nr_chamadas_atendidas,
    SUM(CASE WHEN dsc.ds_status IN ('ABANDONADA','ABANDONADA_FILA') THEN 1 ELSE 0 END)
                                                                          AS nr_chamadas_abandonadas,
    ROUND(AVG(CASE WHEN fc.fl_duracao_valida = 1
                   AND dsc.ds_status IN ('ATENDIDA','COMPLETADA','TRANSFERIDA')
                   THEN CAST(fc.nr_duracao_segundos AS DOUBLE) END), 2)   AS nr_tma_segundos,
    ROUND(AVG(CASE WHEN fc.fl_duracao_valida = 1
                   AND dsc.ds_status IN ('ATENDIDA','COMPLETADA','TRANSFERIDA')
                   THEN CAST(fc.nr_duracao_minutos AS DOUBLE) END), 2)    AS nr_tma_minutos,
    ROUND(100.0 * SUM(CASE WHEN dsc.ds_status IN ('ATENDIDA','COMPLETADA','TRANSFERIDA')
                           THEN 1 ELSE 0 END)
          / NULLIF(COUNT(fc.sk_chamada), 0), 2)                           AS pct_taxa_atendimento,
    ROUND(100.0 * SUM(CASE WHEN dsc.ds_status IN ('ABANDONADA','ABANDONADA_FILA')
                           THEN 1 ELSE 0 END)
          / NULLIF(COUNT(fc.sk_chamada), 0), 2)                           AS pct_taxa_abandono,
    CASE WHEN ROUND(100.0 * SUM(CASE WHEN dsc.ds_status IN ('ATENDIDA','COMPLETADA','TRANSFERIDA')
                                      THEN 1 ELSE 0 END)
                   / NULLIF(COUNT(fc.sk_chamada), 0), 2) >= 95 THEN 'VERDE'
         WHEN ROUND(100.0 * SUM(CASE WHEN dsc.ds_status IN ('ATENDIDA','COMPLETADA','TRANSFERIDA')
                                      THEN 1 ELSE 0 END)
                   / NULLIF(COUNT(fc.sk_chamada), 0), 2) >= 85 THEN 'AMARELO'
         ELSE 'VERMELHO' END                                              AS semaforo_atendimento
FROM db_gold.fato_chamada fc
INNER JOIN db_gold.dim_data dd         ON fc.sk_data_inicio = dd.sk_data
INNER JOIN db_gold.dim_canal dc        ON fc.sk_canal = dc.sk_canal
INNER JOIN db_gold.dim_fila df         ON fc.sk_fila = df.sk_fila
LEFT  JOIN db_gold.dim_status_chamada dsc ON fc.sk_status_chamada = dsc.sk_status
WHERE dd.nr_ano >= 2025
GROUP BY dd.dt_completa, dd.nr_ano, dd.nr_mes, dd.nr_dia,
         dd.ds_dia_semana, dd.fl_fim_de_semana,
         dc.nm_canal, df.nm_fila, dsc.ds_status
""",
    },
    {
        "id":   "ds-operadores",
        "name": "CC - Performance e Qualidade Operadores",
        "description": "KPI 02 + 03 - Ranking operadores, TMA individual, nota qualidade",
        "sql": """
SELECT
    do2.nm_operador,
    do2.ds_equipe,
    do2.ds_faixa_tempo_casa,
    do2.fl_operador_ativo,
    dd.dt_completa,
    dd.nr_ano,
    dd.nr_mes,
    dc.nm_canal                                                           AS ds_canal,
    df.nm_fila                                                            AS ds_fila,
    COUNT(fc.sk_chamada)                                                  AS nr_chamadas,
    ROUND(AVG(CASE WHEN fc.fl_duracao_valida = 1
                   THEN CAST(fc.nr_duracao_segundos AS DOUBLE) END), 2)   AS nr_tma_segundos,
    ROUND(AVG(fq.nr_nota), 2)                                             AS nr_nota_qualidade_media,
    COUNT(DISTINCT fq.sk_avaliacao)                                       AS nr_avaliacoes
FROM db_gold.fato_chamada fc
INNER JOIN db_gold.dim_operador do2    ON fc.sk_operador = do2.sk_operador
INNER JOIN db_gold.dim_data dd         ON fc.sk_data_inicio = dd.sk_data
INNER JOIN db_gold.dim_canal dc        ON fc.sk_canal = dc.sk_canal
INNER JOIN db_gold.dim_fila df         ON fc.sk_fila = df.sk_fila
LEFT  JOIN db_gold.fato_qualidade fq   ON fq.sk_operador_avaliado = fc.sk_operador
                                      AND fq.sk_data = fc.sk_data_inicio
WHERE dd.nr_ano >= 2025
  AND do2.fl_operador_ativo = 1
GROUP BY do2.nm_operador, do2.ds_equipe, do2.ds_faixa_tempo_casa,
         do2.fl_operador_ativo, dd.dt_completa, dd.nr_ano, dd.nr_mes,
         dc.nm_canal, df.nm_fila
""",
    },
    {
        "id":   "ds-tickets",
        "name": "CC - Tickets Volume e Eficiencia",
        "description": "KPI 04 + 05 - Volume de tickets, SLA, tempo resolucao por canal",
        "sql": """
SELECT
    dd_ab.dt_completa                                                     AS dt_abertura,
    dd_ab.nr_ano,
    dd_ab.nr_mes,
    dd_ab.ds_dia_semana,
    dc.nm_canal                                                           AS ds_canal,
    dst.ds_status                                                         AS ds_status_ticket,
    do2.nm_operador,
    do2.ds_equipe,
    COUNT(ft.sk_ticket)                                                   AS nr_total_tickets,
    ROUND(AVG(ft.nr_tempo_resolucao_min), 2)                              AS nr_tma_resolucao_min,
    ROUND(AVG(ft.nr_tempo_resolucao_min) / 60.0, 2)                      AS nr_tma_resolucao_horas,
    SUM(CASE WHEN ft.fl_sla_cumprido = 1 THEN 1 ELSE 0 END)              AS nr_tickets_sla_ok,
    ROUND(100.0 * SUM(CASE WHEN ft.fl_sla_cumprido = 1 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(ft.sk_ticket), 0), 2)                            AS pct_sla_cumprido,
    CASE WHEN ROUND(100.0 * SUM(CASE WHEN ft.fl_sla_cumprido = 1 THEN 1 ELSE 0 END)
                   / NULLIF(COUNT(ft.sk_ticket), 0), 2) >= 90 THEN 'VERDE'
         WHEN ROUND(100.0 * SUM(CASE WHEN ft.fl_sla_cumprido = 1 THEN 1 ELSE 0 END)
                   / NULLIF(COUNT(ft.sk_ticket), 0), 2) >= 75 THEN 'AMARELO'
         ELSE 'VERMELHO' END                                              AS semaforo_sla
FROM db_gold.fato_ticket ft
INNER JOIN db_gold.dim_data dd_ab          ON ft.sk_data_abertura = dd_ab.sk_data
INNER JOIN db_gold.dim_canal dc            ON ft.sk_canal = dc.sk_canal
LEFT  JOIN db_gold.dim_status_ticket dst   ON ft.sk_status_ticket = dst.sk_status
LEFT  JOIN db_gold.dim_operador do2        ON ft.sk_operador_abertura = do2.sk_operador
WHERE dd_ab.nr_ano >= 2025
GROUP BY dd_ab.dt_completa, dd_ab.nr_ano, dd_ab.nr_mes, dd_ab.ds_dia_semana,
         dc.nm_canal, dst.ds_status, do2.nm_operador, do2.ds_equipe
""",
    },
    {
        "id":   "ds-digital",
        "name": "CC - Canais Digitais Chat e WhatsApp",
        "description": "KPI 06 + 07 - Volume e satisfacao nos canais digitais",
        "sql": """
SELECT
    dd.dt_completa,
    dd.nr_ano,
    dd.nr_mes,
    dd.ds_dia_semana,
    'CHAT' AS ds_tipo_digital,
    df.nm_fila                                                            AS ds_fila,
    do2.nm_operador,
    COUNT(fch.sk_chat)                                                    AS nr_total_interacoes,
    ROUND(AVG(fch.nr_duracao_minutos), 2)                                 AS nr_duracao_media_min,
    SUM(CASE WHEN fch.fl_chat_resolvido = 1 THEN 1 ELSE 0 END)           AS nr_resolvidos,
    ROUND(100.0 * SUM(CASE WHEN fch.fl_chat_resolvido = 1 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(fch.sk_chat), 0), 2)                             AS pct_resolucao
FROM db_gold.fato_chat fch
INNER JOIN db_gold.dim_data dd         ON fch.sk_data = dd.sk_data
INNER JOIN db_gold.dim_fila df         ON fch.sk_fila = df.sk_fila
LEFT  JOIN db_gold.dim_operador do2    ON fch.sk_operador = do2.sk_operador
WHERE dd.nr_ano >= 2025
GROUP BY dd.dt_completa, dd.nr_ano, dd.nr_mes, dd.ds_dia_semana,
         df.nm_fila, do2.nm_operador

UNION ALL

SELECT
    dd.dt_completa,
    dd.nr_ano,
    dd.nr_mes,
    dd.ds_dia_semana,
    'WHATSAPP' AS ds_tipo_digital,
    df.nm_fila,
    do2.nm_operador,
    COUNT(fw.sk_whatsapp)                                                 AS nr_total_interacoes,
    ROUND(AVG(fw.nr_duracao_minutos), 2)                                  AS nr_duracao_media_min,
    SUM(CASE WHEN fw.fl_whatsapp_resolvido = 1 THEN 1 ELSE 0 END)        AS nr_resolvidos,
    ROUND(100.0 * SUM(CASE WHEN fw.fl_whatsapp_resolvido = 1 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(fw.sk_whatsapp), 0), 2)                          AS pct_resolucao
FROM db_gold.fato_whatsapp fw
INNER JOIN db_gold.dim_data dd         ON fw.sk_data = dd.sk_data
INNER JOIN db_gold.dim_fila df         ON fw.sk_fila = df.sk_fila
LEFT  JOIN db_gold.dim_operador do2    ON fw.sk_operador = do2.sk_operador
WHERE dd.nr_ano >= 2025
GROUP BY dd.dt_completa, dd.nr_ano, dd.nr_mes, dd.ds_dia_semana,
         df.nm_fila, do2.nm_operador
""",
    },
    {
        "id":   "ds-campanhas-ura",
        "name": "CC - Campanhas Discagem e URA",
        "description": "KPI 08 + 09 + 12 - ROI campanhas, taxa conversao, efetividade URA",
        "sql": """
SELECT
    dd.dt_completa,
    dd.nr_ano,
    dd.nr_mes,
    dd.ds_dia_semana,
    dc.nm_canal                                                           AS ds_canal,
    df.nm_fila                                                            AS ds_fila,
    COUNT(fd.sk_discagem)                                                 AS nr_total_discagens,
    SUM(CASE WHEN fd.fl_discagem_atendida = 1 THEN 1 ELSE 0 END)         AS nr_discagens_atendidas,
    ROUND(100.0 * SUM(CASE WHEN fd.fl_discagem_atendida = 1 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(fd.sk_discagem), 0), 2)                          AS pct_taxa_atendimento_discagem,
    SUM(CASE WHEN fd.fl_conversao = 1 THEN 1 ELSE 0 END)                 AS nr_conversoes,
    ROUND(100.0 * SUM(CASE WHEN fd.fl_conversao = 1 THEN 1 ELSE 0 END)
          / NULLIF(SUM(CASE WHEN fd.fl_discagem_atendida = 1 THEN 1 ELSE 0 END), 0), 2)
                                                                          AS pct_taxa_conversao,
    ROUND(SUM(fd.nr_valor_convertido), 2)                                 AS nr_valor_total_convertido,
    ROUND(AVG(fd.nr_custo_discagem), 4)                                   AS nr_custo_medio_discagem,
    ROUND(SUM(fd.nr_valor_convertido) / NULLIF(SUM(fd.nr_custo_discagem), 0), 2)
                                                                          AS nr_roi_campanha
FROM db_gold.fato_discagem fd
INNER JOIN db_gold.dim_data dd         ON fd.sk_data = dd.sk_data
INNER JOIN db_gold.dim_canal dc        ON fd.sk_canal = dc.sk_canal
INNER JOIN db_gold.dim_fila df         ON fd.sk_fila = df.sk_fila
WHERE dd.nr_ano >= 2025
GROUP BY dd.dt_completa, dd.nr_ano, dd.nr_mes, dd.ds_dia_semana,
         dc.nm_canal, df.nm_fila
""",
    },
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_account_id(session: boto3.Session) -> str:
    sts = session.client("sts")
    return sts.get_caller_identity()["Account"]


def log(msg: str) -> None:
    print(f"[QuickSight] {msg}")


def ok(msg: str) -> None:
    print(f"  ✓ {msg}")


def warn(msg: str) -> None:
    print(f"  ⚠ {msg}")


def err(msg: str) -> None:
    print(f"  ✗ {msg}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Etapa 1 — IAM Role
# ---------------------------------------------------------------------------

def ensure_quicksight_role(iam, dry_run: bool) -> str:
    """Cria (ou valida) a IAM role que o QuickSight usará para acessar Athena/S3."""
    log(f"IAM Role: {QUICKSIGHT_ROLE_NAME}")

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "quicksight.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }

    try:
        role = iam.get_role(RoleName=QUICKSIGHT_ROLE_NAME)
        ok(f"Role já existe: {role['Role']['Arn']}")
        arn = role["Role"]["Arn"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            raise
        if dry_run:
            warn("DRY RUN — role seria criada aqui")
            return f"arn:aws:iam::000000000000:role/{QUICKSIGHT_ROLE_NAME}"
        role = iam.create_role(
            RoleName=QUICKSIGHT_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Role para QuickSight acessar Athena e S3 (Contact Center Lakehouse)",
        )
        arn = role["Role"]["Arn"]
        ok(f"Role criada: {arn}")
        time.sleep(5)  # propagação IAM

    # Anexa a policy inline com permissões de Athena/Glue/S3
    policy_path = (
        __file__.replace("01_setup_quicksight.py", "iam_quicksight_policy.json")
    )
    try:
        with open(policy_path, "r") as f:
            policy_doc = f.read()
    except FileNotFoundError:
        warn(f"Arquivo de policy não encontrado em {policy_path}. Pulando attach.")
        return arn

    if not dry_run:
        iam.put_role_policy(
            RoleName=QUICKSIGHT_ROLE_NAME,
            PolicyName="QuickSightContactCenterPolicy",
            PolicyDocument=policy_doc,
        )
        ok("Policy inline anexada à role")

    return arn


# ---------------------------------------------------------------------------
# Etapa 2 — Data Source (Athena)
# ---------------------------------------------------------------------------

def ensure_datasource(qs, aws_account_id: str, region: str, dry_run: bool) -> None:
    """Cria ou valida o Athena Data Source no QuickSight."""
    log(f"Data Source: {DATASOURCE_ID}")

    try:
        resp = qs.describe_data_source(
            AwsAccountId=aws_account_id,
            DataSourceId=DATASOURCE_ID,
        )
        status = resp["DataSource"]["Status"]
        ok(f"Data source já existe (status: {status})")
        return
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("ResourceNotFoundException", "AccessDeniedException"):
            raise

    if dry_run:
        warn(f"DRY RUN — data source '{DATASOURCE_ID}' seria criado aqui")
        return

    log("Criando Athena data source...")
    qs.create_data_source(
        AwsAccountId=aws_account_id,
        DataSourceId=DATASOURCE_ID,
        Name=DATASOURCE_NAME,
        Type="ATHENA",
        DataSourceParameters={
            "AthenaParameters": {
                "WorkGroup": DEFAULT_ATHENA_WG,
                "RoleArn": f"arn:aws:iam::{aws_account_id}:role/{QUICKSIGHT_ROLE_NAME}",
            }
        },
        Permissions=[
            {
                "Principal": f"arn:aws:quicksight:{region}:{aws_account_id}:user/{NAMESPACE}/admin",
                "Actions": [
                    "quicksight:DescribeDataSource",
                    "quicksight:DescribeDataSourcePermissions",
                    "quicksight:PassDataSource",
                    "quicksight:UpdateDataSource",
                    "quicksight:DeleteDataSource",
                    "quicksight:UpdateDataSourcePermissions",
                ],
            }
        ],
        SslProperties={"DisableSsl": False},
    )

    # Aguarda criação
    for attempt in range(12):
        time.sleep(5)
        resp = qs.describe_data_source(
            AwsAccountId=aws_account_id, DataSourceId=DATASOURCE_ID
        )
        status = resp["DataSource"]["Status"]
        if status == "CREATION_SUCCESSFUL":
            ok(f"Data source criado com sucesso (status: {status})")
            return
        elif "FAILED" in status:
            err(f"Falha na criação do data source: {status}")
            err(str(resp["DataSource"].get("ErrorInfo", "")))
            sys.exit(1)
        log(f"  aguardando... ({attempt+1}/12) status={status}")

    warn("Timeout aguardando data source — verifique no console AWS")


# ---------------------------------------------------------------------------
# Etapa 3 — Datasets SPICE
# ---------------------------------------------------------------------------

def ensure_dataset(
    qs,
    aws_account_id: str,
    region: str,
    ds: dict,
    dry_run: bool,
    user_arn: str,
) -> None:
    """Cria ou valida um dataset SPICE com Custom SQL no QuickSight."""
    ds_id   = ds["id"]
    ds_name = ds["name"]
    sql     = ds["sql"].strip()

    log(f"Dataset: {ds_name}")

    try:
        resp = qs.describe_data_set(
            AwsAccountId=aws_account_id,
            DataSetId=ds_id,
        )
        ok(f"Dataset já existe: {resp['DataSet']['DataSetId']}")
        return
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("ResourceNotFoundException", "AccessDeniedException"):
            raise

    if dry_run:
        warn(f"DRY RUN — dataset '{ds_id}' seria criado aqui")
        return

    datasource_arn = (
        f"arn:aws:quicksight:{region}:{aws_account_id}:datasource/{DATASOURCE_ID}"
    )

    qs.create_data_set(
        AwsAccountId=aws_account_id,
        DataSetId=ds_id,
        Name=ds_name,
        ImportMode="SPICE",
        PhysicalTableMap={
            f"{ds_id}-physical": {
                "CustomSql": {
                    "DataSourceArn": datasource_arn,
                    "Name":          ds_name,
                    "SqlQuery":      sql,
                    "Columns":       [],   # QuickSight infere o schema automaticamente
                }
            }
        },
        LogicalTableMap={
            f"{ds_id}-logical": {
                "Alias":  ds_name,
                "Source": {"PhysicalTableId": f"{ds_id}-physical"},
            }
        },
        Permissions=[
            {
                "Principal": user_arn,
                "Actions": [
                    "quicksight:DescribeDataSet",
                    "quicksight:DescribeDataSetPermissions",
                    "quicksight:PassDataSet",
                    "quicksight:UpdateDataSet",
                    "quicksight:DeleteDataSet",
                    "quicksight:CreateIngestion",
                    "quicksight:ListIngestions",
                    "quicksight:DescribeIngestion",
                    "quicksight:CancelIngestion",
                    "quicksight:UpdateDataSetPermissions",
                ],
            }
        ],
    )
    ok(f"Dataset '{ds_id}' criado — aguardando ingestão SPICE...")

    # Dispara ingestão SPICE
    ingestion_id = f"init-{int(time.time())}"
    qs.create_ingestion(
        DataSetId=ds_id,
        IngestionId=ingestion_id,
        AwsAccountId=aws_account_id,
    )

    for attempt in range(24):   # até 2 min
        time.sleep(5)
        resp = qs.describe_ingestion(
            DataSetId=ds_id,
            IngestionId=ingestion_id,
            AwsAccountId=aws_account_id,
        )
        status = resp["Ingestion"]["IngestionStatus"]
        if status == "COMPLETED":
            rows = resp["Ingestion"].get("RowInfo", {}).get("RowsIngested", "?")
            ok(f"Ingestão concluída — {rows} linhas carregadas no SPICE")
            return
        elif status in ("FAILED", "CANCELLED"):
            err(f"Ingestão '{ds_id}' falhou: {status}")
            err(str(resp["Ingestion"].get("ErrorInfo", "")))
            return
        log(f"  ingestão... ({attempt+1}/24) status={status}")

    warn(f"Timeout aguardando ingestão de '{ds_id}' — verifique no console")


# ---------------------------------------------------------------------------
# Etapa 4 — Checklist pós-setup
# ---------------------------------------------------------------------------

def print_checklist(aws_account_id: str, region: str) -> None:
    console_url = (
        f"https://{region}.quicksight.aws.amazon.com/sn/start"
    )
    print("\n" + "=" * 65)
    print("  SETUP CONCLUIDO — PROXIMOS PASSOS MANUAIS")
    print("=" * 65)
    print(f"""
1. Acesse o QuickSight:
   {console_url}

2. Crie a Analysis 'Contact Center Dashboard':
   Analyses > New analysis > selecione ds-chamadas

3. Adicione os outros 4 datasets à mesma analysis:
   Edit > Add data > ds-operadores, ds-tickets, ds-digital, ds-campanhas-ura

4. Crie 5 páginas (sheets) na analysis:
   Page 1 — Visao Geral Chamadas
     - KPI cards: nr_total_chamadas, pct_taxa_atendimento, nr_tma_minutos
     - Line chart: dt_completa x nr_total_chamadas (cor = ds_canal)
     - Bar chart:  ds_fila x pct_taxa_abandono
     - Filtro: nr_ano, nr_mes

   Page 2 — Performance Operadores
     - Table:       nm_operador x nr_chamadas, nr_tma_segundos, nr_nota_qualidade_media
     - Bar chart:   top 10 operadores por nr_chamadas
     - Scatter:     nr_tma_segundos x nr_nota_qualidade_media

   Page 3 — Tickets SLA
     - KPI cards: nr_total_tickets, pct_sla_cumprido, nr_tma_resolucao_horas
     - Pie chart:  ds_canal (% de tickets)
     - Line chart: dt_abertura x pct_sla_cumprido
     - Filtro: semaforo_sla (VERDE/AMARELO/VERMELHO)

   Page 4 — Canais Digitais
     - Bar chart:  ds_tipo_digital x nr_total_interacoes (Chat vs WhatsApp)
     - Line chart: dt_completa x pct_resolucao (cor = ds_tipo_digital)
     - Table:      nm_operador x nr_total_interacoes, pct_resolucao

   Page 5 — Campanhas e URA
     - KPI cards:  nr_total_discagens, pct_taxa_conversao, nr_roi_campanha
     - Bar chart:  ds_fila x nr_roi_campanha
     - Line chart: dt_completa x pct_taxa_atendimento_discagem

5. Publique como Dashboard:
   Share > Publish dashboard > 'Contact Center - Operational Dashboard'

6. (Opcional) Ative refresh automático do SPICE:
   Datasets > cada dataset > Schedule refresh > Daily 06:00 UTC

7. (Opcional) Configure alertas de threshold:
   Dashboard > ícone de sino > Email alert
   Ex: pct_taxa_atendimento < 85 → alert!
""")
    print("=" * 65)
    print(f"  Account ID: {aws_account_id}  |  Região: {region}")
    print("=" * 65 + "\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Setup do Amazon QuickSight para o Contact Center Data Lakehouse"
    )
    p.add_argument("--aws-account-id", help="ID da conta AWS (detectado automaticamente se omitido)")
    p.add_argument("--region",         default=DEFAULT_REGION, help=f"Região AWS (padrão: {DEFAULT_REGION})")
    p.add_argument("--quicksight-user", default="admin", help="Usuário QuickSight para permissões (padrão: admin)")
    p.add_argument("--dry-run",        action="store_true", help="Simula as operações sem criar recursos")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    session         = boto3.Session(region_name=args.region)
    aws_account_id  = args.aws_account_id or get_account_id(session)
    iam             = session.client("iam")
    qs              = session.client("quicksight", region_name=args.region)
    user_arn        = (
        f"arn:aws:quicksight:{args.region}:{aws_account_id}"
        f":user/{NAMESPACE}/{args.quicksight_user}"
    )

    if args.dry_run:
        print("\n[DRY RUN] Nenhum recurso será criado.\n")

    print(f"\nConta AWS  : {aws_account_id}")
    print(f"Região     : {args.region}")
    print(f"Usuário QS : {args.quicksight_user}")
    print(f"User ARN   : {user_arn}\n")

    # ---- 1. IAM Role ----
    ensure_quicksight_role(iam, args.dry_run)

    # ---- 2. Athena Data Source ----
    ensure_datasource(qs, aws_account_id, args.region, args.dry_run)

    # ---- 3. Datasets SPICE ----
    for ds in DATASETS:
        ensure_dataset(qs, aws_account_id, args.region, ds, args.dry_run, user_arn)

    # ---- 4. Checklist manual ----
    print_checklist(aws_account_id, args.region)


if __name__ == "__main__":
    main()
