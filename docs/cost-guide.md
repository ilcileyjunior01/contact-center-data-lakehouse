# Guia de Custo Mínimo — AWS

## Estratégia para manter custos baixos sem comprometer a demonstração

Este guia explica como executar o projeto completo na AWS com custo inferior a **$10/mês** mesmo com o free tier expirado.

---

## 1. Substituir Kinesis + DMS pelo Loader Local

O maior custo evitável é a ingestão em tempo real com DMS e Kinesis:

| Componente | Custo/mês | Alternativa |
|---|---|---|
| DMS Replication Instance (t3.micro) | ~$13.00 | Script Python |
| Kinesis Streams (1 shard × 18) | ~$10.80 | Script Python |
| Kinesis Firehose | ~$0.50 | Script Python |
| **Total evitado** | **~$24.30** | **$0.00** |

**Como usar:**
```bash
python pipeline/ingestion/s3_data_loader.py \
  --bucket act-cc-dev-lakehouse \
  --tabela all \
  --rows 5000
```

O script gera Parquet particionado em `bronze/` com a mesma estrutura que o Firehose produziria.

---

## 2. Redshift Serverless — Usar com Auto-Pause

O Redshift Serverless cobra apenas quando ativo: **$0.375 × 8 RPUs = $3.00/hora**.

**Configurar auto-pause:**
```python
# infrastructure/04_setup_redshift.py já configura:
# base_capacity = 8  (mínimo)
# max_query_execution_time = 3600
# ConfigParameters: auto_suspend após 30 min de idle
```

**Regra de ouro:** Sempre desligue após a sessão de análise.
```bash
aws redshift-serverless update-workgroup \
  --workgroup-name wg-cc-analytics \
  --config-parameters parameterKey=enable_user_activity_logging,parameterValue=true
```

**Custo estimado com uso de 2h/semana:** ~$3/mês

---

## 3. EMR Serverless — Zero Custo em Idle

O EMR Serverless só cobra durante a execução dos jobs:

| Recurso | Preço |
|---|---|
| vCPU-hora | $0.052624 |
| GB-hora | $0.0057785 |

Para um job de 5 minutos com 4 vCPUs e 8 GB:
- CPU: 4 × (5/60) × $0.052624 = **$0.017**
- Memória: 8 × (5/60) × $0.0057785 = **$0.004**
- **Total: ~$0.021 por execução**

**A aplicação EMR Serverless não tem custo de idle** — só cobra durante execução.

---

## 4. Glue — Minimizar Tempo de Execução

Cada job usa a configuração mínima:
- Workers: **2** (mínimo obrigatório)
- Worker type: **G.1X** (1 DPU = $0.44/DPU-hora)
- Custo por minuto por job: 2 × $0.44/60 = **$0.0147/min**

Para dados de demonstração (5K linhas), cada job roda em ~2-3 minutos:
- Custo por job: ~$0.03-0.04
- 40 jobs completos: ~**$1.20-1.60 por pipeline completo**

**Dica:** Desabilite o Glue Bookmark em jobs que não precisam de estado entre execuções.

---

## 5. Athena — Parquet + Particionamento = Custo Mínimo

O Athena cobra **$5 por TB escaneado**. Com Parquet particionado, as queries escaneiam apenas as partições necessárias.

Exemplo: uma query KPI em dados Silver (100 MB total, query filtra 1 partição = 5 MB):
- Custo: 5 MB / 1.000.000 MB × $5 = **$0.000025 por query**

As 12 queries KPI completas: **< $0.001**

---

## 6. S3 — Custo Negligível

Para dados sintéticos de demonstração (~500 MB total):
- Armazenamento: 0.5 GB × $0.023/GB = **$0.012/mês**
- Requests de PUT/GET: **< $0.01/mês**

**Total S3: < $0.03/mês**

---

## Resumo Consolidado

| Serviço | Uso estimado | Custo/mês |
|---|---|---|
| S3 | 500 MB + requests | $0.03 |
| Lambda | ~100 invocações/mês | $0.00 |
| Athena | ~50 queries/mês | $0.01 |
| Glue Jobs | 2 pipelines completos/mês | $3.20 |
| Glue Crawlers | 2 rodadas completas/mês | $1.00 |
| Redshift Serverless | 2h ativas/semana | $3.00 |
| EMR Serverless | 5 jobs/mês | $0.10 |
| CloudWatch Logs | Logs de 40 jobs | $0.05 |
| **TOTAL** | | **~$7.39/mês** |

---

## Alertas de Custo Recomendados

Configure no AWS Budgets para não ter surpresas:

```bash
# Criar alerta de budget mensal de $15
aws budgets create-budget \
  --account-id $(aws sts get-caller-identity --query Account --output text) \
  --budget '{
    "BudgetName": "contact-center-lakehouse",
    "BudgetLimit": {"Amount": "15", "Unit": "USD"},
    "TimeUnit": "MONTHLY",
    "BudgetType": "COST"
  }' \
  --notifications-with-subscribers '[{
    "Notification": {
      "NotificationType": "ACTUAL",
      "ComparisonOperator": "GREATER_THAN",
      "Threshold": 80
    },
    "Subscribers": [{
      "SubscriptionType": "EMAIL",
      "Address": "seu-email@exemplo.com"
    }]
  }]'
```
