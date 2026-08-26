# Guia de Custo Mínimo — AWS

## Para quem não é da área de tecnologia

> Manter um projeto na nuvem (AWS) tem custo, assim como pagar uma conta de luz. Este guia explica as estratégias usadas para manter esse custo o mais baixo possível durante o desenvolvimento — sem desligar nada que seja necessário para demonstrar o projeto.
>
> **Resultado: o projeto fica ativo na AWS por aproximadamente R$ 3,00/mês** (equivalente a ~$0,57 dólares) quando não está processando dados ativamente.

---

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
python src/ingestion/s3_data_loader.py \
  --bucket act-cc-dev-lakehouse \
  --tabela all \
  --rows 5000
```

O script gera Parquet particionado em `bronze/` com a mesma estrutura que o Firehose produziria.

---

## 2. Redshift Serverless — Auto-Pause Automático

> **Para não-técnicos:** O Redshift é o banco de dados analítico do projeto. Em vez de cobrar uma taxa fixa todo mês (como uma assinatura), ele cobra apenas pelo tempo em que está sendo usado ativamente. Quando fica 30 minutos sem ninguém consultando, ele "hiberna" sozinho e para de cobrar.

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

## 6. S3 — Lifecycle Rules para Eliminar Acúmulo de Arquivos Temporários

> **Para não-técnicos:** O S3 é o "HD virtual" do projeto na nuvem. Sem regras de limpeza automática, arquivos temporários e versões antigas de arquivos continuam ocupando espaço e gerando custo para sempre. As regras abaixo limpam esses arquivos automaticamente.

Para dados sintéticos de demonstração (~500 MB total):
- Armazenamento: 0.5 GB × $0.023/GB = **$0.012/mês**
- Requests de PUT/GET: **< $0.01/mês**

**Total S3: < $0.03/mês**

### Lifecycle Rules aplicadas (via `infrastructure/apply_cost_optimizations.py`)

| Regra | Prefixo | Ação | Motivo |
|---|---|---|---|
| `redshift-staging-expiry` | `redshift-staging/` | Expira em **3 dias** | Arquivos temporários do UNLOAD para Redshift — inúteis após a carga |
| `logs-expiry` | `logs/` | Expira em **14 dias** | Logs de EMR e Glue — necessários apenas para diagnóstico recente |
| `global-noncurrent-version-expiry` | (todos os prefixos) | Versões antigas expiram em **7 dias** | O versionamento S3 está ativo; sem esta regra, cada atualização de arquivo guarda a versão anterior indefinidamente |
| `abort-incomplete-multipart` | (todos os prefixos) | Cancela em **3 dias** | Uploads interrompidos ficam em estado "zumbi" gerando cobrança de storage |

---

## 7. CloudWatch Logs — Retenção Reduzida

> **Para não-técnicos:** O CloudWatch é o sistema de "diário de bordo" que registra o que acontece em cada processo. Quanto mais tempo guardamos esses registros, mais custam. Para desenvolvimento, 7 dias são suficientes.

- Retenção configurada: **7 dias** (reduzida de 14 dias)
- Log groups afetados: `/aws-glue/jobs/error`, `/aws-glue/jobs/logs-v2`, `/aws-glue/jobs/output`
- Economia: ~$0.03/mês vs retenção infinita

---

## Resumo Consolidado

### Modo ocioso (infraestrutura ativa, sem processar dados)

| Serviço | Configuração aplicada | Custo/mês |
|---|---|---|
| S3 | Lifecycle rules eliminam arquivos temporários automaticamente | $0.03 |
| Redshift Serverless | Auto-pause 30 min — cobra zero quando inativo | $0.50 |
| CloudWatch Logs | Retenção 7 dias | $0.02 |
| Lambda, Glue, Athena, EMR | Pay-per-use — $0 quando ociosos | $0.00 |
| **TOTAL ocioso** | | **~$0.57/mês** |

### Modo ativo (2 execuções completas do pipeline por mês)

| Serviço | Uso estimado | Custo/mês |
|---|---|---|
| S3 | 500 MB + requests | $0.03 |
| Lambda | ~100 invocações/mês | $0.00 |
| Athena | ~50 queries/mês | $0.01 |
| Glue Jobs | 2 pipelines completos/mês | $3.20 |
| Glue Crawlers | 2 rodadas completas/mês | $1.00 |
| Redshift Serverless | Carga pontual + queries demo | $0.50 |
| EMR Serverless | 5 jobs/mês | $0.10 |
| CloudWatch Logs | Logs de 40 jobs (retenção 7 dias) | $0.02 |
| **TOTAL ativo** | | **~$4.86/mês** |

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
