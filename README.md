# Contact Center Data Lakehouse — AWS

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.3-orange?logo=apachespark)](https://spark.apache.org/)
[![AWS Glue](https://img.shields.io/badge/AWS-Glue%204.0-FF9900?logo=amazonaws)](https://aws.amazon.com/glue/)
[![Apache Iceberg](https://img.shields.io/badge/Apache-Iceberg-4A90D9)](https://iceberg.apache.org/)
[![Athena](https://img.shields.io/badge/AWS-Athena-FF9900?logo=amazonaws)](https://aws.amazon.com/athena/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Pipeline de dados **end-to-end** para Contact Center implementado como um **Data Lakehouse na AWS**. O projeto cobre ingestão via CDC, processamento em camadas com arquitetura Medallion, modelagem dimensional Star Schema e análise analítica com Athena, Redshift Serverless e EMR Serverless.

---

## Sumário

- [Visão Geral](#visão-geral)
- [Arquitetura](#arquitetura)
- [Stack Tecnológica](#stack-tecnológica)
- [Modelo de Dados](#modelo-de-dados)
- [Estrutura do Repositório](#estrutura-do-repositório)
- [Como Executar](#como-executar)
- [Notebooks de Análise](#notebooks-de-análise)
- [KPIs e Métricas](#kpis-e-métricas)
- [Custo AWS Estimado](#custo-aws-estimado)
- [Boas Práticas Implementadas](#boas-práticas-implementadas)
- [Autor](#autor)

---

## Visão Geral

Este projeto implementa um **Data Lakehouse completo** para operações de Contact Center, transformando dados transacionais brutos em inteligência analítica pronta para BI e ML.

**Domínio coberto:**
- Chamadas telefônicas (inbound/outbound)
- Tickets de suporte
- Chat e WhatsApp
- Campanhas de discagem
- Jornada de operadores e qualidade
- Navegação em URA (IVR)

**Resultados:**
- 50+ KPIs operacionais calculados
- Conformidade LGPD com mascaramento de PII
- Pipeline incremental idempotente via Apache Iceberg ACID
- Star Schema com 11 dimensões e 11 tabelas fato
- Custo operacional < $10/mês em escala de demonstração

---

## Arquitetura

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        FONTE DE DADOS                                        │
│                                                                               │
│   ┌──────────────────┐                                                        │
│   │   PostgreSQL      │  WAL (logical replication)                            │
│   │   (18 tabelas)    │─────────────────────────────────────────────────┐    │
│   └──────────────────┘                                                   │    │
└──────────────────────────────────────────────────────────────────────────┼───┘
                                                                           │
                    ┌──────────────────────────────────────────────────────▼───┐
                    │                   INGESTÃO (CDC)                          │
                    │                                                            │
                    │  ┌─────────┐    ┌──────────────┐    ┌────────────────┐   │
                    │  │  AWS    │    │   Kinesis    │    │    Kinesis     │   │
                    │  │  DMS    │───►│   Streams    │───►│   Firehose    │   │
                    │  │(18 tasks│    │ (18 streams) │    │  (Parquet +   │   │
                    │  └─────────┘    └──────────────┘    │   Snappy)     │   │
                    │                                       └───────┬────────┘  │
                    └───────────────────────────────────────────────┼───────────┘
                                                                    │
          ┌─────────────────────────────────────────────────────────▼───────────┐
          │                    CAMADA BRONZE  (S3)                               │
          │                                                                       │
          │   s3://act-cc-dev-lakehouse/bronze/                                  │
          │   ├── operacao/chamada/ano=2024/mes=01/dia=15/                       │
          │   ├── operacao/ticket/...                                             │
          │   ├── cadastro/cliente/...                                            │
          │   └── ... (18 entidades, Parquet + Snappy)                           │
          │                                                                       │
          │   [S3 Event] ──► [EventBridge] ──► [Lambda] ──► [Glue Crawler]      │
          └─────────────────────────────────────────────────────────┬───────────┘
                                                                    │ Glue Workflow
                                                               18 jobs PySpark
                    ┌───────────────────────────────────────────────▼───────────┐
                    │                   CAMADA SILVER  (S3 + Iceberg)            │
                    │                                                             │
                    │   db_silver (Glue Data Catalog)                             │
                    │   ├── Deduplicação CDC (Window Function)                   │
                    │   ├── Mascaramento PII / LGPD                              │
                    │   ├── Validação + Quarentena                               │
                    │   ├── MERGE ACID idempotente (Iceberg)                     │
                    │   └── Watermark incremental (S3 JSON)                      │
                    └─────────────────────────────────────────────────┬──────────┘
                                                                      │
                                                                 22 jobs PySpark
                    ┌─────────────────────────────────────────────────▼──────────┐
                    │                   CAMADA GOLD  (S3 + Iceberg)               │
                    │                                                              │
                    │   db_gold — Star Schema                                      │
                    │   ┌─────────────────┐    ┌──────────────────────────────┐  │
                    │   │   11 Dimensões  │    │       11 Tabelas Fato         │  │
                    │   │                 │    │                               │  │
                    │   │ dim_cliente     │    │ fato_chamada                  │  │
                    │   │ dim_operador    │    │ fato_ticket                   │  │
                    │   │ dim_fila        │    │ fato_chat                     │  │
                    │   │ dim_campanha    │    │ fato_whatsapp                 │  │
                    │   │ dim_canal       │    │ fato_discagem                 │  │
                    │   │ dim_skill       │    │ fato_jornada_operador         │  │
                    │   │ dim_data        │    │ fato_metricas_operacionais    │  │
                    │   │ dim_status_*    │    │ fato_qualidade                │  │
                    │   │ dim_prioridade  │    │ fato_mensagem_chat            │  │
                    │   │ dim_categoria   │    │ fato_interacao_ticket         │  │
                    │   └─────────────────┘    │ fato_ura_navegacao            │  │
                    │                          └──────────────────────────────┘  │
                    └─────────────────────────────────────────────────┬──────────┘
                                                                      │
                    ┌─────────────────────────────────────────────────▼──────────┐
                    │                   CAMADA ANALÍTICA                           │
                    │                                                              │
                    │   ┌──────────────┐  ┌────────────────────┐  ┌──────────┐  │
                    │   │    Athena    │  │ Redshift Serverless │  │   EMR    │  │
                    │   │ (ad-hoc SQL) │  │ (Spectrum + DW)     │  │Serverles │  │
                    │   └──────────────┘  └────────────────────┘  └──────────┘  │
                    │                                                              │
                    │   ┌──────────────────────────────────────────────────────┐  │
                    │   │              Amazon QuickSight (BI)                   │  │
                    │   └──────────────────────────────────────────────────────┘  │
                    └──────────────────────────────────────────────────────────────┘
```

---

## Stack Tecnológica

| Categoria | Tecnologia | Versão | Finalidade |
|---|---|---|---|
| **Linguagem** | Python | 3.9 | Jobs, Lambda, scripts |
| **Processamento** | PySpark | 3.3 | Transformações ETL em escala |
| **Orquestração** | AWS Glue Workflow | 4.0 | Pipeline event-driven com 8 triggers |
| **Formato** | Apache Iceberg | v2 | ACID, MERGE, time travel, schema evolution |
| **Compressão** | Parquet + Snappy | — | Eficiência de armazenamento e leitura |
| **Ingestão CDC** | AWS DMS + Kinesis | — | Captura de mudanças em tempo real |
| **Data Catalog** | Glue Data Catalog | — | Metadados unificados para todas as camadas |
| **Query Engine** | Amazon Athena | — | SQL serverless sobre S3 |
| **Data Warehouse** | Redshift Serverless | — | Analytics com Spectrum sobre Iceberg |
| **Spark Alternativo** | EMR Serverless | — | Jobs Spark sem gerenciamento de cluster |
| **Governança** | AWS Lake Formation | — | Controle de acesso por coluna (PII) |
| **Alertas** | SNS + CloudWatch | — | Monitoramento e falhas de pipeline |
| **Auditoria** | CloudTrail | — | Log de acesso aos dados |

---

## Modelo de Dados

### Diagrama Star Schema (Gold Layer)

```
                        ┌─────────────────┐
                        │   dim_data      │
                        │─────────────────│
                        │ sk_data (PK)    │
                        │ dt_completa     │
                        │ nr_ano          │
                        │ nr_mes          │
                        │ nr_dia          │
                        │ ds_dia_semana   │
                        │ fl_fim_semana   │
                        └────────┬────────┘
                                 │
         ┌───────────────────────┼───────────────────────┐
         │                       │                       │
┌────────▼────────┐   ┌──────────▼──────────┐  ┌────────▼────────┐
│  dim_cliente    │   │    fato_chamada      │  │  dim_operador   │
│─────────────────│   │─────────────────────│  │─────────────────│
│ sk_cliente (PK) │◄──│ sk_chamada (PK)     │  │ sk_operador(PK) │
│ nk_cliente      │   │ nk_chamada          │──►│ nk_operador     │
│ nm_cliente      │   │ sk_cliente (FK)     │  │ nm_operador     │
│ nr_doc_mascarado│   │ sk_operador (FK)    │  │ ds_cargo        │
│ ds_email_masc   │   │ sk_fila (FK)        │  │ fl_supervisor   │
│ fl_cliente_ativo│   │ sk_canal (FK)       │  └─────────────────┘
│ ds_cidade       │   │ sk_status_chamada   │
│ ds_estado       │   │ sk_data_inicio (FK) │  ┌─────────────────┐
└─────────────────┘   │ sk_data_fim (FK)   │  │   dim_fila      │
                       │ nr_duracao_seg     │◄─│─────────────────│
                       │ nr_duracao_min     │  │ sk_fila (PK)    │
                       │ fl_duracao_valida  │  │ nk_fila         │
                       │ fl_chamada_completa│  │ nm_fila         │
                       └─────────────────────┘  │ ds_tipo_fila   │
                                                └─────────────────┘
```

### Camadas Bronze / Silver / Gold

| Camada | Formato | ACID | PII | Partição | Finalidade |
|---|---|---|---|---|---|
| **Bronze** | Parquet + Snappy | Não | Original | ano/mes/dia | Raw, imutável, 90 dias |
| **Silver** | Iceberg v2 | Sim | Mascarado | ano/mes | Limpo, deduplicado, LGPD |
| **Gold** | Iceberg v2 | Sim | Mascarado | sk_data | Star Schema, BI/ML ready |

---

## Estrutura do Repositório

```
contact-center-data-lakehouse/
│
├── README.md                           ← Este arquivo
├── .gitignore
├── requirements.txt
│
├── docs/
│   ├── architecture.md                 ← Decisões técnicas e trade-offs
│   ├── data-model.md                   ← ERD completo + Star Schema
│   ├── aws-setup.md                    ← Passo a passo de provisionamento
│   └── cost-guide.md                   ← Estratégias de custo mínimo
│
├── infrastructure/
│   ├── 01_setup_s3.py                  ← Criação do bucket e estrutura de pastas
│   ├── 02_setup_glue.py                ← Databases, crawlers e jobs no Glue
│   ├── 03_setup_lambda.py              ← Deploy da função Lambda de trigger
│   ├── 04_setup_redshift.py            ← Redshift Serverless + Spectrum
│   ├── 05_setup_emr_serverless.py      ← EMR Serverless application
│   └── iam_policies/
│       ├── glue_execution_role.json
│       └── lambda_execution_role.json
│
├── pipeline/
│   ├── ingestion/
│   │   └── s3_data_loader.py           ← Substitui DMS/Kinesis: carrega CSV→S3 Bronze
│   ├── bronze_to_silver/               ← 18 jobs PySpark (Glue 4.0)
│   │   ├── job_tb_chamada_bronze_to_silver.py
│   │   ├── job_tb_cliente_bronze_to_silver.py
│   │   └── ... (16 outros jobs)
│   └── silver_to_gold/                 ← 22 jobs PySpark (11 dims + 11 fatos)
│       ├── job_dim_cliente_gold.py
│       ├── job_fato_chamada_gold.py
│       └── ... (20 outros jobs)
│
├── lambda/
│   └── fn_start_glue_crawler/
│       └── lambda_function.py          ← Trigger S3 → EventBridge → Crawler
│
├── sql/
│   └── athena_kpis/
│       ├── 01_volume_desempenho_chamadas.sql
│       ├── 02_performance_operadores.sql
│       ├── 03_qualidade_atendimento.sql
│       ├── 04_volume_tickets.sql
│       ├── 05_eficiencia_tickets.sql
│       ├── 06_volume_chat_whatsapp.sql
│       ├── 07_satisfacao_digital.sql
│       ├── 08_desempenho_campanhas.sql
│       ├── 09_roi_campanhas.sql
│       ├── 10_jornada_operador.sql
│       ├── 11_ocupacao_filas.sql
│       └── 12_efetividade_ura.sql
│
├── data/
│   └── synthetic/
│       └── generate_data.py            ← Gera dados realistas para todas as 18 tabelas
│
├── notebooks/
│   ├── 01_exploratory_data_analysis.ipynb
│   ├── 02_kpi_operacional.ipynb
│   ├── 03_performance_operadores.ipynb
│   └── 04_analise_campanhas.ipynb
│
└── tests/
    ├── test_bronze_to_silver.py
    └── test_silver_to_gold.py
```

---

## Como Executar

### Pré-requisitos

- Python 3.9+
- AWS CLI configurado (`aws configure`)
- Conta AWS com permissões em: S3, Glue, Lambda, Athena, Redshift, EMR
- Java 8+ (para PySpark local)

```bash
# Clonar o repositório
git clone https://github.com/ilcileyjunior01/contact-center-data-lakehouse.git
cd contact-center-data-lakehouse

# Instalar dependências
pip install -r requirements.txt
```

### Passo 1 — Provisionar infraestrutura AWS

```bash
# Criar bucket S3 e estrutura de pastas
python infrastructure/01_setup_s3.py

# Criar databases Glue, crawlers e registrar jobs
python infrastructure/02_setup_glue.py

# Deploy da Lambda de trigger
python infrastructure/03_setup_lambda.py

# Configurar Redshift Serverless (opcional)
python infrastructure/04_setup_redshift.py

# Configurar EMR Serverless (opcional)
python infrastructure/05_setup_emr_serverless.py
```

### Passo 2 — Gerar dados sintéticos e carregar Bronze

```bash
# Gerar ~5.000 linhas por tabela (18 tabelas)
python data/synthetic/generate_data.py

# Carregar dados no S3 Bronze (simula DMS + Kinesis)
python pipeline/ingestion/s3_data_loader.py
```

### Passo 3 — Executar pipeline Bronze → Silver → Gold

```bash
# Os crawlers são acionados automaticamente via Lambda + EventBridge
# Os jobs Glue rodam em sequência via Workflow
# Ou acione manualmente via AWS Console / CLI:

aws glue start-job-run --job-name job-tb-chamada-bronze-to-silver
aws glue start-job-run --job-name job-tb-cliente-bronze-to-silver
# ... demais jobs
```

### Passo 4 — Consultar KPIs com Athena

```bash
# Execute as queries SQL em sql/athena_kpis/
# Através do AWS Console > Athena > Editor de Queries
# Workgroup: wg-cc-analytics
```

---

## Notebooks de Análise

| Notebook | Descrição |
|---|---|
| [01 — EDA](notebooks/01_exploratory_data_analysis.ipynb) | Exploração completa das 18 entidades: distribuições, volumes, qualidade dos dados |
| [02 — KPIs Operacionais](notebooks/02_kpi_operacional.ipynb) | TMA, TME, taxa de abandono, SLA, FCR, volume por canal |
| [03 — Performance de Operadores](notebooks/03_performance_operadores.ipynb) | Ranking de operadores, qualidade, produtividade, ausências |
| [04 — Análise de Campanhas](notebooks/04_analise_campanhas.ipynb) | ROI de campanhas, taxa de conversão, desempenho por skill |

---

## KPIs e Métricas

| Área | KPIs |
|---|---|
| **Chamadas** | Volume, TMA, TME, taxa de abandono, calls por operador, SLA |
| **Tickets** | Volume, MTTR, FCR, backlog, distribuição por categoria/prioridade |
| **Chat / WhatsApp** | Volume de sessões, mensagens por sessão, CSAT digital |
| **Campanhas** | Taxa de contato, taxa de conversão, ROI, custo por conversão |
| **Operadores** | Produtividade, qualidade (nota média), jornada, ocupação |
| **Filas** | Nível de ocupação, tempo de espera, distribuição de carga |
| **URA** | Taxa de auto-atendimento, abandono na URA, fluxos mais usados |

---

## Custo AWS Estimado

> Com dados sintéticos pequenos (~5K linhas/tabela) e uso moderado.

| Serviço | Custo/mês estimado |
|---|---|
| S3 (armazenamento + requests) | ~$0.01 |
| Lambda (triggers de crawler) | ~$0.00 |
| Athena (queries KPI em Parquet) | ~$0.05 |
| Glue Jobs (rodada completa do pipeline) | ~$2.50/run |
| Glue Crawlers (18 crawlers × 2 min) | ~$0.50/rodada |
| Redshift Serverless (auto-pause 30 min) | ~$3.00/hora ativa |
| EMR Serverless (jobs pontuais) | ~$0.50/job |
| **Total (uso moderado)** | **< $10/mês** |

**Dicas de economia:**
- Desligue o Redshift Serverless após cada sessão de análise
- Use Parquet + particionamento para reduzir custo no Athena
- Evite DMS + Kinesis em modo demo — use o `s3_data_loader.py` diretamente

---

## Boas Práticas Implementadas

- **Arquitetura Medallion** — separação clara de responsabilidades por camada
- **Idempotência** — todos os jobs usam `MERGE INTO` com hash MD5 para evitar duplicatas em reprocessamentos
- **Incremental** — Glue Job Bookmarks + Watermark JSON para processar apenas dados novos
- **CDC Deduplication** — Window Functions com `partitionBy(natural_key).orderBy(timestamp DESC)`
- **Quarentena** — registros inválidos isolados em path separado com motivo de rejeição
- **LGPD** — mascaramento irreversível de CPF, e-mail e telefone na camada Silver
- **Observabilidade** — logs estruturados JSON no CloudWatch + S3, alertas SNS em falhas
- **Governança** — Lake Formation com controle de acesso por coluna para dados PII
- **Event-driven** — pipeline acionado por eventos S3, não por schedules fixos

---

## Autor

**Ilciley Junior**
GitHub: [@ilcileyjunior01](https://github.com/ilcileyjunior01)

Engenheiro de Dados especializado em pipelines AWS, arquitetura Lakehouse e processamento em escala com PySpark.
