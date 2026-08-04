# Contact Center Data Lakehouse — AWS

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.3-orange?logo=apachespark)](https://spark.apache.org/)
[![AWS Glue](https://img.shields.io/badge/AWS-Glue%204.0-FF9900?logo=amazonaws)](https://aws.amazon.com/glue/)
[![Apache Iceberg](https://img.shields.io/badge/Apache-Iceberg%20v2-4A90D9)](https://iceberg.apache.org/)
[![Athena](https://img.shields.io/badge/AWS-Athena-FF9900?logo=amazonaws)](https://aws.amazon.com/athena/)
[![Airflow](https://img.shields.io/badge/Apache-Airflow%202.9.3-017CEE?logo=apacheairflow)](https://airflow.apache.org/)
[![Redshift](https://img.shields.io/badge/AWS-Redshift%20Serverless-8C4FFF?logo=amazonaws)](https://aws.amazon.com/redshift/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Pipeline de dados **end-to-end** para Contact Center implementado como um **Data Lakehouse na AWS**. O projeto cobre ingestão via CDC, processamento em camadas com arquitetura Medallion (Bronze → Silver → Gold), modelagem dimensional Star Schema, análise analítica com Athena e **carga real no Amazon Redshift Serverless** orquestrada pelo Apache Airflow.

---

## Para quem não é da área de tecnologia

> Se você é recrutador, gestor de negócio ou está conhecendo o projeto pela primeira vez, esta seção explica o que foi construído em linguagem simples.

### O que é este projeto?

Imagine que uma empresa de atendimento ao cliente (call center) recebe milhares de ligações, chats e mensagens de WhatsApp por dia. Esses atendimentos geram uma enorme quantidade de informação: quem ligou, qual operador atendeu, quanto tempo durou, o cliente ficou satisfeito, o problema foi resolvido?

Sem organização, esses dados ficam espalhados em sistemas diferentes e ninguém consegue extrair respostas úteis deles. Este projeto constrói a **infraestrutura que coleta, organiza e transforma esses dados em relatórios e indicadores** — de forma automática, confiável e econômica.

### O que o projeto faz, passo a passo?

```
1. COLETA       Os dados brutos chegam de diversas fontes (sistema de telefonia,
                chat, WhatsApp, tickets de suporte) e são salvos na nuvem (AWS S3),
                como uma grande "caixa de entrada" digital.

2. LIMPEZA      Um processo automático lê esses dados brutos, remove erros,
                elimina duplicatas e protege informações pessoais dos clientes
                (como CPF e e-mail), seguindo a Lei LGPD.

3. ORGANIZAÇÃO  Os dados limpos são reorganizados em um formato ideal para análise:
                tabelas de "quem" (clientes, operadores), "o quê" (chamadas, tickets)
                e "quando" (datas), conectadas entre si — como as peças de um quebra-cabeça.

4. ANÁLISE      Com os dados organizados, é possível responder perguntas como:
                - Qual operador tem o melhor desempenho?
                - Qual horário tem mais chamadas abandonadas?
                - Qual campanha de discagem gerou mais retorno?
                - Quanto tempo em média os tickets levam para ser resolvidos?

5. AUTOMAÇÃO    Todo esse processo acontece sozinho, sem intervenção manual,
                monitorado e com alertas em caso de falha.
```

### Por que isso é valioso para uma empresa?

Antes deste tipo de sistema, essas análises eram feitas manualmente em planilhas Excel, levavam dias para ficar prontas e frequentemente continham erros. Com a infraestrutura construída aqui, os relatórios ficam disponíveis em minutos, são atualizados automaticamente e podem ser acessados por qualquer analista da empresa com um clique.

### Quanto custa rodar isso na nuvem?

O projeto foi construído com foco em economia. Em modo de demonstração (sem processar volumes de produção), o custo mensal na Amazon AWS é de aproximadamente **$0,57 por mês** — menos do que uma xícara de café.

---

## Sumário

- [Para quem não é da área de tecnologia](#para-quem-não-é-da-área-de-tecnologia)
- [Visão Geral](#visão-geral)
- [Arquitetura](#arquitetura)
- [Stack Tecnológica](#stack-tecnológica)
- [Pipeline Bronze → Silver](#pipeline-bronze--silver)
- [Pipeline Silver → Gold](#pipeline-silver--gold)
- [Modelo de Dados Gold](#modelo-de-dados-gold)
- [Redshift Serverless](#redshift-serverless)
- [Airflow — Orquestração](#airflow--orquestração)
- [Padrões de Engenharia](#padrões-de-engenharia)
- [Conformidade LGPD](#conformidade-lgpd)
- [Estrutura do Repositório](#estrutura-do-repositório)
- [Como Executar](#como-executar)
- [KPIs e Queries Athena](#kpis-e-queries-athena)
- [Notebooks de Análise](#notebooks-de-análise)
- [Custo AWS Estimado](#custo-aws-estimado)
- [Autor](#autor)

---

## Visão Geral

Este projeto implementa um **Data Lakehouse completo** para operações de Contact Center, transformando dados transacionais brutos em inteligência analítica pronta para BI e ML.

**Domínios cobertos:**

| Domínio | Entidades |
|---|---|
| Atendimento por voz | Chamadas telefônicas (inbound/outbound), gravações, URA/IVR |
| Atendimento digital | Chat, WhatsApp, mensagens individuais |
| Suporte | Tickets, interações/comentários em tickets |
| Cadastro | Clientes, endereços, operadores, filas, skills |
| Marketing | Campanhas de discagem, eventos de discagem |
| Qualidade | Avaliações de qualidade, jornada de operadores, métricas operacionais |

**Entregáveis:**

- 40 jobs PySpark prontos para deploy no AWS Glue (18 Bronze→Silver + 22 Silver→Gold)
- 18 tabelas Silver Iceberg v2 com CDC, deduplicação, PII mascarado e quarentena
- Star Schema com 11 dimensões + 11 tabelas fato, particionadas e otimizadas para Athena
- 12 arquivos SQL com KPIs prontos para execução no Amazon Athena
- 5 notebooks Jupyter com EDA, KPIs operacionais, performance de operadores, campanhas e canais digitais
- Script boto3 de setup do Amazon QuickSight com 5 datasets SPICE e 5 páginas de dashboard
- **Apache Airflow 2.9.3** (Docker Compose): 2 DAGs orquestrando os 40 Glue jobs + carga Redshift, com suporte a **reprocessamento por camada** (`reprocess_date` + `start_layer`) sem buscar dados na origem
- **Redshift Serverless provisionado e carregado**: 22 tabelas Gold + 9 views analíticas de KPI com dados reais
- Estratégia de carga **Athena UNLOAD → S3 staging → Redshift COPY** (compatível com Iceberg v2)
- GitHub Actions CI/CD: 7 jobs (syntax check, lint, SQL validation, unit tests, JSON validation, DAG validation)
- Terraform IaC: S3, IAM, Glue, Lambda, EventBridge, CloudWatch, Redshift Serverless
- Suite de 240 testes pytest (sintaxe, SQL, transformações, infraestrutura, DAGs)
- Custo operacional < $10/mês em escala de demonstração

---

## Arquitetura

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                             FONTE DE DADOS                                    │
│                                                                               │
│   PostgreSQL (18 tabelas)  →  WAL logical replication                        │
└────────────────────────────────────────────────────────────────┬─────────────┘
                                                                 │
                         ┌───────────────────────────────────────▼─────────────┐
                         │                    INGESTÃO (CDC)                    │
                         │                                                       │
                         │  AWS DMS  →  Kinesis Streams  →  Kinesis Firehose   │
                         │  (18 tasks)   (18 streams)      (Parquet + Snappy)  │
                         └───────────────────────────────────────┬─────────────┘
                                                                 │
          ┌──────────────────────────────────────────────────────▼──────────────┐
          │                       CAMADA BRONZE  (S3)                            │
          │                                                                       │
          │  s3://act-cc-dev-lakehouse/bronze/                                   │
          │  ├── operacao/  (chamada, ticket, chat, whatsapp, ura, discagem,     │
          │  │               metricas_operacionais, gravacao_chamada)             │
          │  ├── cadastro/  (cliente, endereco_cliente, operador,                │
          │  │               skill_operador, fila_atendimento)                   │
          │  ├── qualidade/ (avaliacao_qualidade, jornada_operador,              │
          │  │               interacao_ticket)                                    │
          │  ├── marketing/ (campanha)                                            │
          │  └── suporte/   (mensagem_chat)                                      │
          │                                                                       │
          │  [S3 Event]  →  [EventBridge]  →  [Lambda]  →  [Glue Crawler]       │
          └──────────────────────────────────────────────────────┬───────────────┘
                                                                 │
                                                      18 jobs PySpark (Glue 4.0)
                         ┌───────────────────────────────────────▼─────────────┐
                         │                   CAMADA SILVER  (S3 + Iceberg v2)   │
                         │                                                       │
                         │  db_silver — Glue Data Catalog                       │
                         │                                                       │
                         │  ✓ Deduplicação CDC  (Window partitionBy + orderBy)  │
                         │  ✓ Mascaramento PII / LGPD                           │
                         │  ✓ Validação + Quarentena automática                 │
                         │  ✓ MERGE ACID idempotente (Iceberg)                  │
                         │  ✓ Controle incremental (Watermark JSON + Bookmark)  │
                         └───────────────────────────────────────┬─────────────┘
                                                                 │
                                             22 jobs PySpark (11 dims + 11 fatos)
                         ┌───────────────────────────────────────▼─────────────┐
                         │                   CAMADA GOLD  (S3 + Iceberg v2)     │
                         │                                                       │
                         │  db_gold — Star Schema                                │
                         │                                                       │
                         │  11 Dimensões        11 Tabelas Fato                 │
                         │  ─────────────────   ──────────────────────────────  │
                         │  dim_data            fato_chamada                    │
                         │  dim_cliente         fato_chat                       │
                         │  dim_operador        fato_ticket                     │
                         │  dim_fila            fato_whatsapp                   │
                         │  dim_campanha        fato_discagem                   │
                         │  dim_canal           fato_jornada_operador           │
                         │  dim_skill           fato_metricas_operacionais      │
                         │  dim_status_chamada  fato_qualidade                  │
                         │  dim_status_ticket   fato_mensagem_chat              │
                         │  dim_categoria_ticket fato_interacao_ticket          │
                         │  dim_prioridade_ticket fato_ura_navegacao            │
                         └───────────────────────────────────────┬─────────────┘
                                                                 │
                         ┌───────────────────────────────────────▼─────────────┐
                         │              CARGA REDSHIFT (via Athena UNLOAD)      │
                         │                                                       │
                         │  Athena UNLOAD (Iceberg → CSV)                       │
                         │      → s3://.../redshift-staging/<tabela>/           │
                         │      → Redshift COPY (CSV)                           │
                         │                                                       │
                         │  Orquestrado pela DAG cc_carga_redshift (Airflow)    │
                         └───────────────────────────────────────┬─────────────┘
                                                                 │
                         ┌───────────────────────────────────────▼─────────────┐
                         │                  CAMADA ANALÍTICA                    │
                         │                                                       │
                         │  Amazon Athena        → SQL ad-hoc serverless        │
                         │  Redshift Serverless  → DW com 22 tabelas + 9 views  │
                         │  EMR Serverless       → jobs Spark sem cluster fixo  │
                         │  Amazon QuickSight    → dashboards BI                │
                         └───────────────────────────────────────────────────────┘
```

---

## Stack Tecnológica

| Categoria | Tecnologia | Versão | Finalidade |
|---|---|---|---|
| Linguagem | Python | 3.9+ | Jobs, Lambda, scripts de infraestrutura |
| Processamento | PySpark | 3.3 | Transformações ETL distribuídas |
| Orquestração | Apache Airflow | 2.9.3 | DAGs com GlueJobOperator + TriggerDagRunOperator |
| Orquestração (alt.) | AWS Glue Workflow | 4.0 | Pipeline event-driven com triggers encadeados |
| Formato de tabela | Apache Iceberg | v2 | ACID, MERGE INTO, time travel, schema evolution |
| Formato de arquivo | Parquet + Snappy | — | Compressão e leitura colunar eficiente |
| Ingestão CDC | AWS DMS + Kinesis | — | Captura de mudanças via WAL PostgreSQL |
| Metadados | Glue Data Catalog | — | Catálogo unificado Bronze / Silver / Gold |
| Query engine | Amazon Athena | — | SQL serverless sobre S3/Iceberg; UNLOAD para staging |
| Data Warehouse | Redshift Serverless | — | DW com 22 tabelas + 9 views KPI; auto-pause |
| Spark alternativo | EMR Serverless | — | Jobs Spark sem gerenciamento de cluster |
| Governança PII | AWS Lake Formation | — | Controle de acesso por coluna |
| Monitoramento | CloudWatch + SNS | — | Logs estruturados, alertas de falha |
| Auditoria | CloudTrail | — | Rastreabilidade de acesso aos dados |

---

## Pipeline Bronze → Silver

18 jobs PySpark (Glue 4.0, G.1X, 2 workers). Cada job segue o mesmo fluxo:

```
Ler watermark JSON (S3)
    → Leitura incremental via Glue Job Bookmarks
    → Deduplicação CDC: Window.partitionBy(pk).orderBy(dt_cdc_evento DESC)
    → Mascaramento PII (CPF, e-mail, telefone, CEP)
    → Campos derivados e flags de negócio
    → Separação: registros válidos vs quarentena
    → MERGE INTO Iceberg: ON pk WHEN MATCHED AND hash <> hash THEN UPDATE
    → Atualizar watermark JSON (S3)
```

### Catálogo de Jobs Bronze → Silver

| Job | Tabela Bronze | Tabela Silver | Principais Transformações |
|---|---|---|---|
| `job_tb_chamada` | tb_chamada | chamada | Cast timestamps, normalização st_chamada, nr_duracao_minutos, fl_duracao_valida, fl_chamada_completa |
| `job_tb_gravacao_chamada` | tb_gravacao_chamada | gravacao_chamada | Cast dt_expiracao, nr_tamanho_mb, fl_tem_gravacao, fl_expirada, nr_dias_para_expirar, remoção ds_url_arquivo |
| `job_tb_ura_navegacao` | tb_ura_navegacao | ura_navegacao | Cast nr_tempo_espera, fl_abandonou_ura, ds_faixa_espera (IMEDIATO/CURTO/MEDIO/LONGO) |
| `job_tb_chat` | tb_chat | chat | Cast timestamps, nr_duracao_minutos, fl_chat_completo, coalesce id_operador -1 |
| `job_tb_mensagem_chat` | tb_mensagem_chat | mensagem_chat | nr_tamanho_chars via len(ds_conteudo), drop ds_conteudo (LGPD), fl_mensagem_cliente, fl_mensagem_operador |
| `job_tb_whatsapp_atendimento` | tb_whatsapp | whatsapp_atendimento | Mascaramento nr_telefone → nr_telefone_mascarado, nr_duracao_minutos, fl_atendimento_completo |
| `job_tb_ticket` | tb_ticket | ticket | Cast timestamps, nr_tempo_resolucao_min, fl_ticket_resolvido, fl_dentro_sla (SLA=480 min) |
| `job_tb_interacao_ticket` | tb_interacao_ticket | interacao_ticket | nr_tamanho_observacao_chars, fl_tem_observacao, drop ds_observacao |
| `job_tb_campanha` | tb_campanha | campanha | nr_duracao_dias, fl_campanha_ativa, fl_campanha_vigente |
| `job_tb_discagem` | tb_discagem | discagem | Mascaramento nr_telefone → nr_telefone_mascarado, fl_discagem_atendida, fl_discagem_nao_atendida |
| `job_tb_cliente` | tb_cliente | cliente | Mascaramento CPF (3+\*\*\*\*\*+2), e-mail (domínio), telefone (4 últimos); remoção PII original; fl_cliente_ativo |
| `job_tb_endereco_cliente` | tb_endereco | endereco_cliente | Normalização upper, mascaramento nr_cep → nr_cep_mascarado (5 primeiros), remoção nr_cep |
| `job_tb_operador` | tb_operador | operador | Mascaramento e-mail e login, nr_dias_casa, ds_faixa_tempo_casa, fl_supervisor, cast boolean |
| `job_tb_skill_operador` | tb_skill_operador | skill_operador | Chave composta (id_skill + id_operador), ds_faixa_nivel (BASICO/INTERMEDIARIO/AVANCADO/ESPECIALISTA) |
| `job_tb_jornada_operador` | tb_jornada | jornada_operador | nr_horas_trabalhadas, nr_chamadas_atendidas, nr_tickets_resolvidos, fl_presente |
| `job_tb_fila_atendimento` | tb_fila | fila_atendimento | Cast nr_sla_segundos, nr_sla_minutos |
| `job_tb_avaliacao_qualidade` | tb_avaliacao | avaliacao_qualidade | nr_tamanho_feedback_chars, drop ds_feedback, ds_faixa_nota (CRITICO/INSATISFATORIO/REGULAR/BOM/EXCELENTE), fl_aprovado (>=7), fl_critico (<=4) |
| `job_tb_metricas_operacionais` | tb_metricas | metricas_operacionais | nr_taxa_atendimento, nr_taxa_abandono, nr_tma_minutos, nr_tme_minutos, fl_meta_nivel_servico, fl_alto_abandono |

---

## Pipeline Silver → Gold

22 jobs PySpark (Glue 4.0). Cada job:

```
Ler dimensões/fatos dependentes (spark.table)
    → Resolver surrogate keys via LEFT JOIN com as dimensões
    → Coalesce para sk = -1 quando dimensão não encontrada (registro "DESCONHECIDO")
    → Gerar sk do fato com monotonically_increasing_id() [BIGINT, distribuído, sem OOM]
    → CREATE TABLE IF NOT EXISTS (Iceberg v2, Parquet/Snappy, particionada)
    → MERGE INTO ON nk_* (natural key) — idempotente em reexecuções
```

### Dimensões Gold (11)

| Dimensão | Natural Key | SK | Atributos de Destaque |
|---|---|---|---|
| `dim_data` | dt_completa | sk_data = yyyyMMdd (INT) | nr_ano, nr_mes, nr_dia, nr_trimestre, ds_dia_semana, ds_mes, fl_fim_semana, fl_feriado, fl_dia_util |
| `dim_cliente` | nk_cliente | row_number() | nm_cliente, nr_documento_mascarado, ds_email_mascarado, nr_telefone_mascarado, fl_cliente_ativo, ds_cidade, ds_estado + endereco via LEFT JOIN |
| `dim_operador` | nk_operador | row_number() | nm_operador, ds_email_mascarado, ds_login_mascarado, sk_supervisor (auto-referência), nr_dias_casa, ds_faixa_tempo_casa; gera dim_supervisor como derivada |
| `dim_fila` | nk_fila | row_number() | nm_fila, ds_tipo_canal, nr_sla_segundos, nr_sla_minutos |
| `dim_campanha` | nk_campanha | row_number() | nm_campanha, dt_inicio, dt_fim, st_campanha, nr_duracao_dias, fl_campanha_ativa, fl_campanha_vigente |
| `dim_canal` | sk_canal (fixo) | hardcoded | TELEFONE=1, CHAT=2, WHATSAPP=3, EMAIL=4; fl_digital |
| `dim_skill` | ds_skill + nr_nivel | row_number() | ds_faixa_nivel (BASICO/INTERMEDIARIO/AVANCADO/ESPECIALISTA) |
| `dim_status_chamada` | ds_status | row_number() | Derivada de distinct(st_chamada); fl_chamada_concluida |
| `dim_status_ticket` | ds_status | row_number() | Derivada de distinct(st_ticket); fl_ticket_aberto, fl_ticket_resolvido, fl_ticket_cancelado |
| `dim_categoria_ticket` | nm_categoria | row_number() | Derivada de distinct(ds_categoria) |
| `dim_prioridade_ticket` | nm_prioridade | row_number() | nr_ordem_prioridade (CRITICA=1, ALTA=2, MEDIA=3, BAIXA=4), fl_prioridade_critica |

> Todas as dimensões incluem registro padrão com sk = -1 (`"DESCONHECIDO"`) para integridade referencial quando a chave não é encontrada no JOIN.

### Fatos Gold (11)

| Fato | Entrada Silver | Dimensões | Métricas Principais | Partição |
|---|---|---|---|---|
| `fato_chamada` | chamada | dim_cliente, dim_operador, dim_fila, dim_canal, dim_status_chamada, dim_data (×2) | nr_duracao_segundos, nr_duracao_minutos, fl_duracao_valida, fl_chamada_completa | sk_data_inicio |
| `fato_chat` | chat | dim_cliente, dim_operador, dim_canal (fixo=2), dim_data (×2) | nr_duracao_segundos, nr_duracao_minutos, fl_chat_completo | sk_data_inicio |
| `fato_ticket` | ticket | dim_cliente, dim_operador, dim_status_ticket, dim_categoria_ticket, dim_prioridade_ticket, dim_data (×2) | nr_tempo_resolucao_min, fl_ticket_resolvido, fl_dentro_sla | sk_data_abertura |
| `fato_whatsapp` | whatsapp_atendimento | dim_cliente, dim_operador, dim_canal (fixo=3), dim_data (×2) | nr_duracao_segundos, nr_duracao_minutos, fl_atendimento_completo | sk_data_inicio |
| `fato_discagem` | discagem | dim_campanha, dim_cliente, dim_data | fl_discagem_atendida, fl_discagem_nao_atendida, nr_telefone_mascarado | sk_data |
| `fato_jornada_operador` | jornada_operador | dim_operador, dim_data | nr_horas_trabalhadas, nr_chamadas_atendidas, nr_tickets_resolvidos, fl_presente | sk_data |
| `fato_metricas_operacionais` | metricas_operacionais | dim_fila, dim_data | nr_chamadas_recebidas/atendidas/abandonadas, nr_tma/tme_segundos/minutos, nr_nivel_servico, nr_taxa_atendimento/abandono, fl_meta_nivel_servico, fl_alto_abandono | sk_data |
| `fato_qualidade` | avaliacao_qualidade | fato_chamada (sk_chamada, sk_operador_avaliado), dim_operador (sk_avaliador), dim_data | nr_nota, ds_faixa_nota, fl_aprovado, fl_critico, fl_tem_feedback, nr_tamanho_feedback_chars | sk_data |
| `fato_mensagem_chat` | mensagem_chat | fato_chat (via nk_chat), dim_data | nr_tamanho_chars, fl_mensagem_cliente, fl_mensagem_operador, ds_remetente | sk_data |
| `fato_interacao_ticket` | interacao_ticket | fato_ticket (via nk_ticket), dim_operador, dim_data | nr_tamanho_observacao_chars, fl_tem_observacao, ds_canal | sk_data |
| `fato_ura_navegacao` | ura_navegacao | fato_chamada (via nk_chamada, LEFT), dim_data | ds_opcao_selecionada, nr_duracao_segundos, fl_abandonou_ura, ds_faixa_espera | sk_data |

---

## Modelo de Dados Gold

### Star Schema

```
                         ┌──────────────────────┐
                         │      dim_data         │
                         │──────────────────────│
                         │ sk_data (PK)          │
                         │ dt_completa           │
                         │ nr_ano / nr_mes       │
                         │ ds_dia_semana / ds_mes│
                         │ fl_fim_semana         │
                         │ fl_feriado            │
                         │ fl_dia_util           │
                         └──────────┬───────────┘
                                    │
           ┌────────────────────────┼────────────────────────┐
           │                        │                        │
 ┌─────────▼────────┐  ┌────────────▼─────────────┐  ┌──────▼──────────┐
 │   dim_cliente    │  │       fato_chamada        │  │  dim_operador   │
 │──────────────────│  │──────────────────────────│  │─────────────────│
 │ sk_cliente  (PK) │◄─│ sk_chamada  (PK, BIGINT) │  │ sk_operador(PK) │
 │ nk_cliente       │  │ nk_chamada               │─►│ nk_operador     │
 │ nm_cliente       │  │ sk_cliente  (FK)         │  │ nm_operador     │
 │ nr_doc_mascarado │  │ sk_operador (FK)         │  │ sk_supervisor   │
 │ ds_email_mascarado  │ sk_fila    (FK)          │  └─────────────────┘
 │ fl_cliente_ativo │  │ sk_canal   (FK)          │
 │ ds_cidade        │  │ sk_status_chamada (FK)   │  ┌─────────────────┐
 │ ds_estado        │  │ sk_data_inicio (FK)      │  │    dim_fila     │
 └──────────────────┘  │ sk_data_fim    (FK)      │◄─│─────────────────│
                        │ nr_duracao_segundos      │  │ sk_fila    (PK) │
                        │ nr_duracao_minutos       │  │ nm_fila         │
                        │ fl_duracao_valida        │  │ ds_tipo_canal   │
                        │ fl_chamada_completa      │  │ nr_sla_segundos │
                        └──────────────────────────┘  └─────────────────┘

 ┌──────────────────┐   ┌──────────────────────────┐  ┌─────────────────┐
 │   dim_campanha   │   │      fato_ticket          │  │ dim_status_*    │
 │──────────────────│   │──────────────────────────│  │─────────────────│
 │ sk_campanha (PK) │◄──│ sk_ticket  (PK, BIGINT)  │  │ sk_status  (PK) │
 │ nm_campanha      │   │ sk_cliente  (FK)          │◄─│ ds_status       │
 │ fl_campanha_ativa│   │ sk_operador_abertura (FK) │  │ fl_*            │
 └──────────────────┘   │ sk_status_ticket (FK)    │  └─────────────────┘
                         │ sk_categoria   (FK)      │
                         │ sk_prioridade  (FK)      │  ┌─────────────────┐
                         │ sk_data_abertura (FK)    │  │ dim_prioridade  │
                         │ sk_data_fechamento (FK)  │  │─────────────────│
                         │ nr_tempo_resolucao_min   │  │ sk_prioridade   │
                         │ fl_ticket_resolvido      │◄─│ nm_prioridade   │
                         │ fl_dentro_sla            │  │ nr_ordem        │
                         └──────────────────────────┘  └─────────────────┘
```

### Convenções de Nomenclatura

| Prefixo | Tipo | Exemplo |
|---|---|---|
| `sk_` | Surrogate Key (gerada pelo DW) | `sk_cliente`, `sk_data` |
| `nk_` | Natural Key (vinda do sistema fonte) | `nk_cliente`, `nk_chamada` |
| `id_` | Identificador técnico (Bronze/Silver) | `id_chamada`, `id_operador` |
| `nm_` | Nome / Label descritivo | `nm_operador`, `nm_fila` |
| `ds_` | Descrição textual | `ds_status`, `ds_faixa_nota` |
| `nr_` | Valor numérico / métrica | `nr_duracao_segundos`, `nr_nota` |
| `dt_` | Data ou timestamp | `dt_inicio`, `dt_admissao` |
| `st_` | Status (texto) | `st_chamada`, `st_operador` |
| `fl_` | Flag booleano (SMALLINT 0/1) | `fl_duracao_valida`, `fl_presente` |
| `tp_` | Tipo / categoria | `tp_chamada` |

---

## Redshift Serverless

### Infraestrutura Provisionada

```
Namespace : cc-lakehouse-namespace
Workgroup : cc-lakehouse-workgroup  (8 RPUs, auto-pause 30 min)
Database  : dev
Schema    : gold
Endpoint  : cc-lakehouse-workgroup.642825398802.us-east-1.redshift-serverless.amazonaws.com:5439
```

O workgroup é configurado com `publicly_accessible = false`. Toda a comunicação usa o **Redshift Data API** (boto3 `redshift-data`) via HTTPS, sem necessidade de conexão TCP direta.

### Estratégia de Carga: Athena UNLOAD → S3 Staging → Redshift COPY

As tabelas Gold usam **Apache Iceberg v2**, que armazena metadados (`.metadata.json`, `.avro`) no mesmo prefixo S3 dos dados. O Redshift COPY direto falha ao encontrar esses arquivos. A solução:

```
1. Athena UNLOAD
   SELECT date_format(dt_ingestao_gold, '%Y-%m-%d %H:%i:%S'), ...
   FROM db_gold.<tabela>
   TO 's3://act-cc-dev-lakehouse/redshift-staging/<tabela>/'
   WITH (format='TEXTFILE', field_delimiter=',')

   ↳ Athena lê o Iceberg nativamente via Glue Catalog
   ↳ Converte timestamp(6) with time zone → string (evita incompatibilidade)
   ↳ Gera CSV limpo sem metadados Iceberg

2. Redshift COPY
   COPY gold.<tabela>
   FROM 's3://act-cc-dev-lakehouse/redshift-staging/<tabela>/'
   IAM_ROLE 'arn:aws:iam::...:role/RedshiftS3CopyRole-ContactCenter'
   DELIMITER ',' DATEFORMAT 'auto' TIMEFORMAT 'auto'
   ACCEPTINVCHARS BLANKSASNULL EMPTYASNULL REMOVEQUOTES;
```

### Tabelas e Views no Redshift

**22 tabelas** carregadas no schema `gold` com dados reais:

| Tipo | Tabela | Registros | DISTKEY | SORTKEY |
|---|---|---:|---|---|
| DIM | dim_data | 5.844 | ALL | sk_data |
| DIM | dim_cliente | 4.159 | sk_cliente | nk_cliente |
| DIM | dim_operador | 208 | ALL | nk_operador |
| DIM | dim_fila | 17 | ALL | — |
| DIM | dim_canal | 6 | ALL | — |
| DIM | dim_status_chamada | 5 | ALL | — |
| DIM | dim_status_ticket | 5 | ALL | — |
| DIM | dim_campanha | 25 | ALL | dt_inicio |
| DIM | dim_categoria_ticket | 5 | ALL | — |
| DIM | dim_prioridade_ticket | 5 | ALL | — |
| DIM | dim_skill | 26 | ALL | — |
| FATO | fato_chamada | 4.168 | sk_chamada | sk_data_inicio |
| FATO | fato_ticket | 2.059 | sk_ticket | sk_data_abertura |
| FATO | fato_qualidade | 2.504 | sk_avaliacao | sk_data |
| FATO | fato_discagem | 4.189 | sk_discagem | sk_data |
| FATO | fato_ura_navegacao | 4.126 | sk_ura | sk_data |
| FATO | fato_chat | 2.113 | sk_chat | sk_data_inicio |
| FATO | fato_whatsapp | 1.386 | sk_whatsapp | sk_data_inicio |
| FATO | fato_jornada_operador | 5.985 | sk_jornada | sk_data |
| FATO | fato_metricas_operacionais | 4.187 | sk_metrica | sk_data |
| FATO | fato_interacao_ticket | 2.494 | sk_interacao | sk_data |
| FATO | fato_mensagem_chat | 4.905 | sk_mensagem | sk_data |

**9 views analíticas** validadas com dados reais:

| View | KPI | Linhas |
|---|---|---:|
| `vw_kpi_01_volume_chamadas` | Volume, TMA, taxa de atendimento por mês/status | 144 |
| `vw_kpi_02_performance_operadores` | Ranking de operadores por chamadas e TMA | 208 |
| `vw_kpi_03_qualidade` | Nota média, % aprovados, % críticos por operador/mês | 1.523 |
| `vw_kpi_04_volume_tickets` | Volume por status, categoria e prioridade | 1.350 |
| `vw_kpi_05_eficiencia_tickets` | TRT médio em minutos/horas, % SLA cumprido | 144 |
| `vw_kpi_06_volume_digital` | Atendimentos Chat e WhatsApp por mês | 72 |
| `vw_kpi_08_campanhas` | Taxa de atendimento por campanha | 25 |
| `vw_kpi_09_metricas_fila` | Nível de serviço, TMA, TME, abandono por fila/mês | 611 |
| `vw_kpi_12_ura` | Navegações URA, opções mais usadas, % abandono | 252 |

---

## Airflow — Orquestração

### DAG 1: cc_pipeline_diario

Schedule: `0 2 * * *` (diário às 02:00 UTC). Executa os 40 Glue jobs em 4 waves e ao final aciona a carga no Redshift.

```
inicio
  └─► [gate_bronze] ShortCircuit
        └─► [Wave 1] 18 jobs Bronze→Silver (paralelo)
              └─► fim_bronze
                    └─► [gate_silver] ShortCircuit / ALL_DONE
                          └─► [Wave 2] 11 jobs Silver→Gold Dimensões (paralelo)
                                └─► fim_dims
                                      └─► [gate_gold] ShortCircuit / ALL_DONE
                                            └─► [Wave 3] 7 jobs Fatos base (paralelo)
                                                  └─► fim_wave1
                                                        └─► [Wave 4] 4 jobs Fatos dependentes
                                                              └─► fim_pipeline
                                                                    └─► TriggerDagRunOperator → cc_carga_redshift
```

**Parâmetros da DAG:**

| Parâmetro | Default | Descrição |
|---|---|---|
| `reprocess_date` | `""` (usa `{{ ds }}`) | Data a reprocessar (`YYYY-MM-DD`). Passada como `--REPROCESS_DATE` a todos os Glue jobs. |
| `start_layer` | `"bronze"` | Camada de entrada: `bronze` \| `silver` \| `gold` \| `redshift` |

Os gates `gate_bronze`, `gate_silver` e `gate_gold` são `ShortCircuitOperator` com `trigger_rule=ALL_DONE`: quando uma wave é pulada, os tasks ficam com status SKIPPED, mas o gate seguinte executa normalmente — garantindo que o pipeline sempre chega ao fim e aciona a carga do Redshift.

### DAG 2: cc_carga_redshift

Schedule: `None` (acionada pela DAG 1 ou manualmente). Faz TRUNCATE + COPY das 22 tabelas Gold no Redshift Serverless. Recebe `reprocess_date` via `dag_run.conf` para rastreabilidade nos logs.

```
inicio_carga
  └─► log_reprocess_date
        └─► dims em paralelo (11 tabelas)
              └─► dimensoes_carregadas
                    └─► fatos em paralelo (11 tabelas)
                          └─► carga_redshift_concluida
```

### Reprocessamento por camada

Para reprocessar uma data específica **sem buscar dados na origem**, acione a DAG manualmente com o parâmetro `start_layer` correspondente à camada desejada. Os dados já persistidos no S3 (Bronze, Silver ou Gold) são reutilizados.

| Cenário | Comando |
|---|---|
| Bug na lógica Silver | `start_layer=silver` → reprocessa Silver→Gold→Redshift a partir do Bronze |
| Bug só no Gold | `start_layer=gold` → reprocessa Gold→Redshift a partir do Silver |
| Recarregar só o Redshift | `start_layer=redshift` → só TRUNCATE+COPY a partir do Gold |

```bash
# Via CLI (reprocessar Silver e Gold do dia 15/06/2024)
airflow dags trigger cc_pipeline_diario \
  --conf '{"reprocess_date": "2024-06-15", "start_layer": "silver"}'

# Ou só recarregar o Redshift diretamente
airflow dags trigger cc_carga_redshift \
  --conf '{"reprocess_date": "2024-06-15"}'
```

### Setup local

```bash
cd infrastructure/airflow
cp .env.example .env
# Edite .env com AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, REDSHIFT_PASSWORD

docker compose up airflow-init
docker compose up -d
# Acesse http://localhost:8080  (admin/admin)
```

---

## Padrões de Engenharia

### 1. Idempotência via MERGE + Hash MD5

Todos os jobs usam `hash_registro` (MD5 de todas as colunas de negócio) para garantir que um reprocessamento não sobrescreva dados inalterados:

```sql
MERGE INTO glue_catalog.db_silver.chamada AS target
USING stg_chamada AS source
ON target.id_chamada = source.id_chamada
WHEN MATCHED AND target.hash_registro <> source.hash_registro
    THEN UPDATE SET *
WHEN NOT MATCHED
    THEN INSERT *
```

### 2. Controle Incremental

Dois mecanismos complementares:

- **Glue Job Bookmarks** — rastreia os arquivos S3 já processados; impede releitura de dados antigos
- **Watermark JSON** — armazenado em `s3://{bucket}/checkpoints/{tabela}/watermark.json`; filtra registros por `dt_cdc_evento > last_watermark`

### 3. Deduplicação CDC

Elimina eventos duplicados mantendo sempre o evento mais recente por chave primária:

```python
window_dedup = Window.partitionBy("id_chamada").orderBy(F.col("dt_cdc_evento").desc())
df_dedup = (
    df_cdc
    .withColumn("_row_num", F.row_number().over(window_dedup))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num", "Op", "_timestamp")
)
```

### 4. Quarentena Automática

Registros inválidos (chave nula, timestamp ausente, etc.) são isolados com motivo de rejeição em vez de causar falha no job:

```python
df_transformed = df_transformed.withColumn(
    "_motivo_quarentena",
    F.when(F.col("id_chamada").isNull(), F.lit("id_chamada_nulo"))
     .when(F.col("dt_inicio").isNull(),  F.lit("dt_inicio_nula"))
     .otherwise(F.lit(None))
)

df_valid      = df_transformed.filter(F.col("_motivo_quarentena").isNull())
df_quarantine = df_transformed.filter(F.col("_motivo_quarentena").isNotNull())

# Grava quarentena particionada por data de ingestão
df_quarantine.write.mode("append")
    .partitionBy("ano_ingestao", "mes_ingestao", "dia_ingestao")
    .parquet(QUARANTINE_PATH)
```

### 5. Surrogate Keys nos Fatos

Fatos usam `monotonically_increasing_id()` ao invés de `row_number().over(Window.orderBy(...))`:

- `row_number()` sem `partitionBy` coleta todos os dados numa única partição → risco de OOM em escala
- `monotonically_increasing_id()` gera IDs únicos distribuídos entre os executores → BIGINT, sem shuffle global, produção-safe

```python
.withColumn("sk_chamada", F.monotonically_increasing_id())  # BIGINT
```

O MERGE usa `nk_*` (natural key) como chave de join, tornando o `sk_*` estável.

### 6. Adaptive Query Execution (AQE)

Todos os jobs Gold ativam AQE para otimização automática de joins e partições:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

### 7. Configuração Iceberg v2

Todas as tabelas seguem o padrão:

```sql
USING iceberg
TBLPROPERTIES (
    'format-version'                  = '2',
    'write.format.default'            = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'write.target-file-size-bytes'    = '134217728'   -- 128 MB
)
```

### 8. Reprocessamento por Camada (sem buscar na origem)

A arquitetura Medallion permite reprocessar qualquer camada aproveitando os dados já persistidos no S3 — sem acesso à fonte original. Os Glue jobs recebem `--REPROCESS_DATE` e filtram apenas a partição da data solicitada; o Iceberg v2 substitui somente aquela partição via `overwritePartitions()`.

```
Fonte → [Bronze S3] → Silver → Gold → Redshift
              ↑            ↑       ↑        ↑
         jamais        ponto   ponto    ponto
         retocar       de re-  de re-   de re-
                      processo processo processo
```

Os gates `ShortCircuitOperator` na DAG pulam as waves anteriores ao `start_layer` escolhido. O `trigger_rule=ALL_DONE` garante que mesmo com tasks em SKIPPED o pipeline avança até o Redshift.

### 9. Event-Driven Pipeline

Zero agendamento fixo. O pipeline é acionado por chegada de dados:

```
S3 PutObject  →  EventBridge Rule
              →  Lambda fn_start_glue_crawler
              →  Glue Crawler (catálogo atualizado)
              →  Glue Workflow (18 jobs B→S + 22 jobs S→G)
```

---

## Conformidade LGPD

Os seguintes dados pessoais identificáveis (PII) são mascarados de forma **irreversível** na camada Silver. O dado original permanece **apenas no Bronze**, com acesso controlado via AWS Lake Formation.

| Campo | Técnica | Exemplo |
|---|---|---|
| CPF / Nr Documento | 3 dígitos + \*\*\*\*\* + 2 dígitos | `123*****45` |
| E-mail | Domínio preservado, local mascarado | `***@gmail.com` |
| Telefone | 6 asteriscos + 4 últimos dígitos | `******1234` |
| CEP | 5 primeiros dígitos + *** | `01310***` |
| Login do operador | 3 primeiros chars + *** | `joh***` |
| Conteúdo de mensagem de chat | Drop completo; substituído por `nr_tamanho_chars` | — |
| Observações de tickets | Drop completo; substituído por `nr_tamanho_observacao_chars` | — |
| Feedback de avaliação | Drop completo; substituído por `nr_tamanho_feedback_chars` | — |

---

## Estrutura do Repositório

```
contact-center-data-lakehouse/
│
├── README.md
├── .gitignore
├── requirements.txt
│
├── docs/
│   ├── architecture.md               ← Decisões técnicas e trade-offs
│   ├── cost-guide.md                 ← Estratégia de custo mínimo AWS
│   ├── manual-provisionamento-aws.md ← Passo a passo de setup (11 etapas)
│   └── quicksight_guide.md           ← Setup QuickSight: datasource, datasets, 5 páginas BI
│
├── dags/                             ← Airflow DAGs
│   ├── dag_pipeline_diario.py        ← Orquestração completa: 40 Glue jobs em 4 waves
│   └── dag_carga_redshift.py         ← TRUNCATE + COPY → Redshift (22 tabelas, schedule=None)
│
├── infrastructure/
│   ├── 01_setup_s3.py                ← Bucket, pastas, lifecycle, versionamento
│   ├── 02_setup_glue.py              ← Databases, 18 crawlers, IAM role, registro dos 40 jobs
│   ├── 03_setup_lambda.py            ← Deploy da Lambda de trigger de crawler
│   ├── 04_setup_redshift.py          ← Redshift Serverless (boto3, original)
│   ├── 05_setup_emr_serverless.py    ← EMR Serverless application
│   ├── 06_run_pipeline.py            ← Orquestrador: executa os 40 jobs em sequência
│   ├── airflow/
│   │   ├── docker-compose.yaml       ← Airflow 2.9.3 local (webserver + scheduler + postgres)
│   │   ├── .env.example              ← Template de variáveis (AWS, Redshift)
│   │   └── requirements.txt          ← Providers Amazon + Postgres
│   ├── quicksight/
│   │   ├── 01_setup_quicksight.py    ← IAM role + Athena datasource + 5 datasets SPICE
│   │   └── iam_quicksight_policy.json← Policy para QuickSight acessar Athena/S3/Glue
│   ├── redshift/
│   │   ├── ddl/
│   │   │   ├── create_tables.sql     ← DDL das 22 tabelas Gold (schemas validados via Athena DESCRIBE)
│   │   │   └── create_views.sql      ← 9 views analíticas de KPI (01–12)
│   │   └── setup_redshift.py         ← Boto3: Namespace + Workgroup + IAM Role S3CopyPolicy
│   └── terraform/
│       ├── main.tf                   ← Provider AWS, backend, data sources
│       ├── variables.tf              ← Variáveis configuráveis (região, bucket, etc.)
│       ├── s3.tf                     ← Bucket, versionamento, lifecycle, EventBridge
│       ├── iam.tf                    ← Roles Glue, Lambda e QuickSight
│       ├── glue.tf                   ← Databases, 40 jobs, workflow e triggers
│       ├── lambda.tf                 ← Lambda + EventBridge (S3→crawler trigger)
│       ├── cloudwatch.tf             ← Log groups (14 dias) + alarme de falha
│       ├── redshift.tf               ← Redshift Serverless Namespace + Workgroup
│       ├── outputs.tf                ← ARNs e nomes exportados após apply
│       └── terraform.tfvars          ← Valores padrão para ambiente dev
│
├── pipeline/
│   ├── ingestion/
│   │   └── s3_data_loader.py         ← Substitui DMS/Kinesis: CSV → S3 Bronze
│   │
│   ├── bronze_to_silver/             ← 18 jobs PySpark
│   │   └── job_tb_*.py
│   │
│   └── silver_to_gold/               ← 22 jobs PySpark (11 dims + 11 fatos)
│       └── job_dim_*.py / job_fato_*.py
│
├── lambda/
│   └── fn_start_glue_crawler/
│       └── lambda_function.py        ← S3 Event → identifica tabela → inicia crawler
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
│       ├── generate_data.py          ← Gera dados realistas para 18 tabelas (Faker)
│       └── output/                   ← CSVs + distribuições (PNG)
│
└── notebooks/
    ├── 01_exploratory_data_analysis.ipynb
    ├── 02_kpi_operacional.ipynb
    ├── 03_performance_operadores.ipynb
    ├── 04_analise_campanhas.ipynb
    └── 05_canais_digitais.ipynb
```

---

## Como Executar

### Pré-requisitos

- Python 3.9+
- AWS CLI configurado (`aws configure`)
- Conta AWS com permissões em: S3, Glue, Lambda, Athena, Redshift, EMR, IAM
- Java 8+ (para PySpark local)
- Docker Desktop (para Airflow local)

```bash
git clone https://github.com/ilcileyjunior01/contact-center-data-lakehouse.git
cd contact-center-data-lakehouse
pip install -r requirements.txt
```

### Passo 1 — Provisionar infraestrutura AWS

```bash
# Bucket S3 com estrutura de pastas, lifecycle e versionamento
python infrastructure/01_setup_s3.py

# Glue: databases (db_bronze/db_silver/db_gold), 18 crawlers, jobs
python infrastructure/02_setup_glue.py

# Lambda: fn_start_glue_crawler + regra EventBridge
python infrastructure/03_setup_lambda.py

# Redshift Serverless: Namespace + Workgroup + IAM Role S3CopyPolicy
python infrastructure/redshift/setup_redshift.py \
  --aws-account-id <ACCOUNT_ID> \
  --wait

# EMR Serverless application (opcional)
python infrastructure/05_setup_emr_serverless.py
```

### Passo 2 — Criar tabelas e views no Redshift

O Redshift Serverless não aceita conexão TCP direta quando `publicly_accessible=false`. Use o **Redshift Data API** via boto3 ou o Query Editor v2 no console AWS:

```python
# Execute via boto3 redshift-data (ver infrastructure/redshift/setup_redshift.py)
# ou cole o conteúdo dos arquivos DDL no Query Editor v2:
#   infrastructure/redshift/ddl/create_tables.sql  (22 tabelas)
#   infrastructure/redshift/ddl/create_views.sql   (9 views KPI)
```

### Passo 3 — Gerar dados sintéticos e carregar Bronze

```bash
# Gera ~5.000 registros por tabela (18 tabelas) com dados realistas via Faker
python data/synthetic/generate_data.py

# Carrega CSVs no S3 Bronze (simula DMS + Kinesis Firehose)
python pipeline/ingestion/s3_data_loader.py
```

### Passo 4 — Executar pipeline

```bash
# Executa os 40 jobs na ordem correta com paralelismo controlado
python infrastructure/06_run_pipeline.py

# Simula sem executar (mostra a ordem e as etapas)
python infrastructure/06_run_pipeline.py --dry-run

# Executa apenas uma etapa
python infrastructure/06_run_pipeline.py --only bronze   # Bronze→Silver
python infrastructure/06_run_pipeline.py --only gold     # Dims + Fatos
python infrastructure/06_run_pipeline.py --only dims     # Apenas as 11 dimensões Gold
python infrastructure/06_run_pipeline.py --only fatos    # Apenas as 11 tabelas fato Gold
```

O script respeita a ordem de dependências:
1. Bronze → Silver (18 jobs, lotes de 5 em paralelo)
2. Silver → Gold Dimensões (11 jobs, paralelo total)
3. Silver → Gold Fatos Wave 1 — fatos base (fato_chamada, fato_chat, fato_ticket, fato_whatsapp, fato_discagem, fato_jornada_operador, fato_metricas_operacionais)
4. Silver → Gold Fatos Wave 2 — fatos dependentes: fato_qualidade, fato_interacao_ticket, fato_mensagem_chat, fato_ura_navegacao

### Passo 5 — Orquestrar com Airflow

```bash
cd infrastructure/airflow
cp .env.example .env
# Edite .env com AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, REDSHIFT_PASSWORD

docker compose up airflow-init
docker compose up -d
# Acesse http://localhost:8080  (admin/admin)
# Ative: cc_pipeline_diario
```

A DAG `cc_pipeline_diario` executa as 4 waves de jobs Glue e ao final aciona automaticamente a `cc_carga_redshift`, que usa a estratégia **Athena UNLOAD → S3 staging → Redshift COPY** para carregar as 22 tabelas.

### Passo 6 — Consultar KPIs

**Via Athena** (workgroup: `wg-cc-analytics`):
```sql
-- Exemplo: Volume e TMA de chamadas por fila e data
SELECT d.nr_ano, d.nr_mes, f.nm_fila,
    COUNT(*) AS total_chamadas,
    ROUND(AVG(c.nr_duracao_segundos) / 60.0, 2) AS tma_minutos
FROM db_gold.fato_chamada c
JOIN db_gold.dim_data d ON c.sk_data_inicio = d.sk_data
JOIN db_gold.dim_fila  f ON c.sk_fila        = f.sk_fila
GROUP BY d.nr_ano, d.nr_mes, f.nm_fila
ORDER BY d.nr_ano, d.nr_mes, total_chamadas DESC;
```

**Via Redshift** (schema `gold`):
```sql
-- KPI pronto: métricas de fila com nível de serviço
SELECT * FROM gold.vw_kpi_09_metricas_fila
ORDER BY nr_ano, nr_mes, nm_fila;

-- KPI de qualidade por operador
SELECT * FROM gold.vw_kpi_03_qualidade
ORDER BY pct_aprovacao DESC;
```

---

## KPIs e Queries Athena

| Arquivo SQL | KPIs Calculados |
|---|---|
| `01_volume_desempenho_chamadas.sql` | Volume total, TMA, TME, taxa de abandono, calls por hora |
| `02_performance_operadores.sql` | Ranking de operadores, chamadas atendidas, TMA individual |
| `03_qualidade_atendimento.sql` | Nota média, % aprovados, % críticos, distribuição por faixa |
| `04_volume_tickets.sql` | Volume por status, categoria, prioridade; tickets abertos |
| `05_eficiencia_tickets.sql` | MTTR (min), % dentro do SLA, backlog atual |
| `06_volume_chat_whatsapp.sql` | Sessões por canal, mensagens por sessão, duração média |
| `07_satisfacao_digital.sql` | Taxa de conclusão, comparativo chat vs WhatsApp |
| `08_desempenho_campanhas.sql` | Taxa de contato, taxa de atendimento por campanha |
| `09_roi_campanhas.sql` | Custo por contato, conversão, ROI estimado |
| `10_jornada_operador.sql` | Horas trabalhadas, presença, produtividade por operador |
| `11_ocupacao_filas.sql` | Volume por fila, cumprimento de SLA, distribuição de carga |
| `12_efetividade_ura.sql` | Taxa de auto-atendimento, abandono na URA, opções mais usadas |

---

## Notebooks de Análise

| Notebook | Conteúdo |
|---|---|
| `01_exploratory_data_analysis.ipynb` | Distribuições, volumes, qualidade dos dados nas 18 entidades |
| `02_kpi_operacional.ipynb` | TMA, TME, taxa de abandono, SLA, FCR, volume por canal |
| `03_performance_operadores.ipynb` | Ranking, qualidade, produtividade, ausências, matriz de quadrantes |
| `04_analise_campanhas.ipynb` | ROI de campanhas, taxa de conversão, curva de diminishing returns |
| `05_canais_digitais.ipynb` | Chat, WhatsApp e URA: volume, satisfação CSAT, taxa de autoatendimento |

---

## Custo AWS Estimado

> Baseado em dados sintéticos (~5K linhas/tabela) e 2 execuções completas do pipeline por mês.

### Modo ocioso (sem rodar jobs — apenas infraestrutura ativa)

| Serviço | Configuração | Custo/mês |
|---|---|---|
| S3 (armazenamento + lifecycle rules) | ~500 MB, versões antigas expiram em 7 dias | ~$0.03 |
| Redshift Serverless (auto-pause 30 min) | 8 RPUs, pausa automática quando inativo | ~$0.50 |
| CloudWatch Logs (retenção 7 dias) | Log groups Glue, Lambda, EMR | ~$0.02 |
| Lambda, Athena, Glue, EMR (ociosos) | Pay-per-use, $0 sem execução | $0.00 |
| **Total em modo ocioso** | | **~$0.57/mês** |

### Modo ativo (2 execuções completas do pipeline por mês)

| Serviço | Uso | Custo/mês estimado |
|---|---|---|
| S3 (500 MB armazenamento + requests) | Todas as camadas + staging Redshift | ~$0.03 |
| Lambda (triggers de crawler) | ~100 invocações | ~$0.00 |
| Athena (queries KPI + UNLOAD para Redshift) | ~50 queries + 22 UNLOADs | ~$0.10 |
| Glue Jobs (pipeline completo, 2×/mês) | 40 jobs × G.1X × 2 workers | ~$3.20 |
| Glue Crawlers (18 crawlers × 2 min) | 2 rodadas completas | ~$1.00 |
| Redshift Serverless (auto-pause 30 min) | Carga pontual + queries demo | ~$0.50 |
| EMR Serverless (jobs pontuais) | 5 jobs/mês | ~$0.10 |
| CloudWatch Logs (retenção 7 dias) | Logs de jobs | ~$0.02 |
| **Total em modo ativo** | | **< $5/mês** |

**Principais economias aplicadas:**

- Substituir DMS + Kinesis por `s3_data_loader.py` em modo demo: economia de ~$24/mês
- Redshift Serverless com auto-pause: cobra apenas RPUs × segundos de uso efetivo
- CloudWatch Logs com retenção de 14 dias: economia de ~$0,30/mês vs NEVER_EXPIRE
- Parquet + Snappy + particionamento: reduz custo de Athena em ~70% vs CSV

---

## Validação dos Dados (Execução de Referência)

Resultados reais obtidos após execução completa do pipeline em 2026-07-10/14.

### Contagens Gold — Dimensões (Athena)

| Tabela | Linhas |
|--------|-------:|
| dim_data | 5.845 (range: 2015-01-01 → 2030-12-31) |
| dim_cliente | 4.159 |
| dim_operador | 208 |
| dim_skill | 26 |
| dim_campanha | 25 |
| dim_fila | 17 |
| dim_canal | 6 |
| dim_status_chamada | 5 |
| dim_status_ticket | 5 |
| dim_categoria_ticket | 5 |
| dim_prioridade_ticket | 5 |

### Contagens Gold — Fatos (Athena)

| Tabela | Linhas |
|--------|-------:|
| fato_jornada_operador | 5.985 |
| fato_metricas_operacionais | 4.187 |
| fato_discagem | 4.189 |
| fato_mensagem_chat | 4.905 |
| fato_chamada | 4.168 |
| fato_ura_navegacao | 4.126 |
| fato_qualidade | 2.504 |
| fato_interacao_ticket | 2.494 |
| fato_chat | 2.113 |
| fato_ticket | 2.059 |
| fato_whatsapp | 1.386 |

### Redshift Serverless — Views KPI validadas

| View | Linhas | Amostra |
|---|---:|---|
| vw_kpi_01_volume_chamadas | 144 | TMA abandono ~1 min, atendidas ~23 min |
| vw_kpi_02_performance_operadores | 208 | Top operador: 29 chamadas, TMA 18,36 min |
| vw_kpi_03_qualidade | 1.523 | Nota média 7,0–10,0 por operador/mês |
| vw_kpi_04_volume_tickets | 1.350 | Categorias: ELOGIO, RECLAMAÇÃO, SOLICITAÇÃO |
| vw_kpi_05_eficiencia_tickets | 144 | TRT médio 3,7–9,4h por prioridade |
| vw_kpi_06_volume_digital | 72 | WhatsApp ~67 min vs Chat ~30 min (duração média) |
| vw_kpi_08_campanhas | 25 | Taxa de atendimento 55–66% por campanha |
| vw_kpi_09_metricas_fila | 611 | Nível de serviço 86–93% por fila |
| vw_kpi_12_ura | 252 | Opção mais usada: "4 - FALAR COM OPERADOR" |

### Integridade Referencial (0 orphans)

- `fato_chamada → dim_canal`: 0 registros sem correspondência
- `fato_chamada → dim_operador`: 0 registros sem correspondência
- `fato_ticket → dim_cliente`: 0 registros sem correspondência
- `fato_qualidade → fato_chamada`: 0 registros sem correspondência

### Distribuição de Qualidade

| Faixa | Total | Nota Média |
|-------|------:|-----------:|
| EXCELENTE | 492 | 9,49 |
| BOM | 501 | 7,47 |
| REGULAR | 522 | 5,49 |
| RUIM | 496 | 3,52 |
| CRITICO | 493 | 1,49 |

---

## Autor

**Ilciley Junior**

Engenheiro de Dados especializado em pipelines AWS, arquitetura Lakehouse e processamento distribuído com PySpark.

[![GitHub](https://img.shields.io/badge/GitHub-ilcileyjunior01-181717?logo=github)](https://github.com/ilcileyjunior01)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Ilciley%20Junior-0A66C2?logo=linkedin)](https://www.linkedin.com/in/ilcileyjunior)
