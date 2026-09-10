# Glossário — Contact Center Data Lakehouse

Termos técnicos e siglas de negócio usados neste projeto.

---

## Termos de Engenharia de Dados

| Termo | Definição no contexto do projeto |
|---|---|
| **CDC** | *Change Data Capture* — captura de INSERT/UPDATE/DELETE via WAL do PostgreSQL. Cada evento carrega o campo `Op` (I/U/D) e o timestamp `_timestamp`. |
| **WAL** | *Write-Ahead Log* — log transacional do PostgreSQL lido pelo AWS DMS para capturar mudanças sem impactar as queries da aplicação. |
| **Medallion** | Arquitetura em camadas: **Bronze** (raw, dado exatamente como veio) → **Silver** (limpo, deduplicado, PII mascarado) → **Gold** (modelado em Star Schema para consumo analítico). |
| **Iceberg v2** | Formato de tabela open-source com suporte a ACID, `MERGE INTO`, Time Travel e Schema Evolution. Usado nas camadas Silver e Gold. |
| **Watermark** | Timestamp do último registro CDC processado por tabela, persistido em `s3://{bucket}/checkpoints/{tabela}/watermark.json`. Garante leitura incremental no nível de registro. |
| **Job Bookmark** | Mecanismo nativo do AWS Glue que rastreia quais arquivos S3 já foram lidos. Complementa o Watermark, que opera no nível de registro dentro de cada arquivo. |
| **Quarentena** | Área separada (`s3://{bucket}/quarantine/{tabela}/`) onde registros inválidos (chave nula, timestamp ausente) são gravados com a coluna `_motivo_quarentena`. O job nunca falha por conta desses registros. |
| **Surrogate Key (sk_)** | Chave técnica gerada pelo Data Warehouse, independente do sistema fonte. Dimensões usam `row_number()` (INT determinístico); fatos usam `monotonically_increasing_id()` (BIGINT distribuído, sem shuffle global). |
| **Natural Key (nk_)** | Chave de negócio vinda do sistema fonte, preservada para rastreabilidade e usada como chave de join no `MERGE INTO` das tabelas fato. |
| **Star Schema** | Modelo dimensional com tabelas fato no centro ligadas a dimensões via surrogate keys. Este projeto possui 11 dimensões e 11 fatos na camada Gold. |
| **AQE** | *Adaptive Query Execution* — otimização automática de joins e coalescência de partições em tempo de execução no Spark. Ativado nos jobs Gold com `spark.sql.adaptive.enabled = true`. |
| **UNLOAD** | Comando do Athena que exporta o resultado de uma query para o S3 em formato CSV ou Parquet. Usado na carga do Redshift para contornar os metadados do Iceberg v2 no prefixo S3. |
| **RPU** | *Redshift Processing Unit* — unidade de capacidade do Redshift Serverless. O workgroup `cc-lakehouse-workgroup` usa 8 RPUs com auto-pause de 30 minutos. |
| **ShortCircuitOperator** | Operator do Airflow que avalia uma condição e, se `False`, pula (SKIPPED) todas as tasks downstream. Usado nos gates `gate_bronze`, `gate_silver` e `gate_gold` para o reprocessamento seletivo por camada. |
| **Time Travel** | Capacidade do Iceberg de consultar snapshots históricos de uma tabela via `FOR SYSTEM_TIME AS OF TIMESTAMP '...'`. Usado no plano de contingência para reverter dados corrompidos. |
| **hash_registro** | MD5 calculado sobre todos os campos de negócio de um registro. O `MERGE INTO` só atualiza a linha quando o hash muda — garante idempotência e evita escritas desnecessárias. |
| **start_layer** | Parâmetro da DAG `cc_pipeline_diario` que define o ponto de entrada do reprocessamento: `bronze` (padrão), `silver`, `gold` ou `redshift`. |
| **DISTSTYLE ALL** | Estratégia de distribuição do Redshift que replica a tabela em todos os nós. Aplicada nas dimensões pequenas (operador, fila, canal) para eliminar shuffle nos JOINs. |

---

## Siglas de Contact Center

| Sigla | Significado | Onde aparece no projeto |
|---|---|---|
| **TMA** | *Tempo Médio de Atendimento* — duração média das chamadas atendidas | `fato_metricas_operacionais.nr_tma_segundos/minutos`; KPI `01` e `02` |
| **TME** | *Tempo Médio de Espera* — tempo na fila antes do início do atendimento | `fato_metricas_operacionais.nr_tme_segundos/minutos`; KPI `01` |
| **SLA** | *Service Level Agreement* — % de chamadas atendidas dentro de X segundos; também usado em tickets (480 min = 8h) | `fl_meta_nivel_servico`, `fl_dentro_sla`; KPI `01` e `05` |
| **MTTR** | *Mean Time To Resolution* — tempo médio de resolução de um ticket de suporte | `fato_ticket.nr_tempo_resolucao_min`; KPI `05` |
| **FCR** | *First Call Resolution* — percentual de atendimentos resolvidos no primeiro contato | Notebooks `02` e `05`; KPI `01` |
| **URA / IVR** | *Unidade de Resposta Audível / Interactive Voice Response* — sistema de autoatendimento por voz | Tabelas `tb_ura_navegacao` → `db_silver.ura_navegacao` → `fato_ura_navegacao`; KPI `12` |
| **CSAT** | *Customer Satisfaction Score* — pontuação de satisfação do cliente | `fato_qualidade.nr_nota`; notebooks `02` e `05` |
| **NPS** | *Net Promoter Score* — métrica de lealdade do cliente (não implementado — campo reservado) | — |

---

## Convenções de Prefixo de Colunas

| Prefixo | Significado | Exemplo |
|---|---|---|
| `sk_` | Surrogate Key (gerada pelo DW) | `sk_cliente`, `sk_data` |
| `nk_` | Natural Key (vinda do sistema fonte) | `nk_cliente`, `nk_chamada` |
| `id_` | Identificador técnico (Bronze/Silver) | `id_chamada`, `id_operador` |
| `nm_` | Nome / label descritivo | `nm_operador`, `nm_fila` |
| `ds_` | Descrição textual ou categórica | `ds_status`, `ds_faixa_nota` |
| `nr_` | Valor numérico / métrica | `nr_duracao_segundos`, `nr_nota` |
| `dt_` | Data ou timestamp | `dt_inicio`, `dt_admissao` |
| `st_` | Status (texto) | `st_chamada`, `st_operador` |
| `fl_` | Flag booleano (SMALLINT 0/1) | `fl_duracao_valida`, `fl_presente` |
| `tp_` | Tipo / categoria | `tp_chamada` |

> Ver também: `docs/book_de_variaveis.md` para documentação detalhada de cada variável analítica (lineage, qualidade, governança).
