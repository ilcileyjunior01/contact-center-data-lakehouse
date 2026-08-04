# Arquitetura — Contact Center Data Lakehouse

## Decisões Técnicas e Trade-offs

### 1. Por que Arquitetura Medallion (Bronze / Silver / Gold)?

A arquitetura Medallion separa cada responsabilidade em uma camada distinta, garantindo rastreabilidade e reprocessamento seguro.

| Camada | Responsabilidade | Formato |
|---|---|---|
| **Bronze** | Ingestão raw sem transformação. Dado exatamente como veio da fonte. | Parquet + Snappy |
| **Silver** | Limpeza, deduplicação CDC, mascaramento PII, validação. | Apache Iceberg v2 |
| **Gold** | Agregação, enriquecimento, modelagem dimensional (Star Schema). | Apache Iceberg v2 |

**Trade-off:** Aumenta o custo de armazenamento (dados em três camadas), mas garante que qualquer camada pode ser reprocessada a partir da anterior sem necessidade de acesso à fonte original.

**Reprocessamento na prática:** Os jobs Glue recebem o parâmetro `--REPROCESS_DATE` e filtram apenas a partição da data solicitada nas tabelas Iceberg. A DAG `cc_pipeline_diario` expõe os params `reprocess_date` e `start_layer` com gates `ShortCircuitOperator` que pulam as waves anteriores à camada de entrada — sem tocar na origem.

---

### 2. Por que Apache Iceberg e não Delta Lake ou Hudi?

- **Integração nativa AWS Glue 4.0**: Iceberg é suportado nativamente no Glue sem bibliotecas adicionais.
- **ACID com MERGE INTO**: Permite operações de upsert que são essenciais para o padrão CDC.
- **Time Travel**: Possibilidade de consultar snapshots históricos diretamente pelo Athena.
- **Schema Evolution**: Adição e remoção de colunas sem reescrever toda a tabela.
- **Compatibilidade com Athena e Redshift Spectrum**: Consulta direta sobre S3 sem mover dados.

---

### 3. Por que CDC via WAL do PostgreSQL?

O Change Data Capture via Write-Ahead Log captura **todas as mudanças** (INSERT, UPDATE, DELETE) sem impacto nas queries da aplicação (leitura do log, não da tabela).

```
PostgreSQL WAL → AWS DMS → Kinesis Streams → Kinesis Firehose → S3 Bronze
```

**Alternativa para ambiente de demonstração:** Script Python `s3_data_loader.py` que simula a mesma estrutura de arquivos Parquet no S3, eliminando o custo do DMS (~$0.18/hora) e Kinesis (~$10/mês por shard).

---

### 4. Por que Glue 4.0 e não EMR dedicado?

| Critério | Glue 4.0 | EMR Dedicado |
|---|---|---|
| Gerenciamento | Serverless, zero ops | Requer provisionamento de cluster |
| Custo idle | Zero (paga por execução) | Alto (cluster ligado = cobrança) |
| Escalabilidade | Automática | Manual (auto-scaling configurável) |
| Integração Catalog | Nativa | Requer configuração |
| Cold start | ~30-60s | Cluster já quente |

**Quando usar EMR Serverless:** Jobs Spark muito longos (>2h), uso intensivo de GPU para ML, ou quando o Glue atingir limites de workers.

---

### 5. Padrão de Idempotência: MERGE + Hash MD5

Todos os jobs Silver e Gold usam o seguinte padrão para garantir que reprocessamentos não criam duplicatas:

```python
# 1. Calcula hash dos campos relevantes
df = df.withColumn("hash_registro", md5(concat_ws("|", col1, col2, ...)))

# 2. MERGE INTO: atualiza se hash diferente, insere se não existe
spark.sql("""
    MERGE INTO db_silver.chamada AS target
    USING staging AS source
    ON target.id_chamada = source.id_chamada
    WHEN MATCHED AND target.hash_registro <> source.hash_registro
        THEN UPDATE SET *
    WHEN NOT MATCHED
        THEN INSERT *
""")
```

---

### 6. Controle Incremental: Watermark + Glue Bookmark

Dois mecanismos complementares evitam reprocessamento desnecessário:

- **Glue Job Bookmark**: rastreia arquivos S3 já processados em nível de arquivo.
- **Watermark JSON**: rastreia o `max(_timestamp)` processado por tabela, armazenado em `s3://bucket/checkpoints/`.

```json
{
  "tabela": "tb_chamada",
  "watermark": "2024-06-15T23:59:59.000000",
  "atualizado_em": "2024-06-16T01:05:32.123456"
}
```

---

### 7. Padrão de Quarentena

Registros que falham nas regras de qualidade são isolados em path separado com metadados de diagnóstico:

```
s3://act-cc-dev-lakehouse/quarantine/tb_chamada/ano=2024/mes=06/dia=15/
  └── quarantine_20240615_143022_run-abc123.parquet
```

Cada registro em quarentena inclui a coluna `motivo_quarentena` para facilitar diagnóstico e eventual correção.

---

### 8. Observabilidade: Logs Estruturados

Cada job Glue emite logs JSON estruturados para CloudWatch e S3:

```json
{
  "job_name": "job-tb-chamada-bronze-to-silver",
  "run_id": "jr_abc123",
  "tabela": "tb_chamada",
  "dt_inicio": "2024-06-15T01:00:00Z",
  "dt_fim": "2024-06-15T01:03:22Z",
  "watermark_anterior": "2024-06-14T23:59:59Z",
  "watermark_novo": "2024-06-15T23:59:59Z",
  "qt_lidos": 15420,
  "qt_validos": 15398,
  "qt_quarentena": 22,
  "status": "SUCCESS"
}
```

---

### 9. Governança LGPD na Camada Silver

| Campo Original | Tratamento na Silver | Método |
|---|---|---|
| `nr_documento` (CPF) | `123*****45` (primeiros 3 + últimos 2) | Python string slicing |
| `ds_email` | `***@gmail.com` (somente domínio) | split("@")[1] |
| `nr_telefone` | `******9876` (últimos 4 dígitos) | Python string slicing |
| `nr_cep` | `01310***` (primeiros 5 dígitos) | Python string slicing |

O dado original permanece apenas na camada Bronze, protegido por AWS Lake Formation com controle de acesso por coluna. Apenas roles com permissão explícita conseguem ler as colunas PII na Bronze.

---

### 10. Reprocessamento por Camada — ShortCircuit Gates

A DAG `cc_pipeline_diario` implementa três `ShortCircuitOperator` (gates) que controlam o ponto de entrada do pipeline em cada reprocessamento:

```
inicio → [gate_bronze] → Wave 1 (Bronze→Silver) → fim_bronze
              → [gate_silver] ALL_DONE → Wave 2 (Gold Dims) → fim_dims
                    → [gate_gold]  ALL_DONE → Wave 3+4 (Fatos) → fim_pipeline
                                                    → TriggerDagRunOperator (ALL_DONE) → Redshift
```

**Como funciona:**
- `gate_bronze` retorna `True` apenas se `start_layer == "bronze"`. Se `False`, os 18 jobs ficam SKIPPED.
- `gate_silver` tem `trigger_rule=ALL_DONE`: executa mesmo com Bronze SKIPPED. Retorna `True` se `start_layer in ("bronze", "silver")`.
- `gate_gold` idem: executa mesmo com Bronze+Silver SKIPPED.
- O `TriggerDagRunOperator` final também tem `trigger_rule=ALL_DONE`, garantindo que a carga do Redshift sempre ocorre independentemente de quais waves foram puladas.

**Passagem do parâmetro para os Glue jobs:**
```python
script_args={
    "--REPROCESS_DATE": "{{ params.reprocess_date or ds }}",
}
```
O job Glue usa esse valor para filtrar a partição correta na leitura da tabela Iceberg e reescreve apenas aquela partição com `overwritePartitions()`, sem impactar datas anteriores.

---

### 11. Orquestração Event-Driven

O pipeline **não usa schedules fixos** (cron). Cada etapa é acionada pelo evento da anterior:

```
S3 recebe arquivo → S3 Event Notification → EventBridge Rule
    → Lambda fn-start-glue-crawler → Glue Crawler executa
    → Crawler atualiza Catalog → Glue Workflow Trigger ativado
    → Job Bronze→Silver executa → Conclui → Próximo trigger
    → ... (8 triggers encadeados) → Gold completo
```

**Vantagem:** O pipeline processa dados assim que chegam, sem esperar o próximo horário de cron. Múltiplos arquivos que chegam simultaneamente são agrupados pelo Firehose antes de acionar o crawler.
