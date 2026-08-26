# Pipeline Bronze → Silver

Jobs PySpark (AWS Glue 4.0) responsáveis pela ingestão incremental, limpeza, mascaramento PII e escrita idempotente na camada Silver do Data Lakehouse Contact Center.

---

## Fluxo Padrão de Processamento (7 Passos)

Todos os 18 jobs seguem o mesmo padrão de execução:

```
1. LER WATERMARK
   └─ Recupera o último timestamp processado de s3://{bucket}/checkpoints/{tabela}/watermark.json
   └─ Na primeira execução, usa 1970-01-01T00:00:00Z (full load)

2. LEITURA INCREMENTAL (Bronze)
   └─ Glue Job Bookmark controla quais arquivos S3 já foram lidos
   └─ Filtro adicional por _timestamp > last_watermark garante janela exata de eventos CDC

3. DEDUPLICAÇÃO CDC
   └─ Converte Op (I/U/D) → op_cdc (INSERT/UPDATE/DELETE)
   └─ Window partitionBy(chave_primária).orderBy(dt_cdc_evento DESC)
   └─ Mantém apenas o evento mais recente de cada entidade no batch

4. MASCARAMENTO PII (LGPD)
   └─ Aplicado apenas em tabelas com dados pessoais
   └─ Dados originais permanecem no Bronze com acesso restrito via Lake Formation
   └─ Técnicas: hash parcial de documento, extração de domínio do e-mail, truncamento de telefone

5. CAMPOS DERIVADOS E VALIDAÇÕES
   └─ Flags de negócio (fl_*), faixas categóricas (ds_faixa_*), durações calculadas
   └─ Hash MD5 de integridade (hash_registro) para controle de idempotência no MERGE

6. SEPARAÇÃO VÁLIDOS × QUARENTENA
   └─ Registros com chave primária nula ou campos obrigatórios ausentes → s3://{bucket}/quarantine/{tabela}/
   └─ Registros válidos prosseguem para o MERGE

7. MERGE ICEBERG + ATUALIZAR WATERMARK
   └─ INSERT quando id não existe na Silver
   └─ UPDATE quando hash_registro mudou (dado alterado na origem)
   └─ Registros com mesmo hash não são reescritos (idempotência)
   └─ Watermark atualizado para max(_timestamp) do batch
   └─ job.commit() avança o ponteiro do Glue Job Bookmark
```

---

## Catálogo dos 18 Jobs

### Domínio: Operação / Voz

| # | Nome do Job | Tabela Bronze (Origem) | Tabela Silver (Destino) | Principais Transformações |
|---|-------------|------------------------|-------------------------|---------------------------|
| 1 | `job_tb_chamada_bronze_to_silver.py` | `db_bronze.tb_chamada` | `db_silver.chamada` | Cast de timestamps dt_inicio/dt_fim; normalização upper/trim de st_chamada e tp_chamada; limpeza de caracteres em nr_telefone_origem e nr_telefone_destino; coalesce de id_operador/id_fila para -1; flags `fl_duracao_valida` e `fl_chamada_completa`; campo `nr_duracao_minutos`; partição por ano/mes/dia de dt_inicio |
| 2 | `job_tb_gravacao_chamada_bronze_to_silver.py` | `db_bronze.tb_gravacao_chamada` | `db_silver.gravacao_chamada` | Cast de dt_expiracao; `nr_tamanho_mb` com coalesce para 0; remoção de `ds_url_arquivo` (dado sensível de acesso ao arquivo); flags `fl_tem_gravacao` e `fl_expirada`; campo `nr_dias_para_expirar` via datediff |
| 3 | `job_tb_ura_navegacao_bronze_to_silver.py` | `db_bronze.tb_ura_navegacao` | `db_silver.ura_navegacao` | Normalização upper de ds_opcao; cast de nr_tempo_espera; coalesce de nr_tempo_espera para 0; flag `fl_abandonou_ura`; campo `ds_faixa_espera` (categorização do tempo de espera em faixas); partição por ano/mes/dia de dt_evento |

### Domínio: Canais Digitais

| # | Nome do Job | Tabela Bronze (Origem) | Tabela Silver (Destino) | Principais Transformações |
|---|-------------|------------------------|-------------------------|---------------------------|
| 4 | `job_tb_chat_bronze_to_silver.py` | `db_bronze.tb_chat` | `db_silver.chat` | Cast de timestamps dt_inicio/dt_fim; normalização upper de st_chat; coalesce de id_operador para -1; coalesce de nr_duracao_segundos para 0; campo `nr_duracao_minutos`; flag `fl_chat_completo`; partição por ano/mes/dia de dt_inicio |
| 5 | `job_tb_mensagem_chat_bronze_to_silver.py` | `db_bronze.tb_mensagem_chat` | `db_silver.mensagem_chat` | Normalização upper de ds_remetente; campo `nr_tamanho_chars` (len da mensagem); remoção de `ds_mensagem` (conteúdo PII removido na Silver); flags `fl_mensagem_cliente` e `fl_mensagem_operador`; partição por ano/mes/dia de dt_envio |
| 6 | `job_tb_whatsapp_atendimento_bronze_to_silver.py` | `db_bronze.tb_whatsapp_atendimento` | `db_silver.whatsapp_atendimento` | Cast de timestamps dt_inicio/dt_fim; normalização upper de st_atendimento; coalesce de id_operador para -1; mascaramento PII de `nr_telefone` → `nr_telefone_mascarado` (4 últimos dígitos); campo `nr_duracao_minutos`; flag `fl_atendimento_completo` |

### Domínio: Tickets / Suporte

| # | Nome do Job | Tabela Bronze (Origem) | Tabela Silver (Destino) | Principais Transformações |
|---|-------------|------------------------|-------------------------|---------------------------|
| 7 | `job_tb_ticket_bronze_to_silver.py` | `db_bronze.tb_ticket` | `db_silver.ticket` | Cast de timestamps dt_abertura/dt_fechamento; normalização upper de st_ticket, ds_prioridade, ds_categoria; coalesce de id_operador_abertura para -1; campo `nr_tempo_resolucao_min` (datediff entre abertura e fechamento); flags `fl_ticket_resolvido` e `fl_dentro_sla`; partição por ano/mes/dia de dt_abertura |
| 8 | `job_tb_interacao_ticket_bronze_to_silver.py` | `db_bronze.tb_interacao_ticket` | `db_silver.interacao_ticket` | Cast de dt_interacao; normalização upper de ds_canal; coalesce de id_operador para -1; campo `nr_tamanho_observacao_chars`; remoção de `ds_observacao` (conteúdo textual removido na Silver); flag `fl_tem_observacao`; partição por ano/mes/dia de dt_interacao |

### Domínio: Campanhas / Discagem

| # | Nome do Job | Tabela Bronze (Origem) | Tabela Silver (Destino) | Principais Transformações |
|---|-------------|------------------------|-------------------------|---------------------------|
| 9 | `job_tb_campanha_bronze_to_silver.py` | `db_bronze.tb_campanha` | `db_silver.campanha` | Cast de timestamps dt_inicio/dt_fim; normalização upper/trim de nm_campanha e st_campanha; coalesce de st_campanha para "INATIVO"; campo `nr_duracao_dias` (datediff entre dt_inicio e dt_fim); flags `fl_campanha_ativa` e `fl_campanha_vigente` (baseada na data atual) |
| 10 | `job_tb_discagem_bronze_to_silver.py` | `db_bronze.tb_discagem` | `db_silver.discagem` | Cast de dt_discagem; normalização upper de st_discagem; `nr_telefone` **não existe no Bronze** → `nr_telefone_mascarado` definido como string vazia `''`; `fl_discagem_atendida` derivado de `fl_contato_realizado` (campo Bronze renomeado); remoção de `id_operador`, `nr_tentativas`, `fl_contato_realizado`, `fl_convertido` (colunas Bronze-only não mapeadas para Silver); flag `fl_discagem_nao_atendida`; partição por ano/mes/dia de dt_discagem |

### Domínio: Cadastro de Clientes

| # | Nome do Job | Tabela Bronze (Origem) | Tabela Silver (Destino) | Principais Transformações |
|---|-------------|------------------------|-------------------------|---------------------------|
| 11 | `job_tb_cliente_bronze_to_silver.py` | `db_bronze.tb_cliente` | `db_silver.cliente` | Cast de `dt_cadastro` como DATE (`yyyy-MM-dd`, somente data — não datetime); normalização upper/trim de nm_cliente e st_cliente; lower de ds_email; limpeza de nr_documento (apenas dígitos) e nr_telefone; mascaramento PII completo: `nr_documento_mascarado` (3 primeiros + "*****" + 2 últimos dígitos), `ds_email_mascarado` (somente domínio), `nr_telefone_mascarado` (4 últimos dígitos); remoção das colunas PII originais; flags `fl_cliente_ativo`, `fl_tem_email`, `fl_tem_telefone`, `fl_tem_documento`; partição por ano/mes de dt_cadastro |
| 12 | `job_tb_endereco_cliente_bronze_to_silver.py` | `db_bronze.tb_endereco_cliente` | `db_silver.endereco_cliente` | Normalização upper/trim de ds_logradouro, ds_bairro, ds_cidade, ds_estado; cast de nr_numero; limpeza de nr_cep (apenas dígitos); mascaramento PII de `nr_cep` → `nr_cep_mascarado` (mantém apenas os 5 primeiros dígitos); remoção de `nr_cep` original |

### Domínio: Cadastro de Operadores

| # | Nome do Job | Tabela Bronze (Origem) | Tabela Silver (Destino) | Principais Transformações |
|---|-------------|------------------------|-------------------------|---------------------------|
| 13 | `job_tb_operador_bronze_to_silver.py` | `db_bronze.tb_operador` | `db_silver.operador` | Cast de dt_admissao para DATE; normalização upper de nm_operador e st_operador; lower de ds_email e ds_login; mascaramento PII: `ds_email_mascarado` (somente domínio), `ds_login_mascarado` (3 primeiros chars + "***"); remoção de `ds_email` e `ds_login` originais; coalesce de id_supervisor para -1; flags `fl_operador_ativo` e `fl_tem_supervisor`; campos `nr_dias_casa` (datediff com current_date) e `ds_faixa_tempo_casa` (ATE_3_MESES / 3_A_6_MESES / 6_A_12_MESES / 1_A_2_ANOS / ACIMA_2_ANOS); partição por ano_admissao/mes_admissao |
| 14 | `job_tb_skill_operador_bronze_to_silver.py` | `db_bronze.tb_skill_operador` | `db_silver.skill_operador` | Normalização upper de ds_skill; cast e coalesce de nr_nivel para 0; campo `ds_faixa_nivel` (categorização do nível de skill em faixas descritivas) |
| 15 | `job_tb_jornada_operador_bronze_to_silver.py` | `db_bronze.tb_jornada_operador` | `db_silver.jornada_operador` | Cast de timestamps dt_inicio_turno e dt_fim_turno; coalesce de nr_tempo_pausa_min para 0; campo `nr_duracao_turno_min` (datediff em minutos); campo `nr_duracao_produtiva_min` (duracao_turno - tempo_pausa); flags `fl_turno_completo` e `fl_turno_normal` (240 a 600 minutos); campo `ds_turno` (categorização do horário de início em MANHA/TARDE/NOITE) |

### Domínio: Filas de Atendimento

| # | Nome do Job | Tabela Bronze (Origem) | Tabela Silver (Destino) | Principais Transformações |
|---|-------------|------------------------|-------------------------|---------------------------|
| 16 | `job_tb_fila_atendimento_bronze_to_silver.py` | `db_bronze.tb_fila_atendimento` | `db_silver.fila_atendimento` | Normalização upper/trim de nm_fila e ds_tipo_canal; `nr_sla_segundos` e `nr_sla_minutos` **não existem no Bronze** — criados como `NULL` na Silver (campos reservados para enriquecimento futuro); remoção de `nr_capacidade_max` e `fl_ativa` (colunas Bronze-only não mapeadas para Silver) |

### Domínio: Qualidade

| # | Nome do Job | Tabela Bronze (Origem) | Tabela Silver (Destino) | Principais Transformações |
|---|-------------|------------------------|-------------------------|---------------------------|
| 17 | `job_tb_avaliacao_qualidade_bronze_to_silver.py` | `db_bronze.tb_avaliacao_qualidade` | `db_silver.avaliacao_qualidade` | Cast de dt_avaliacao; cast e coalesce de nr_nota (0-10); coalesce de id_avaliador para -1; campo `nr_tamanho_feedback_chars`; remoção de `ds_feedback` (conteúdo textual removido na Silver); campo `ds_faixa_nota` com faixas: EXCELENTE (>=9.0), BOM (>=7.0), REGULAR (>=5.0), RUIM (>=3.0), CRITICO (<3.0); flags `fl_tem_feedback`, `fl_aprovado` (nota >= 7) e `fl_critico` (`nr_nota_geral < 5.0`); partição por ano/mes/dia de dt_avaliacao |
| 18 | `job_tb_metricas_operacionais_bronze_to_silver.py` | `db_bronze.tb_metricas_operacionais` | `db_silver.metricas_operacionais` | Cast de `dt_referencia` como DATE (`yyyy-MM-dd`, somente data — não datetime); cast e coalesce de contadores (nr_chamadas_recebidas, nr_chamadas_atendidas, nr_chamadas_abandonadas) para 0; cast de tempos em segundos (nr_tma_segundos, nr_tme_segundos); coalesce de id_fila para -1; `nr_nivel_servico` **não existe no Bronze** — calculado como `(nr_chamadas_atendidas * 100.0) / (nr_chamadas_atendidas + nr_chamadas_abandonadas)`; `nr_taxa_atendimento = 100 - nr_taxa_abandono`; campos `nr_tma_minutos` e `nr_tme_minutos`; flags `fl_meta_nivel_servico` e `fl_alto_abandono` |

---

## Convenções de Nomenclatura

| Prefixo | Significado |
|---------|-------------|
| `id_` | Chave natural (natural key) da entidade |
| `nm_` | Nome descritivo |
| `ds_` | Descrição ou string categórica |
| `nr_` | Valor numérico / quantidade |
| `dt_` | Data ou timestamp |
| `st_` | Status da entidade |
| `fl_` | Flag booleana (SMALLINT 0/1) |
| `tp_` | Tipo categórico |
| `op_cdc` | Operação CDC (INSERT/UPDATE/DELETE) |
| `hash_registro` | MD5 dos campos de negócio para controle de idempotência |
| `dt_ingestao_silver` | Timestamp de processamento na Silver |

## Recursos AWS Utilizados

- **AWS Glue 4.0** — runtime PySpark, G.1X / 2 workers
- **Glue Job Bookmarks** — controle de arquivos S3 já processados
- **Watermark em S3 (JSON)** — controle por timestamp CDC (`s3://{bucket}/checkpoints/{tabela}/watermark.json`)
- **Glue Data Catalog** — leitura via DynamicFrame com acesso ao metastore Hive
- **Apache Iceberg (formato versão 2)** — escrita com MERGE idempotente, compactação Snappy, target file size 128 MB
- **Amazon S3** — armazenamento dos dados Silver e quarentena
- **Amazon CloudWatch Logs** — monitoramento via `print()` no Glue
- **AWS Lake Formation** — controle de acesso aos dados PII no Bronze

---

## Notas de Implementação

### 1. Coluna CDC: normalização de case pelo Glue Data Catalog

O campo CDC de operação chega como `Op` (maiúsculo) nos arquivos Parquet gerados pelo DMS/Firehose. O Glue Data Catalog normaliza o nome da coluna para `op` (minúsculo) ao catalogar via Crawler. Todos os jobs usam `F.col("op")` (minúsculo) para garantir compatibilidade com o catálogo.

### 2. Bookmark vazio: `ColumnNotFoundException` em `_timestamp`

Quando o Glue Job Bookmark indica que todos os arquivos já foram processados (estado "sem novos dados"), o DynamicFrame retorna um schema vazio (zero colunas). Qualquer acesso subsequente a `F.col("_timestamp")` lança `ColumnNotFoundException`.

**Solução:** antes de reexecutar um job que já consumiu todos os dados, resetar o bookmark com:
```bash
aws glue reset-job-bookmark --job-name <nome-do-job>
```

### 3. Watermark: formato ISO 8601

O valor padrão de watermark para full load (primeira execução) é `"1970-01-01T00:00:00Z"` — formato ISO 8601 com timezone UTC explícito. Isso garante que o filtro `_timestamp > last_watermark` capture todos os registros históricos na primeira carga.
