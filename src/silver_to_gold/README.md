# Pipeline Silver → Gold

Jobs PySpark (AWS Glue 4.0) responsáveis pela construção do modelo dimensional (Star Schema) na camada Gold do Data Lakehouse Contact Center, a partir das tabelas limpas e enriquecidas da Silver.

---

## Padrão de Modelagem Dimensional

Todos os 22 jobs seguem o mesmo padrão arquitetural de Data Warehouse:

### 1. Surrogate Key (SK)

A geração de SK difere entre dimensões e fatos:

**Dimensões** — `row_number()` sobre uma Window ordenada pela natural key, produz INT determinístico:

```python
# Dimensões: SK sequencial e determinística
window_sk = Window.orderBy("nk_<entidade>")
df = df.withColumn("sk_<entidade>", F.row_number().over(window_sk))
```

- Tipo: **INT**, sequencial e determinístico
- Ordenado pela natural key → mesmo dataset produz mesmo SK
- Adequado para dimensões de baixo volume (sem risco de OOM)

**Fatos** — `monotonically_increasing_id()` distribuído, produz BIGINT sem shuffle global:

```python
# Fatos: SK distribuída sem shuffle global
df = df.withColumn("sk_<fato>", F.monotonically_increasing_id())
```

- Tipo: **BIGINT**, gerado por executor sem coleta em driver
- Não determinístico entre execuções, mas único e crescente dentro do batch
- O MERGE usa `nk_*` (natural key) como chave de join, tornando o SK estável

- A **natural key (nk_)** preserva o ID original da fonte para rastreabilidade
- O MERGE Iceberg é feito pela nk_ nos fatos (não pela SK)

### 2. Registro Default (sk = -1)

Toda dimensão possui um registro default com `sk = -1` para tratar NULL e dados ausentes nos fatos:

```
sk_<entidade> = -1  →  "Não Informado" / "Desconhecido"
```

Nas tabelas fato, qualquer FK que não encontre correspondência na dimensão recebe `sk = -1` via `coalesce(join_result, -1)`.

### 3. SCD Type 1 (Slowly Changing Dimension)

O modelo adota **SCD Tipo 1** — sobrescreve o registro atual sem manter histórico:

```sql
MERGE INTO glue_catalog.{GOLD_TABLE} AS target
USING stg_dim AS source
ON target.sk_<entidade> = source.sk_<entidade>

WHEN MATCHED AND target.hash_registro <> source.hash_registro
THEN UPDATE SET *

WHEN NOT MATCHED
THEN INSERT *
```

O `hash_registro` (MD5 dos atributos de negócio) evita re-escrita de dimensões que não mudaram.

### 4. MERGE Iceberg Idempotente

- Todas as tabelas Gold são Apache Iceberg formato versão 2
- Compressão Snappy, target file size 128 MB
- Particionamento por SK de data (`PARTITIONED BY (sk_data)` nas fatos)
- Dimensões sem particionamento explícito (baixo volume)

### 5. Fluxo de Execução

```
1. Ler tabelas Silver de entrada (filtro op_cdc != "DELETE")
2. Enriquecer / deduzir atributos Gold (faixas, flags, campos calculados)
3. Gerar surrogate key via dense_rank ou hash
4. Inserir registro default sk=-1 (idempotente, INSERT IF NOT EXISTS)
5. Resolver SKs das dimensões via LEFT JOIN (nk Silver → sk Gold)
6. Aplicar coalesce(sk, -1) para NULLs após join
7. MERGE Iceberg na tabela Gold
```

---

## Catálogo das 11 Dimensões

### dim_data

| Campo | Valor |
|-------|-------|
| **Job** | `job_dim_data_gold.py` |
| **Tabelas Silver de Entrada** | Nenhuma — gerada sinteticamente para um intervalo de datas configurável |
| **Tabela Gold de Saída** | `db_gold.dim_data` |
| **Surrogate Key** | `sk_data` (INT, gerado via dense_rank sobre `dt_completa`) |
| **Natural Key** | `dt_completa` (DATE) |
| **Atributos** | nr_ano, nr_mes, nr_dia, nr_trimestre, nr_semana_ano, ds_dia_semana, ds_mes, fl_fim_semana, fl_feriado |
| **Registro Default** | `sk_data = -1`, dt_completa = 1900-01-01, "Não Informado" |

---

### dim_canal

| Campo | Valor |
|-------|-------|
| **Job** | `job_dim_canal_gold.py` |
| **Tabelas Silver de Entrada** | Nenhuma — gerada sinteticamente a partir dos tipos de canal do sistema |
| **Tabela Gold de Saída** | `db_gold.dim_canal` |
| **Surrogate Key** | `sk_canal` (INT, valores fixos: 1=TELEFONE, 2=CHAT, 3=WHATSAPP, 4=EMAIL, -1=default) |
| **Natural Key** | `nm_canal` (STRING) |
| **Atributos** | nm_canal, ds_descricao, fl_digital |
| **Registro Default** | `sk_canal = -1`, nm_canal = "Não Informado" |

---

### dim_cliente

| Campo | Valor |
|-------|-------|
| **Job** | `job_dim_cliente_gold.py` |
| **Tabelas Silver de Entrada** | `db_silver.cliente` JOIN `db_silver.endereco_cliente` (LEFT JOIN por id_cliente) |
| **Tabela Gold de Saída** | `db_gold.dim_cliente` |
| **Surrogate Key** | `sk_cliente` (INT, dense_rank sobre `nk_cliente`) |
| **Natural Key** | `nk_cliente` = id_cliente da Silver |
| **Atributos** | nm_cliente, nr_documento_mascarado, ds_email_mascarado, nr_telefone_mascarado, st_cliente, fl_cliente_ativo, fl_tem_email, fl_tem_telefone, fl_tem_documento, ds_cidade, ds_estado, ds_pais |
| **Registro Default** | `sk_cliente = -1`, nm_cliente = "Não Informado" |

---

### dim_operador

| Campo | Valor |
|-------|-------|
| **Job** | `job_dim_operador_gold.py` |
| **Tabelas Silver de Entrada** | `db_silver.operador` (com self-join para resolver sk_supervisor) |
| **Tabela Gold de Saída** | `db_gold.dim_operador` |
| **Surrogate Key** | `sk_operador` (INT, dense_rank sobre `nk_operador`); `sk_supervisor` resolve a auto-referência via mapa nk_supervisor → sk_operador do supervisor |
| **Natural Key** | `nk_operador` = id_operador da Silver |
| **Atributos** | nm_operador, ds_email_mascarado, ds_login_mascarado, dt_admissao, st_operador, fl_operador_ativo, fl_tem_supervisor, nr_dias_casa, ds_faixa_tempo_casa, sk_supervisor |
| **Registro Default** | `sk_operador = -1`, nm_operador = "Não Informado" |

---

### dim_fila

| Campo | Valor |
|-------|-------|
| **Job** | `job_dim_fila_gold.py` |
| **Tabelas Silver de Entrada** | `db_silver.fila_atendimento` |
| **Tabela Gold de Saída** | `db_gold.dim_fila` |
| **Surrogate Key** | `sk_fila` (INT, dense_rank sobre `nk_fila`) |
| **Natural Key** | `nk_fila` = id_fila da Silver |
| **Atributos** | nm_fila, ds_tipo_canal, nr_sla_segundos, nr_sla_minutos |
| **Registro Default** | `sk_fila = -1`, nm_fila = "Não Informado" |

---

### dim_campanha

| Campo | Valor |
|-------|-------|
| **Job** | `job_dim_campanha_gold.py` |
| **Tabelas Silver de Entrada** | `db_silver.campanha` |
| **Tabela Gold de Saída** | `db_gold.dim_campanha` |
| **Surrogate Key** | `sk_campanha` (INT, dense_rank sobre `nk_campanha`) |
| **Natural Key** | `nk_campanha` = id_campanha da Silver |
| **Atributos** | nm_campanha, dt_inicio, dt_fim, st_campanha, nr_duracao_dias, fl_campanha_ativa, fl_campanha_vigente |
| **Registro Default** | `sk_campanha = -1`, nm_campanha = "Não Informado" |

---

### dim_skill

| Campo | Valor |
|-------|-------|
| **Job** | `job_dim_skill_gold.py` |
| **Tabelas Silver de Entrada** | `db_silver.skill_operador` |
| **Tabela Gold de Saída** | `db_gold.dim_skill` |
| **Surrogate Key** | `sk_skill` (INT, dense_rank sobre `ds_skill` + `nr_nivel`) |
| **Natural Key** | Combinação ds_skill + nr_nivel (sem NK numérica dedicada) |
| **Atributos** | ds_skill, nr_nivel, ds_faixa_nivel |
| **Registro Default** | `sk_skill = -1`, ds_skill = "Não Informado" |

---

### dim_status_chamada

| Campo | Valor |
|-------|-------|
| **Job** | `job_dim_status_chamada_gold.py` |
| **Tabelas Silver de Entrada** | `db_silver.chamada` (distinct de st_chamada) |
| **Tabela Gold de Saída** | `db_gold.dim_status_chamada` |
| **Surrogate Key** | `sk_status` (INT, gerado via dense_rank sobre `ds_status`) |
| **Natural Key** | `ds_status` = st_chamada normalizado |
| **Atributos** | ds_status, fl_chamada_concluida |
| **Registro Default** | `sk_status = -1`, ds_status = "Não Informado" |

---

### dim_status_ticket

| Campo | Valor |
|-------|-------|
| **Job** | `job_dim_status_ticket_gold.py` |
| **Tabelas Silver de Entrada** | `db_silver.ticket` (distinct de st_ticket) |
| **Tabela Gold de Saída** | `db_gold.dim_status_ticket` |
| **Surrogate Key** | `sk_status` (INT, gerado via dense_rank sobre `ds_status`) |
| **Natural Key** | `ds_status` = st_ticket normalizado |
| **Atributos** | ds_status, fl_ticket_aberto |
| **Registro Default** | `sk_status = -1`, ds_status = "Não Informado" |

---

### dim_categoria_ticket

| Campo | Valor |
|-------|-------|
| **Job** | `job_dim_categoria_ticket_gold.py` |
| **Tabelas Silver de Entrada** | `db_silver.ticket` (distinct de ds_categoria) |
| **Tabela Gold de Saída** | `db_gold.dim_categoria_ticket` |
| **Surrogate Key** | `sk_categoria` (INT, gerado via dense_rank sobre `nm_categoria`) |
| **Natural Key** | `nm_categoria` = ds_categoria normalizado |
| **Atributos** | nm_categoria |
| **Registro Default** | `sk_categoria = -1`, nm_categoria = "Não Informado" |

---

### dim_prioridade_ticket

| Campo | Valor |
|-------|-------|
| **Job** | `job_dim_prioridade_ticket_gold.py` |
| **Tabelas Silver de Entrada** | `db_silver.ticket` (distinct de ds_prioridade) |
| **Tabela Gold de Saída** | `db_gold.dim_prioridade_ticket` |
| **Surrogate Key** | `sk_prioridade` (INT, gerado via dense_rank sobre `nm_prioridade`) |
| **Natural Key** | `nm_prioridade` = ds_prioridade normalizado |
| **Atributos** | nm_prioridade, nr_ordem_prioridade (1=CRITICA, 2=ALTA, 3=MEDIA, 4=BAIXA) |
| **Registro Default** | `sk_prioridade = -1`, nm_prioridade = "Não Informado" |

---

## Catálogo das 11 Fatos

### fato_chamada

| Campo | Valor |
|-------|-------|
| **Job** | `job_fato_chamada_gold.py` |
| **Tabelas Silver de Entrada** | `db_silver.chamada` |
| **Dimensões Consultadas** | `dim_cliente`, `dim_operador`, `dim_fila`, `dim_canal`, `dim_status_chamada`, `dim_data` (2x: data_inicio e data_fim) |
| **Tabela Gold de Saída** | `db_gold.fato_chamada` |
| **SKs Geradas** | `sk_chamada` (PK da fato, `monotonically_increasing_id()`, BIGINT), `sk_cliente`, `sk_operador`, `sk_fila`, `sk_canal`, `sk_status`, `sk_data_inicio`, `sk_data_fim` |
| **Nota sk_canal** | `sk_canal` resolvido via lookup pontual `SELECT sk_canal FROM dim_canal WHERE nm_canal='TELEFONE'` — não via JOIN em `tp_chamada` (os valores ENTRADA/SAIDA do Bronze não casam com os valores do dim_canal) |
| **Natural Key** | `nk_chamada` = id_chamada |
| **Métricas** | nr_duracao_segundos, nr_duracao_minutos, fl_duracao_valida, fl_chamada_completa |
| **Partição** | sk_data_inicio |

---

### fato_chat

| Campo | Valor |
|-------|-------|
| **Job** | `job_fato_chat_gold.py` |
| **Tabelas Silver de Entrada** | `db_silver.chat` |
| **Dimensões Consultadas** | `dim_cliente`, `dim_operador`, `dim_canal` (sk_canal fixo=2/CHAT), `dim_data` |
| **Tabela Gold de Saída** | `db_gold.fato_chat` |
| **SKs Geradas** | `sk_chat` (PK da fato), `sk_cliente`, `sk_operador`, `sk_canal`, `sk_data` |
| **Natural Key** | `nk_chat` = id_chat |
| **Métricas** | nr_duracao_segundos, nr_duracao_minutos, fl_chat_completo |
| **Partição** | sk_data |

---

### fato_ticket

| Campo | Valor |
|-------|-------|
| **Job** | `job_fato_ticket_gold.py` |
| **Tabelas Silver de Entrada** | `db_silver.ticket` |
| **Dimensões Consultadas** | `dim_cliente`, `dim_operador`, `dim_status_ticket`, `dim_categoria_ticket`, `dim_prioridade_ticket`, `dim_data` (2x: data_abertura e data_fechamento) |
| **Tabela Gold de Saída** | `db_gold.fato_ticket` |
| **SKs Geradas** | `sk_ticket` (PK da fato), `sk_cliente`, `sk_operador_abertura`, `sk_status_ticket`, `sk_categoria`, `sk_prioridade`, `sk_data_abertura`, `sk_data_fechamento` |
| **Natural Key** | `nk_ticket` = id_ticket |
| **Métricas** | nr_tempo_resolucao_min, fl_ticket_resolvido, fl_dentro_sla |
| **Partição** | sk_data_abertura |

---

### fato_discagem

| Campo | Valor |
|-------|-------|
| **Job** | `job_fato_discagem_gold.py` |
| **Tabelas Silver de Entrada** | `db_silver.discagem` |
| **Dimensões Consultadas** | `dim_campanha`, `dim_cliente`, `dim_data` |
| **Tabela Gold de Saída** | `db_gold.fato_discagem` |
| **SKs Geradas** | `sk_discagem` (PK da fato), `sk_campanha`, `sk_cliente`, `sk_data` |
| **Natural Key** | `nk_discagem` = id_discagem |
| **Métricas** | fl_discagem_atendida, fl_discagem_nao_atendida, nr_telefone_mascarado |
| **Partição** | sk_data |

---

### fato_jornada_operador

| Campo | Valor |
|-------|-------|
| **Job** | `job_fato_jornada_operador_gold.py` |
| **Tabelas Silver de Entrada** | `db_silver.jornada_operador` |
| **Dimensões Consultadas** | `dim_operador`, `dim_data` (2x: data_inicio e data_fim do turno) |
| **Tabela Gold de Saída** | `db_gold.fato_jornada_operador` |
| **SKs Geradas** | `sk_jornada` (PK da fato), `sk_operador`, `sk_data_inicio`, `sk_data_fim` |
| **Natural Key** | `nk_jornada` = id_jornada |
| **Métricas** | nr_duracao_turno_min, nr_tempo_pausa_min, nr_duracao_produtiva_min, fl_turno_completo, fl_turno_normal, ds_turno |
| **Partição** | sk_data_inicio |

---

### fato_mensagem_chat

| Campo | Valor |
|-------|-------|
| **Job** | `job_fato_mensagem_chat_gold.py` |
| **Tabelas Silver de Entrada** | `db_silver.mensagem_chat` |
| **Dimensões Consultadas** | `fato_chat` (para resolução de sk_chat via nk_chat), `dim_data` |
| **Tabela Gold de Saída** | `db_gold.fato_mensagem_chat` |
| **SKs Geradas** | `sk_mensagem` (PK da fato), `sk_chat` (FK para fato_chat, BIGINT — herdado de fato_chat.sk_chat), `sk_data` |
| **Natural Key** | `nk_mensagem` = id_mensagem |
| **Métricas** | nr_tamanho_chars, fl_mensagem_cliente, fl_mensagem_operador, ds_remetente |
| **Partição** | sk_data |

---

### fato_interacao_ticket

| Campo | Valor |
|-------|-------|
| **Job** | `job_fato_interacao_ticket_gold.py` |
| **Tabelas Silver de Entrada** | `db_silver.interacao_ticket` |
| **Dimensões Consultadas** | `fato_ticket` (para resolução de sk_ticket via nk_ticket), `dim_operador`, `dim_data` |
| **Tabela Gold de Saída** | `db_gold.fato_interacao_ticket` |
| **SKs Geradas** | `sk_interacao` (PK da fato), `sk_ticket` (FK para fato_ticket, BIGINT — herdado de fato_ticket.sk_ticket), `sk_operador`, `sk_data` |
| **Natural Key** | `nk_interacao` = id_interacao |
| **Métricas** | nr_tamanho_observacao_chars, fl_tem_observacao, ds_canal |
| **Partição** | sk_data |

---

### fato_metricas_operacionais

| Campo | Valor |
|-------|-------|
| **Job** | `job_fato_metricas_operacionais_gold.py` |
| **Tabelas Silver de Entrada** | `db_silver.metricas_operacionais` |
| **Dimensões Consultadas** | `dim_fila`, `dim_data` |
| **Tabela Gold de Saída** | `db_gold.fato_metricas_operacionais` |
| **SKs Geradas** | `sk_metrica` (PK da fato), `sk_fila`, `sk_data` |
| **Natural Key** | `nk_metrica` = id_metrica |
| **Métricas** | nr_chamadas_recebidas, nr_chamadas_atendidas, nr_chamadas_abandonadas, nr_tma_segundos, nr_tma_minutos, nr_tme_segundos, nr_tme_minutos, nr_nivel_servico, nr_taxa_atendimento, nr_taxa_abandono, fl_meta_nivel_servico, fl_alto_abandono |
| **Partição** | sk_data |

---

### fato_qualidade

| Campo | Valor |
|-------|-------|
| **Job** | `job_fato_qualidade_gold.py` |
| **Tabelas Silver de Entrada** | `db_silver.avaliacao_qualidade` |
| **Dimensões Consultadas** | `fato_chamada` (para resolução de sk_chamada e sk_operador_avaliado), `dim_operador` (para sk_avaliador), `dim_data` |
| **Tabela Gold de Saída** | `db_gold.fato_qualidade` |
| **SKs Geradas** | `sk_avaliacao` (PK da fato), `sk_chamada` (FK para fato_chamada, BIGINT — herdado de fato_chamada.sk_chamada), `sk_operador_avaliado`, `sk_avaliador`, `sk_data` |
| **Natural Key** | `nk_avaliacao` = id_avaliacao |
| **Métricas** | nr_nota, ds_faixa_nota, nr_tamanho_feedback_chars, fl_tem_feedback, fl_aprovado, fl_critico |
| **Partição** | sk_data |

---

### fato_ura_navegacao

| Campo | Valor |
|-------|-------|
| **Job** | `job_fato_ura_navegacao_gold.py` |
| **Tabelas Silver de Entrada** | `db_silver.ura_navegacao` |
| **Dimensões Consultadas** | `fato_chamada` (para resolução de sk_chamada via nk_chamada), `dim_data` |
| **Tabela Gold de Saída** | `db_gold.fato_ura_navegacao` |
| **SKs Geradas** | `sk_ura` (PK da fato), `sk_chamada` (FK para fato_chamada), `sk_data` |
| **Natural Key** | `nk_ura` = id_ura |
| **Métricas** | ds_opcao, nr_tempo_espera, fl_abandonou_ura, ds_faixa_espera |
| **Partição** | sk_data |

---

### fato_whatsapp

| Campo | Valor |
|-------|-------|
| **Job** | `job_fato_whatsapp_gold.py` |
| **Tabelas Silver de Entrada** | `db_silver.whatsapp_atendimento` |
| **Dimensões Consultadas** | `dim_cliente`, `dim_operador`, `dim_canal` (sk_canal fixo=3/WHATSAPP), `dim_data` |
| **Tabela Gold de Saída** | `db_gold.fato_whatsapp` |
| **SKs Geradas** | `sk_whatsapp` (PK da fato), `sk_cliente`, `sk_operador`, `sk_canal`, `sk_data` |
| **Natural Key** | `nk_whatsapp` = id_whatsapp |
| **Métricas** | nr_duracao_segundos, nr_duracao_minutos, fl_atendimento_completo, nr_telefone_mascarado |
| **Partição** | sk_data |

---

## Resumo do Star Schema

```
                    dim_data
                       |
dim_cliente ──── fato_chamada ──── dim_operador
                       |
                    dim_fila
                       |
               dim_status_chamada
                       |
                    dim_canal

                    dim_data
                       |
dim_cliente ──── fato_ticket ──── dim_operador
                       |
               dim_status_ticket
                       |
             dim_categoria_ticket
                       |
             dim_prioridade_ticket

              dim_campanha
                   |
dim_cliente ─ fato_discagem ─ dim_data

dim_operador ─ fato_jornada_operador ─ dim_data

fato_chat ──── fato_mensagem_chat ─── dim_data
fato_ticket ─ fato_interacao_ticket ─ dim_data
fato_chamada ─ fato_qualidade ─────── dim_data
fato_chamada ─ fato_ura_navegacao ─── dim_data

dim_fila ──── fato_metricas_operacionais ─── dim_data
dim_cliente ─ fato_whatsapp ──────────────── dim_data ─ dim_operador
```

## Convenções de Nomenclatura Gold

| Prefixo | Significado |
|---------|-------------|
| `sk_` | Surrogate Key (INT, gerada pelo pipeline Gold) |
| `nk_` | Natural Key (preserva o ID original da Silver/fonte) |
| `dim_` | Tabela de dimensão |
| `fato_` | Tabela de fato |
| `nr_` | Métrica numérica |
| `fl_` | Flag booleana (SMALLINT 0/1) |
| `ds_` | Descrição categórica |
| `dt_` | Data ou timestamp |

## Recursos AWS Utilizados

- **AWS Glue 4.0** — runtime PySpark, G.1X / 2 workers
- **Apache Iceberg (formato versão 2)** — MERGE idempotente com SCD Type 1, compactação Snappy
- **Glue Data Catalog** — acesso unificado às tabelas Silver e Gold via `glue_catalog`
- **Amazon S3** — armazenamento das tabelas Gold em `s3://{bucket}/gold/`
- **Amazon Athena** — consulta analítica sobre as tabelas Gold via Iceberg
- **Amazon CloudWatch Logs** — monitoramento via `print()` no Glue

---

## Bugs Corrigidos em Execução

Problemas encontrados e corrigidos durante a execução de referência (2026-07-10/11):

### 1. `createDataFrame` com colunas de data `None`

Jobs afetados: `dim_cliente`, `dim_operador`, `dim_campanha`, `dim_data`

Colunas como `dt_cadastro`, `dt_admissao`, `dt_completa`, `dt_inicio`/`dt_fim` retornavam `None` quando passadas via `createDataFrame` sem schema explícito — o Spark inferia o tipo como `StringType` ou `NullType`, quebrando o MERGE Iceberg.

**Correção:** substituído `spark.createDataFrame(rows, schema)` por `spark.sql()` com tipos explícitos no DDL e literal `CAST(NULL AS DATE)` para os campos ausentes.

### 2. `sk_chamada`/`sk_ticket`/`sk_chat` definidos como INT nos fatos dependentes

Fatos dependentes (`fato_qualidade`, `fato_interacao_ticket`, `fato_mensagem_chat`, `fato_ura_navegacao`) tinham `sk_chamada`/`sk_ticket`/`sk_chat` definidos como `INT` no `CREATE TABLE` e no `coalesce(..., -1)`, mas os valores herdados dos fatos pai são `BIGINT` (gerados por `monotonically_increasing_id()`).

**Correção:** tipo alterado para `BIGINT` no `CREATE TABLE IF NOT EXISTS` e o literal de fallback atualizado para `F.lit(-1).cast("bigint")` no coalesce.

### 3. `fato_chamada`: JOIN `tp_chamada == nm_canal` nunca casava

O campo `tp_chamada` na Silver contém valores `ENTRADA`/`SAIDA` (tipo de direção da chamada), mas `dim_canal.nm_canal` contém `TELEFONE`/`CHAT`/`WHATSAPP`/`EMAIL`. O JOIN `ON tp_chamada = nm_canal` nunca produzia match, resultando em `sk_canal = -1` para todos os registros.

**Correção:** substituído por lookup pontual fixo — `sk_canal` obtido via `SELECT sk_canal FROM dim_canal WHERE nm_canal='TELEFONE'` e aplicado como literal, já que `fato_chamada` é exclusivamente canal de voz.

### 4. `job-fato-metricas-operacionais-gold` sem extensões Iceberg

O job foi criado sem a propriedade `--conf spark.sql.extensions=IcebergSparkSessionExtensions`, fazendo com que o `spark.sql("MERGE INTO ...")` falhasse com `ParseException`.

**Correção:** atualizado via `aws glue update-job` adicionando o conf Iceberg nas propriedades do job.

### 5. `dim_data` sem parâmetros `--DT_INICIO`/`--DT_FIM`

O job `job_dim_data_gold.py` requer os argumentos `--DT_INICIO` e `--DT_FIM` para definir o intervalo de geração de datas. Sem os defaults configurados no Glue, o job falhava com `KeyError`.

**Correção:** adicionados como default args via `aws glue update-job`:
- `--DT_INICIO`: `2015-01-01`
- `--DT_FIM`: `2030-12-31`
