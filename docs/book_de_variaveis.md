# Book de Variáveis — Contact Center Data Lakehouse

Documento centralizado que cataloga, descreve e governa as variáveis analíticas das camadas **Silver** e **Gold** do pipeline. Serve como referência para engenheiros de dados, analistas e cientistas de dados que consomem as tabelas via Athena, Redshift ou notebooks.

> **Como ler este documento**
> Cada variável é apresentada com: definição técnica, origem e lineage, regras de qualidade, governança e exemplos de valores. As variáveis estão organizadas por domínio de negócio, seguindo a mesma divisão adotada nas tabelas Gold.

---

## Índice

- [Convenções de Nomenclatura](#convenções-de-nomenclatura)
- [Domínio: Chamadas (Voz)](#domínio-chamadas-voz)
- [Domínio: Qualidade de Atendimento](#domínio-qualidade-de-atendimento)
- [Domínio: Métricas Operacionais de Fila](#domínio-métricas-operacionais-de-fila)
- [Domínio: Tickets de Suporte](#domínio-tickets-de-suporte)
- [Domínio: URA / IVR](#domínio-ura--ivr)
- [Domínio: Jornada do Operador](#domínio-jornada-do-operador)
- [Domínio: Dimensão Data](#domínio-dimensão-data)
- [Domínio: Dimensão Operador](#domínio-dimensão-operador)
- [Lineage Cross-Layer](#lineage-cross-layer)
- [Matriz de Governança](#matriz-de-governança)

---

## Convenções de Nomenclatura

| Prefixo | Tipo | Camada |
|---|---|---|
| `sk_` | Surrogate Key — gerada pelo pipeline Gold | Gold |
| `nk_` | Natural Key — vinda do sistema fonte | Silver / Gold |
| `id_` | Identificador técnico | Bronze / Silver |
| `nm_` | Nome / label descritivo | Silver / Gold |
| `ds_` | Descrição textual ou categórica | Silver / Gold |
| `nr_` | Valor numérico, quantidade ou taxa | Silver / Gold |
| `dt_` | Data ou timestamp | Todas |
| `st_` | Status (texto livre normalizado) | Silver |
| `fl_` | Flag booleano — SMALLINT 0 (falso) / 1 (verdadeiro) | Silver / Gold |
| `tp_` | Tipo / categoria | Silver |

> Flags `fl_` são sempre `SMALLINT` (0/1) em vez de `BOOLEAN` para compatibilidade com Redshift Serverless e Athena.

---

## Domínio: Chamadas (Voz)

**Tabela Gold:** `fato_chamada`
**Tabela Silver de origem:** `db_silver.chamada`
**Job Silver:** `job_tb_chamada_bronze_to_silver.py`
**Job Gold:** `job_fato_chamada.py`
**Frequência de atualização:** Diária (DAG `cc_pipeline_diario`, schedule `0 2 * * *`)

---

### `nr_duracao_segundos`

| Atributo | Valor |
|---|---|
| **Descrição** | Duração total da chamada em segundos, calculada como diferença entre `dt_fim` e `dt_inicio` |
| **Tipo de dado** | `INT` |
| **Unidade** | segundos |
| **Intervalo** | 0 – 86.400 (máx. 24 h) |
| **Campo Silver de origem** | `nr_duracao_segundos` (cast do Bronze) |
| **Transformação** | Cast de `dt_fim - dt_inicio` em segundos na Silver; propagado diretamente ao Gold |
| **Completude esperada** | 95% |
| **Nulos permitidos** | Sim (chamada sem `dt_fim` → duração nula) |
| **Validação** | `nr_duracao_segundos >= 0` |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Alta |
| **Exemplos** | 0, 45, 120, 900, 1.800 |

---

### `nr_duracao_minutos`

| Atributo | Valor |
|---|---|
| **Descrição** | Duração total da chamada em minutos — versão derivada de `nr_duracao_segundos` para conveniência analítica |
| **Tipo de dado** | `DOUBLE` |
| **Unidade** | minutos |
| **Intervalo** | 0,0 – 1.440,0 |
| **Campo Silver de origem** | `nr_duracao_minutos` = `nr_duracao_segundos / 60.0` |
| **Transformação** | Divisão por 60 na camada Silver; propagado ao Gold |
| **Completude esperada** | 95% |
| **Nulos permitidos** | Sim |
| **Validação** | `nr_duracao_minutos >= 0` |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Alta |
| **Exemplos** | 0,0; 0,75; 2,0; 15,0; 30,0 |

---

### `fl_duracao_valida`

| Atributo | Valor |
|---|---|
| **Descrição** | Indica se a duração da chamada é válida para fins de cálculo de TMA (Tempo Médio de Atendimento). Chamadas com duração nula ou zerada são excluídas do TMA |
| **Tipo de dado** | `SMALLINT` |
| **Valores possíveis** | 0 (duração inválida), 1 (duração válida) |
| **Campo Silver de origem** | `fl_duracao_valida` |
| **Transformação** | `CASE WHEN nr_duracao_segundos > 0 AND nr_duracao_segundos IS NOT NULL THEN 1 ELSE 0 END` |
| **Completude esperada** | 100% |
| **Nulos permitidos** | Não |
| **Validação** | Valor deve ser 0 ou 1 |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Alta |
| **Uso analítico** | Filtro em queries de TMA: `WHERE fl_duracao_valida = 1` |
| **Exemplos** | 1 (chamada com duração > 0), 0 (chamada sem `dt_fim`) |

---

### `fl_chamada_completa`

| Atributo | Valor |
|---|---|
| **Descrição** | Indica se a chamada possui tanto `dt_inicio` quanto `dt_fim` preenchidos — requisito para ser considerada "atendida" nos KPIs de volume |
| **Tipo de dado** | `SMALLINT` |
| **Valores possíveis** | 0 (incompleta / abandonada), 1 (completa / atendida) |
| **Campo Silver de origem** | `fl_chamada_completa` |
| **Transformação** | `CASE WHEN dt_inicio IS NOT NULL AND dt_fim IS NOT NULL THEN 1 ELSE 0 END` |
| **Completude esperada** | 100% |
| **Nulos permitidos** | Não |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Alta |
| **Exemplos** | 1 (atendida), 0 (abandonada antes do atendimento) |

---

## Domínio: Qualidade de Atendimento

**Tabela Gold:** `fato_qualidade`
**Tabela Silver de origem:** `db_silver.avaliacao_qualidade`
**Job Silver:** `job_tb_avaliacao_qualidade_bronze_to_silver.py`
**Job Gold:** `job_fato_qualidade.py`
**Frequência de atualização:** Diária

---

### `nr_nota`

| Atributo | Valor |
|---|---|
| **Descrição** | Nota de monitoria de qualidade atribuída por um supervisor ao atendimento do operador. Escala de 0 a 10 |
| **Tipo de dado** | `DOUBLE` |
| **Unidade** | pontos |
| **Intervalo** | 0,0 – 10,0 |
| **Campo Silver de origem** | `nr_nota` (cast + coalesce para 0 no Bronze) |
| **Transformação** | Cast para DOUBLE; coalesce para 0 quando nulo no Bronze |
| **Completude esperada** | 100% |
| **Nulos permitidos** | Não (coalesce garante 0 mínimo) |
| **Validação** | `nr_nota BETWEEN 0 AND 10` |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Crítica |
| **Distribuição esperada** | Normal, média ~7,0, desvio padrão ~2,5 |
| **Exemplos** | 1,49; 3,52; 5,49; 7,47; 9,49 |

---

### `ds_faixa_nota`

| Atributo | Valor |
|---|---|
| **Descrição** | Categorização textual da nota de qualidade em faixas de desempenho, facilitando agrupamentos e visualizações em BI |
| **Tipo de dado** | `STRING` |
| **Valores possíveis** | `CRITICO`, `RUIM`, `REGULAR`, `BOM`, `EXCELENTE` |
| **Regra de classificação** | EXCELENTE ≥ 9,0 \| BOM ≥ 7,0 \| REGULAR ≥ 5,0 \| RUIM ≥ 3,0 \| CRITICO < 3,0 |
| **Campo Silver de origem** | `ds_faixa_nota` |
| **Transformação** | `CASE WHEN nr_nota >= 9 THEN 'EXCELENTE' WHEN nr_nota >= 7 THEN 'BOM' ...` na Silver |
| **Completude esperada** | 100% |
| **Nulos permitidos** | Não |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Alta |
| **Exemplos** | 'EXCELENTE' (nota 9,49), 'BOM' (nota 7,47), 'CRITICO' (nota 1,49) |

---

### `fl_aprovado`

| Atributo | Valor |
|---|---|
| **Descrição** | Indica se o operador foi aprovado na avaliação de qualidade (nota ≥ 7,0) |
| **Tipo de dado** | `SMALLINT` |
| **Valores possíveis** | 0 (reprovado), 1 (aprovado) |
| **Regra** | `fl_aprovado = 1` se `nr_nota >= 7.0` |
| **Campo Silver de origem** | `fl_aprovado` |
| **Completude esperada** | 100% |
| **Nulos permitidos** | Não |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Alta |
| **Uso analítico** | `vw_kpi_03_qualidade`: `pct_aprovacao = SUM(fl_aprovado) * 100.0 / COUNT(*)` |
| **Exemplos** | 1 (nota 8,2), 0 (nota 6,8) |

---

### `fl_critico`

| Atributo | Valor |
|---|---|
| **Descrição** | Indica se o atendimento recebeu nota crítica (< 5,0), exigindo ação imediata de coaching pelo supervisor |
| **Tipo de dado** | `SMALLINT` |
| **Valores possíveis** | 0 (não crítico), 1 (crítico) |
| **Regra** | `fl_critico = 1` se `nr_nota < 5.0` |
| **Campo Silver de origem** | `fl_critico` |
| **Completude esperada** | 100% |
| **Nulos permitidos** | Não |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Crítica |
| **Uso analítico** | Alertas operacionais; dashboard de supervisão; filtro para coaching prioritário |
| **Exemplos** | 1 (nota 2,3), 0 (nota 7,1) |

---

## Domínio: Métricas Operacionais de Fila

**Tabela Gold:** `fato_metricas_operacionais`
**Tabela Silver de origem:** `db_silver.metricas_operacionais`
**Job Silver:** `job_tb_metricas_operacionais_bronze_to_silver.py`
**Job Gold:** `job_fato_metricas_operacionais.py`
**Frequência de atualização:** Diária
**Nota:** `nr_nivel_servico` **não existe no Bronze** — calculado na Silver a partir de chamadas atendidas / (atendidas + abandonadas).

---

### `nr_taxa_atendimento`

| Atributo | Valor |
|---|---|
| **Descrição** | Percentual de chamadas recebidas que foram efetivamente atendidas por um operador. Complementar a `nr_taxa_abandono` |
| **Tipo de dado** | `DOUBLE` |
| **Unidade** | % (0–100) |
| **Intervalo** | 0,0 – 100,0 |
| **Campo Silver de origem** | `nr_taxa_atendimento` = `100 - nr_taxa_abandono` |
| **Transformação** | Calculado na Silver: `(nr_chamadas_atendidas * 100.0) / nr_chamadas_recebidas` → `100 - resultado` |
| **Completude esperada** | 95% |
| **Nulos permitidos** | Sim (divisão por zero quando `nr_chamadas_recebidas = 0`) |
| **Validação** | `nr_taxa_atendimento BETWEEN 0 AND 100` |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Crítica |
| **Meta esperada** | ≥ 80% (parâmetro de negócio configurável) |
| **Exemplos** | 86,4; 91,2; 94,7 |

---

### `nr_taxa_abandono`

| Atributo | Valor |
|---|---|
| **Descrição** | Percentual de chamadas recebidas que foram abandonadas pelo cliente antes do atendimento. KPI crítico para dimensionamento de equipe |
| **Tipo de dado** | `DOUBLE` |
| **Unidade** | % (0–100) |
| **Intervalo** | 0,0 – 100,0 |
| **Campo Silver de origem** | `nr_taxa_abandono` = `(nr_chamadas_abandonadas * 100.0) / nr_chamadas_recebidas` |
| **Transformação** | Calculado na Silver; coalesce para 0 quando denominador nulo |
| **Completude esperada** | 95% |
| **Nulos permitidos** | Sim |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Crítica |
| **Correlação** | Alta correlação negativa com `nr_nivel_servico` |
| **Exemplos** | 5,3; 8,8; 13,6 |

---

### `nr_nivel_servico`

| Atributo | Valor |
|---|---|
| **Descrição** | Nível de serviço da fila de atendimento — percentual de chamadas atendidas em relação ao total de chamadas que entraram na fila (atendidas + abandonadas). Campo **não existe no Bronze**, calculado integralmente na Silver |
| **Tipo de dado** | `DOUBLE` |
| **Unidade** | % (0–100) |
| **Intervalo** | 0,0 – 100,0 |
| **Campo Bronze de origem** | Não existe — campo derivado |
| **Transformação** | `(nr_chamadas_atendidas * 100.0) / (nr_chamadas_atendidas + nr_chamadas_abandonadas)` na Silver |
| **Completude esperada** | 95% |
| **Nulos permitidos** | Sim (quando denominador = 0) |
| **Validação** | `nr_nivel_servico BETWEEN 0 AND 100` |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Crítica |
| **Meta SLA** | ≥ 80% (padrão Contact Center — configurável por fila) |
| **Uso analítico** | `vw_kpi_09_metricas_fila`; `fl_meta_nivel_servico` |
| **Exemplos** | 86,3; 90,1; 92,8 |

---

### `nr_tma_segundos` / `nr_tma_minutos`

| Atributo | Valor |
|---|---|
| **Descrição** | TMA — Tempo Médio de Atendimento. Duração média das chamadas atendidas em uma fila no período, em segundos e minutos |
| **Tipo de dado** | `INT` (segundos) / `DOUBLE` (minutos) |
| **Unidade** | segundos / minutos |
| **Intervalo** | 0 – 86.400 (segundos); 0,0 – 1.440,0 (minutos) |
| **Campo Silver de origem** | `nr_tma_segundos` (cast do Bronze) → `nr_tma_minutos = nr_tma_segundos / 60.0` |
| **Transformação** | Cast para INT na Silver; minutos calculados por divisão |
| **Completude esperada** | 90% |
| **Nulos permitidos** | Sim (fila sem chamadas atendidas no período) |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Alta |
| **Exemplos** | nr_tma_segundos: 900; nr_tma_minutos: 15,0 |

---

### `nr_tme_segundos` / `nr_tme_minutos`

| Atributo | Valor |
|---|---|
| **Descrição** | TME — Tempo Médio de Espera. Tempo médio que os clientes aguardaram na fila antes de serem atendidos ou abandonarem |
| **Tipo de dado** | `INT` (segundos) / `DOUBLE` (minutos) |
| **Unidade** | segundos / minutos |
| **Campo Silver de origem** | `nr_tme_segundos` (cast do Bronze) → `nr_tme_minutos = nr_tme_segundos / 60.0` |
| **Completude esperada** | 90% |
| **Nulos permitidos** | Sim |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Alta |
| **Exemplos** | nr_tme_segundos: 60; nr_tme_minutos: 1,0 |

---

### `fl_meta_nivel_servico`

| Atributo | Valor |
|---|---|
| **Descrição** | Indica se a fila atingiu a meta de nível de serviço (≥ 80%) no dia de referência |
| **Tipo de dado** | `SMALLINT` |
| **Valores possíveis** | 0 (abaixo da meta), 1 (meta atingida) |
| **Regra** | `fl_meta_nivel_servico = 1` se `nr_nivel_servico >= 80` |
| **Completude esperada** | 100% |
| **Nulos permitidos** | Não |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Crítica |
| **Uso analítico** | Alertas diários de operação; relatório de compliance de SLA por fila |
| **Exemplos** | 1 (nível 92%), 0 (nível 67%) |

---

### `fl_alto_abandono`

| Atributo | Valor |
|---|---|
| **Descrição** | Sinaliza se a taxa de abandono da fila no dia superou o limiar de alerta (> 15%) |
| **Tipo de dado** | `SMALLINT` |
| **Valores possíveis** | 0 (abandono normal), 1 (abandono elevado) |
| **Regra** | `fl_alto_abandono = 1` se `nr_taxa_abandono > 15` |
| **Completude esperada** | 100% |
| **Nulos permitidos** | Não |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Alta |
| **Uso analítico** | Dashboard operacional em tempo real; acionamento de escalation |
| **Exemplos** | 1 (abandono 18,3%), 0 (abandono 7,1%) |

---

## Domínio: Tickets de Suporte

**Tabela Gold:** `fato_ticket`
**Tabela Silver de origem:** `db_silver.ticket`
**Job Silver:** `job_tb_ticket_bronze_to_silver.py`
**Job Gold:** `job_fato_ticket.py`
**Frequência de atualização:** Diária

---

### `nr_tempo_resolucao_min`

| Atributo | Valor |
|---|---|
| **Descrição** | Tempo total de resolução do ticket, em minutos, calculado como diferença entre `dt_fechamento` e `dt_abertura`. Utilizado para cálculo do MTTR (Mean Time to Resolution) |
| **Tipo de dado** | `DOUBLE` |
| **Unidade** | minutos |
| **Intervalo** | 0,0 – ∞ (sem limite superior — tickets podem permanecer abertos por meses) |
| **Campo Silver de origem** | `nr_tempo_resolucao_min` = `datediff(dt_fechamento, dt_abertura) em minutos` |
| **Transformação** | Datediff calculado na Silver; nulo quando `dt_fechamento IS NULL` (ticket em aberto) |
| **Completude esperada** | 70% (tickets abertos não têm valor) |
| **Nulos permitidos** | Sim (ticket não resolvido) |
| **Validação** | `nr_tempo_resolucao_min >= 0` |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Alta |
| **Distribuição esperada** | Lognormal (maioria resolve em poucas horas; outliers em dias) |
| **Exemplos** | 222 min (3,7 h — prioridade CRÍTICA); 564 min (9,4 h — prioridade BAIXA) |

---

### `fl_ticket_resolvido`

| Atributo | Valor |
|---|---|
| **Descrição** | Indica se o ticket foi resolvido/fechado, independentemente de ter cumprido o SLA |
| **Tipo de dado** | `SMALLINT` |
| **Valores possíveis** | 0 (aberto/em andamento), 1 (resolvido) |
| **Regra** | `fl_ticket_resolvido = 1` se `st_ticket IN ('RESOLVIDO', 'FECHADO')` |
| **Completude esperada** | 100% |
| **Nulos permitidos** | Não |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Alta |
| **Exemplos** | 1 (st_ticket = 'RESOLVIDO'), 0 (st_ticket = 'ABERTO') |

---

### `fl_dentro_sla`

| Atributo | Valor |
|---|---|
| **Descrição** | Indica se o ticket foi resolvido dentro do SLA definido (480 minutos / 8 horas para a configuração padrão) |
| **Tipo de dado** | `SMALLINT` |
| **Valores possíveis** | 0 (SLA violado ou ticket em aberto), 1 (SLA cumprido) |
| **Regra** | `fl_dentro_sla = 1` se `fl_ticket_resolvido = 1 AND nr_tempo_resolucao_min <= 480` |
| **Parâmetro SLA** | 480 minutos (configurável por categoria/prioridade) |
| **Completude esperada** | 100% |
| **Nulos permitidos** | Não |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Crítica |
| **Uso analítico** | `vw_kpi_05_eficiencia_tickets`: `pct_sla = SUM(fl_dentro_sla) * 100.0 / COUNT(*)` |
| **Exemplos** | 1 (resolvido em 4 h), 0 (resolvido em 12 h) |

---

## Domínio: URA / IVR

**Tabela Gold:** `fato_ura_navegacao`
**Tabela Silver de origem:** `db_silver.ura_navegacao`
**Job Silver:** `job_tb_ura_navegacao_bronze_to_silver.py`
**Job Gold:** `job_fato_ura_navegacao.py`
**Frequência de atualização:** Diária

---

### `ds_faixa_espera`

| Atributo | Valor |
|---|---|
| **Descrição** | Categorização do tempo de espera do cliente na URA antes de selecionar uma opção ou abandonar |
| **Tipo de dado** | `STRING` |
| **Valores possíveis** | `IMEDIATO`, `CURTO`, `MEDIO`, `LONGO` |
| **Regra de classificação** | IMEDIATO: 0 s \| CURTO: 1–30 s \| MEDIO: 31–120 s \| LONGO: > 120 s |
| **Campo Silver de origem** | `ds_faixa_espera` |
| **Transformação** | `CASE WHEN nr_tempo_espera = 0 THEN 'IMEDIATO' WHEN nr_tempo_espera <= 30 THEN 'CURTO' ...` |
| **Completude esperada** | 100% |
| **Nulos permitidos** | Não |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Média |
| **Uso analítico** | Otimização do fluxo de URA; identificação de gargalos por opção de menu |
| **Exemplos** | 'CURTO' (15 s), 'LONGO' (180 s) |

---

### `fl_abandonou_ura`

| Atributo | Valor |
|---|---|
| **Descrição** | Indica se o cliente desligou ou abandonou durante a navegação na URA, sem chegar a um operador |
| **Tipo de dado** | `SMALLINT` |
| **Valores possíveis** | 0 (navegou / transferido), 1 (abandonou) |
| **Campo Silver de origem** | `fl_abandonou_ura` (cast do Bronze) |
| **Completude esperada** | 100% |
| **Nulos permitidos** | Não |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Alta |
| **Uso analítico** | `vw_kpi_12_ura`: taxa de abandono por opção de menu |
| **Exemplos** | 0 (selecionou opção e foi transferido), 1 (desligou sem interagir) |

---

## Domínio: Jornada do Operador

**Tabela Gold:** `fato_jornada_operador`
**Tabela Silver de origem:** `db_silver.jornada_operador`
**Job Silver:** `job_tb_jornada_operador_bronze_to_silver.py`
**Job Gold:** `job_fato_jornada_operador.py`
**Frequência de atualização:** Diária

---

### `nr_horas_trabalhadas`

| Atributo | Valor |
|---|---|
| **Descrição** | Total de horas efetivamente trabalhadas pelo operador no dia de referência, descontado o tempo de pausa |
| **Tipo de dado** | `DOUBLE` |
| **Unidade** | horas |
| **Intervalo** | 0,0 – 12,0 (limite prático por legislação trabalhista) |
| **Campo Silver de origem** | `nr_horas_trabalhadas` = `nr_duracao_produtiva_min / 60.0` |
| **Transformação** | `nr_duracao_produtiva_min = nr_duracao_turno_min - nr_tempo_pausa_min` na Silver; divisão por 60 |
| **Completude esperada** | 100% |
| **Nulos permitidos** | Não (coalesce garante 0 mínimo) |
| **Validação** | `nr_horas_trabalhadas BETWEEN 0 AND 12` |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Alta |
| **Exemplos** | 4,0; 6,5; 8,0 |

---

### `fl_presente`

| Atributo | Valor |
|---|---|
| **Descrição** | Indica se o operador estava presente (fez login) no dia de referência. Registros ausentes não são gerados; `fl_presente = 0` ocorre quando o registro de jornada existe mas o operador não cumpriu o turno |
| **Tipo de dado** | `SMALLINT` |
| **Valores possíveis** | 0 (ausente), 1 (presente) |
| **Campo Silver de origem** | `fl_presente` (cast do campo `st_presenca` do Bronze) |
| **Completude esperada** | 100% |
| **Nulos permitidos** | Não |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Alta |
| **Uso analítico** | Taxa de absenteísmo; produtividade por operador |
| **Exemplos** | 1 (turno realizado), 0 (falta registrada) |

---

## Domínio: Dimensão Data

**Tabela Gold:** `dim_data`
**Job Gold:** `job_dim_data.py`
**Método de geração:** Range sintético de datas (2015-01-01 → 2030-12-31) — não depende de Silver
**Registros:** 5.845 linhas

---

### `fl_fim_semana`

| Atributo | Valor |
|---|---|
| **Descrição** | Indica se a data cai em sábado ou domingo |
| **Tipo de dado** | `SMALLINT` |
| **Valores possíveis** | 0 (dia útil da semana), 1 (fim de semana) |
| **Derivação** | `CASE WHEN dayofweek(dt_completa) IN (1, 7) THEN 1 ELSE 0 END` |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Média |
| **Uso analítico** | Segmentação de volume por período; análise de padrões de atendimento |

---

### `fl_feriado`

| Atributo | Valor |
|---|---|
| **Descrição** | Indica se a data é feriado nacional. Lista de feriados mantida no job `job_dim_data.py` como constante Python |
| **Tipo de dado** | `SMALLINT` |
| **Valores possíveis** | 0 (não feriado), 1 (feriado) |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Média |
| **Limitação** | Cobre apenas feriados nacionais fixos e móveis (Carnaval, Páscoa calculados). Feriados municipais/estaduais não estão incluídos |

---

### `fl_dia_util`

| Atributo | Valor |
|---|---|
| **Descrição** | Indica se a data é um dia útil (não é fim de semana nem feriado). Principal flag para cálculo de SLAs e comparações de produtividade |
| **Tipo de dado** | `SMALLINT` |
| **Valores possíveis** | 0 (não útil), 1 (dia útil) |
| **Derivação** | `CASE WHEN fl_fim_semana = 0 AND fl_feriado = 0 THEN 1 ELSE 0 END` |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Alta |
| **Uso analítico** | `WHERE fl_dia_util = 1` para métricas que excluem fins de semana e feriados |

---

## Domínio: Dimensão Operador

**Tabela Gold:** `dim_operador`
**Tabela Silver de origem:** `db_silver.operador`
**Job Silver:** `job_tb_operador_bronze_to_silver.py`
**Job Gold:** `job_dim_operador.py`

---

### `nr_dias_casa`

| Atributo | Valor |
|---|---|
| **Descrição** | Quantidade de dias desde a data de admissão do operador até a data de processamento. Calculado dinamicamente na Silver |
| **Tipo de dado** | `INT` |
| **Unidade** | dias |
| **Intervalo** | 0 – ∞ |
| **Campo Silver de origem** | `nr_dias_casa` = `datediff(current_date(), dt_admissao)` |
| **Completude esperada** | 95% |
| **Nulos permitidos** | Sim (operadores sem `dt_admissao`) |
| **Conformidade LGPD** | Não contém PII diretamente; indiretamente pode identificar pessoa via cruzamento |
| **Criticidade** | Média |
| **Exemplos** | 30; 180; 730; 1.825 |

---

### `ds_faixa_tempo_casa`

| Atributo | Valor |
|---|---|
| **Descrição** | Categorização do tempo de casa do operador em faixas, para análise de turnover, produtividade por senioridade e programas de retenção |
| **Tipo de dado** | `STRING` |
| **Valores possíveis** | `ATE_3_MESES`, `3_A_6_MESES`, `6_A_12_MESES`, `1_A_2_ANOS`, `ACIMA_2_ANOS` |
| **Regra de classificação** | ≤ 90 dias → ATE_3_MESES \| ≤ 180 → 3_A_6_MESES \| ≤ 365 → 6_A_12_MESES \| ≤ 730 → 1_A_2_ANOS \| > 730 → ACIMA_2_ANOS |
| **Campo Silver de origem** | `ds_faixa_tempo_casa` |
| **Completude esperada** | 95% |
| **Nulos permitidos** | Sim (quando `nr_dias_casa` é nulo) |
| **Conformidade LGPD** | Dado agregado — não identifica individualmente |
| **Criticidade** | Média |
| **Exemplos** | 'ATE_3_MESES' (operador novo), 'ACIMA_2_ANOS' (veterano) |

---

### `fl_supervisor`

| Atributo | Valor |
|---|---|
| **Descrição** | Indica se o operador ocupa papel de supervisor — ou seja, se outros operadores têm `id_supervisor` apontando para ele |
| **Tipo de dado** | `SMALLINT` |
| **Valores possíveis** | 0 (agente), 1 (supervisor) |
| **Derivação** | Derivado no job Gold via auto-join: verifica se `nk_operador` aparece como `id_supervisor` em algum outro registro |
| **Completude esperada** | 100% |
| **Nulos permitidos** | Não |
| **Conformidade LGPD** | Não contém PII |
| **Criticidade** | Média |
| **Uso analítico** | Separação de hierarquia em relatórios; joins com `dim_supervisor` (derivada de `dim_operador`) |

---

## Lineage Cross-Layer

Mapa de rastreabilidade das principais variáveis analíticas, da fonte até o Gold:

```
SISTEMA FONTE (PostgreSQL)
│
├─ tb_chamada.nr_duracao_segundos
│       │
│       ▼ Bronze → Silver (job_tb_chamada)
│   chamada.nr_duracao_segundos          [cast]
│   chamada.nr_duracao_minutos           [derivado: / 60]
│   chamada.fl_duracao_valida            [derivado: > 0 AND NOT NULL]
│   chamada.fl_chamada_completa          [derivado: dt_inicio AND dt_fim NOT NULL]
│       │
│       ▼ Silver → Gold (job_fato_chamada)
│   fato_chamada.nr_duracao_segundos     [propagado]
│   fato_chamada.nr_duracao_minutos      [propagado]
│   fato_chamada.fl_duracao_valida       [propagado]
│   fato_chamada.fl_chamada_completa     [propagado]
│
├─ tb_avaliacao_qualidade.nr_nota
│       │
│       ▼ Bronze → Silver (job_tb_avaliacao_qualidade)
│   avaliacao_qualidade.nr_nota          [cast + coalesce 0]
│   avaliacao_qualidade.ds_faixa_nota    [derivado: CASE WHEN]
│   avaliacao_qualidade.fl_aprovado      [derivado: >= 7]
│   avaliacao_qualidade.fl_critico       [derivado: < 5]
│       │
│       ▼ Silver → Gold (job_fato_qualidade)
│   fato_qualidade.nr_nota               [propagado]
│   fato_qualidade.ds_faixa_nota         [propagado]
│   fato_qualidade.fl_aprovado           [propagado]
│   fato_qualidade.fl_critico            [propagado]
│
├─ tb_metricas_operacionais.[chamadas + tempos]
│       │
│       ▼ Bronze → Silver (job_tb_metricas_operacionais)
│   metricas_operacionais.nr_nivel_servico    [DERIVADO NA SILVER — não existe no Bronze]
│   metricas_operacionais.nr_taxa_atendimento [derivado: 100 - taxa_abandono]
│   metricas_operacionais.nr_taxa_abandono    [derivado: abandonadas / recebidas]
│   metricas_operacionais.nr_tma_minutos      [derivado: tma_segundos / 60]
│   metricas_operacionais.fl_meta_nivel_servico [derivado: nivel_servico >= 80]
│   metricas_operacionais.fl_alto_abandono    [derivado: taxa_abandono > 15]
│       │
│       ▼ Silver → Gold (job_fato_metricas_operacionais)
│   fato_metricas_operacionais.*          [propagados]
│
└─ tb_ticket.[dt_abertura, dt_fechamento, st_ticket]
        │
        ▼ Bronze → Silver (job_tb_ticket)
    ticket.nr_tempo_resolucao_min         [derivado: datediff em minutos]
    ticket.fl_ticket_resolvido            [derivado: st_ticket IN ('RESOLVIDO','FECHADO')]
    ticket.fl_dentro_sla                  [derivado: resolvido AND tempo <= 480 min]
        │
        ▼ Silver → Gold (job_fato_ticket)
    fato_ticket.nr_tempo_resolucao_min    [propagado]
    fato_ticket.fl_ticket_resolvido       [propagado]
    fato_ticket.fl_dentro_sla             [propagado]
```

---

## Matriz de Governança

| Variável | Tabela Gold | Owner | Criticidade | LGPD | SLA Disponibilidade | Retenção |
|---|---|---|---|---|---|---|
| `nr_duracao_segundos` | fato_chamada | data-team | Alta | Não | 6 h após meia-noite | 2 anos |
| `nr_duracao_minutos` | fato_chamada | data-team | Alta | Não | 6 h | 2 anos |
| `fl_duracao_valida` | fato_chamada | data-team | Alta | Não | 6 h | 2 anos |
| `fl_chamada_completa` | fato_chamada | data-team | Alta | Não | 6 h | 2 anos |
| `nr_nota` | fato_qualidade | data-team | Crítica | Não | 6 h | 5 anos |
| `ds_faixa_nota` | fato_qualidade | data-team | Alta | Não | 6 h | 5 anos |
| `fl_aprovado` | fato_qualidade | data-team | Alta | Não | 6 h | 5 anos |
| `fl_critico` | fato_qualidade | data-team | Crítica | Não | 4 h | 5 anos |
| `nr_nivel_servico` | fato_metricas_operacionais | data-team | Crítica | Não | 4 h | 3 anos |
| `nr_taxa_atendimento` | fato_metricas_operacionais | data-team | Crítica | Não | 4 h | 3 anos |
| `nr_taxa_abandono` | fato_metricas_operacionais | data-team | Crítica | Não | 4 h | 3 anos |
| `nr_tma_segundos` | fato_metricas_operacionais | data-team | Alta | Não | 6 h | 3 anos |
| `fl_meta_nivel_servico` | fato_metricas_operacionais | data-team | Crítica | Não | 4 h | 3 anos |
| `fl_alto_abandono` | fato_metricas_operacionais | data-team | Alta | Não | 4 h | 3 anos |
| `nr_tempo_resolucao_min` | fato_ticket | data-team | Alta | Não | 6 h | 3 anos |
| `fl_dentro_sla` | fato_ticket | data-team | Crítica | Não | 6 h | 3 anos |
| `ds_faixa_espera` | fato_ura_navegacao | data-team | Média | Não | 6 h | 1 ano |
| `fl_abandonou_ura` | fato_ura_navegacao | data-team | Alta | Não | 6 h | 1 ano |
| `nr_horas_trabalhadas` | fato_jornada_operador | data-team | Alta | Sim¹ | 6 h | 5 anos |
| `fl_presente` | fato_jornada_operador | data-team | Alta | Sim¹ | 6 h | 5 anos |
| `nr_dias_casa` | dim_operador | data-team | Média | Sim¹ | 6 h | Enquanto ativo |
| `ds_faixa_tempo_casa` | dim_operador | data-team | Média | Não² | 6 h | Enquanto ativo |
| `fl_supervisor` | dim_operador | data-team | Média | Não | 6 h | Enquanto ativo |
| `fl_fim_semana` | dim_data | data-team | Baixa | Não | 24 h | Permanente |
| `fl_feriado` | dim_data | data-team | Baixa | Não | 24 h | Permanente |
| `fl_dia_util` | dim_data | data-team | Média | Não | 24 h | Permanente |

> ¹ Dado ligado a uma pessoa física (operador); acesso controlado via IAM + Lake Formation.
> ² Dado agregado/categorizado — não identifica individualmente.

---

*Última atualização: 2026-09-09 — Execução de referência: 2026-07-10/14*
