# Amazon QuickSight — Guia de Setup (Contact Center Lakehouse)

**Pré-requisito:** Pipeline AWS rodando (40 jobs OK, tabelas Gold no Glue Catalog).  
**Custo estimado:** US$ 18/mês (1 autor). Free trial de 30 dias disponível.

---

## 1. Habilitar o QuickSight na conta AWS

### 1.1 Console

1. Acesse **https://quicksight.aws.amazon.com**
2. Clique em **Sign up for QuickSight**
3. Escolha a edição:
   - **Standard** — suficiente para este projeto (sem row-level security)
   - **Enterprise** — necessário se precisar de RLS ou embedding
4. **Account name:** `act-cc-contact-center`
5. **Email:** seu email
6. Em **QuickSight access to AWS services**, marque:
   - ✅ Amazon Athena
   - ✅ Amazon S3 → selecione o bucket `act-cc-dev-lakehouse`
7. Clique em **Finish**

### 1.2 Verificar via CLI

```bash
aws quicksight describe-account-settings \
  --aws-account-id $(aws sts get-caller-identity --query Account --output text) \
  --namespace default
```

---

## 2. Executar o script de setup automático

```bash
cd contact-center-data-lakehouse/infrastructure/quicksight

# Instalar dependências
pip install boto3

# Testar sem criar recursos (dry-run)
python 01_setup_quicksight.py --dry-run

# Executar de verdade
python 01_setup_quicksight.py
```

O script cria automaticamente:
- **IAM Role** `QuickSightServiceRole-ContactCenter` (Athena + Glue + S3)
- **Athena Data Source** `athena-contact-center` (workgroup `primary`)
- **5 SPICE Datasets** com ingestão automática:

| Dataset ID | Conteúdo | KPIs |
|---|---|---|
| `ds-chamadas` | Volume e desempenho de chamadas | KPI 01 |
| `ds-operadores` | Performance e qualidade operadores | KPI 02 + 03 |
| `ds-tickets` | Tickets SLA e eficiência | KPI 04 + 05 |
| `ds-digital` | Chat e WhatsApp | KPI 06 + 07 |
| `ds-campanhas-ura` | Campanhas de discagem e URA | KPI 08 + 09 + 12 |

---

## 3. Criar a Analysis no console QuickSight

### 3.1 Nova analysis

1. Console QuickSight → **Analyses** → **New analysis**
2. Selecione `CC - Chamadas Volume e Desempenho`
3. Clique em **Create**

### 3.2 Adicionar os outros datasets

- **Edit** (ícone de lápis) → **Add data**
- Adicione: `ds-operadores`, `ds-tickets`, `ds-digital`, `ds-campanhas-ura`

---

## 4. Estrutura do Dashboard (5 páginas)

### Página 1 — Visão Geral Chamadas (`ds-chamadas`)

| Visual | Tipo | Campos |
|---|---|---|
| Total de Chamadas | KPI Card | `nr_total_chamadas` |
| Taxa de Atendimento | KPI Card | `pct_taxa_atendimento` (meta: ≥95%) |
| TMA Médio | KPI Card | `nr_tma_minutos` (meta: ≤3 min) |
| Volume por Dia | Line Chart | X=`dt_completa`, Y=`nr_total_chamadas`, Color=`ds_canal` |
| Abandono por Fila | Bar Chart | X=`ds_fila`, Y=`pct_taxa_abandono` |
| Status Semáforo | Table | `ds_canal`, `semaforo_atendimento`, `semaforo_abandono` |

**Filtros sugeridos:** `nr_ano`, `nr_mes`, `ds_canal`

---

### Página 2 — Performance Operadores (`ds-operadores`)

| Visual | Tipo | Campos |
|---|---|---|
| Ranking Operadores | Table | `nm_operador`, `nr_chamadas`, `nr_tma_segundos`, `nr_nota_qualidade_media` |
| Top 10 por Volume | Horizontal Bar | X=`nr_chamadas`, Y=`nm_operador` |
| TMA vs Qualidade | Scatter Plot | X=`nr_tma_segundos`, Y=`nr_nota_qualidade_media`, Size=`nr_chamadas` |
| Por Equipe | Pie Chart | `ds_equipe` (fatia = % chamadas) |

**Filtros sugeridos:** `ds_equipe`, `ds_faixa_tempo_casa`, `nr_mes`

---

### Página 3 — Tickets e SLA (`ds-tickets`)

| Visual | Tipo | Campos |
|---|---|---|
| Total Tickets | KPI Card | `nr_total_tickets` |
| SLA Cumprido | KPI Card | `pct_sla_cumprido` (meta: ≥90%) |
| TMA Resolução | KPI Card | `nr_tma_resolucao_horas` |
| SLA no Tempo | Line Chart | X=`dt_abertura`, Y=`pct_sla_cumprido` |
| Tickets por Canal | Pie Chart | `ds_canal` |
| Semáforo SLA | Table | `ds_canal`, `semaforo_sla`, `nr_total_tickets` |

**Filtros sugeridos:** `semaforo_sla`, `ds_canal`, `nr_mes`

---

### Página 4 — Canais Digitais (`ds-digital`)

| Visual | Tipo | Campos |
|---|---|---|
| Total Interações | KPI Card | `nr_total_interacoes` |
| Taxa Resolução | KPI Card | `pct_resolucao` |
| Chat vs WhatsApp | Bar Chart | X=`ds_tipo_digital`, Y=`nr_total_interacoes` |
| Resolução no Tempo | Line Chart | X=`dt_completa`, Y=`pct_resolucao`, Color=`ds_tipo_digital` |
| Ranking Operadores | Table | `nm_operador`, `nr_total_interacoes`, `pct_resolucao` |

**Filtros sugeridos:** `ds_tipo_digital`, `ds_fila`, `nr_mes`

---

### Página 5 — Campanhas e URA (`ds-campanhas-ura`)

| Visual | Tipo | Campos |
|---|---|---|
| Total Discagens | KPI Card | `nr_total_discagens` |
| Taxa Conversão | KPI Card | `pct_taxa_conversao` |
| ROI Campanhas | KPI Card | `nr_roi_campanha` |
| ROI por Fila | Bar Chart | X=`ds_fila`, Y=`nr_roi_campanha` |
| Conversão no Tempo | Line Chart | X=`dt_completa`, Y=`pct_taxa_conversao` |
| Atendimento Discagem | Line Chart | X=`dt_completa`, Y=`pct_taxa_atendimento_discagem` |

**Filtros sugeridos:** `ds_canal`, `ds_fila`, `nr_mes`

---

## 5. Publicar como Dashboard

1. Analysis aberta → **Share** (ícone no canto superior direito)
2. **Publish dashboard**
3. Nome: `Contact Center - Operational Dashboard`
4. Clique em **Publish dashboard**
5. URL pública gerada — cole no README do projeto

---

## 6. Configurar refresh automático do SPICE

Para manter os dados atualizados diariamente:

1. Console QuickSight → **Datasets**
2. Clique em cada dataset → **Schedule refresh**
3. **Add new schedule**:
   - Frequency: **Daily**
   - Time: **06:00 UTC** (03:00 BRT)
4. Repita para os 5 datasets

Custo SPICE: os 5 datasets juntos ficam dentro dos 10 GB grátis por autor.

---

## 7. Solução de problemas

### Data source com erro "CREATION_FAILED"

```bash
# Verificar se o QuickSight tem acesso ao workgroup Athena
aws athena get-work-group --work-group primary

# Verificar a policy da IAM role
aws iam get-role-policy \
  --role-name QuickSightServiceRole-ContactCenter \
  --policy-name QuickSightContactCenterPolicy
```

### Dataset com erro de SQL

- Abra o Athena Query Editor e execute a query manualmente primeiro
- Verifique se as tabelas existem em `db_gold`:
  ```sql
  SHOW TABLES IN db_gold;
  ```

### Ingestão SPICE travada

```bash
aws quicksight list-ingestions \
  --aws-account-id $(aws sts get-caller-identity --query Account --output text) \
  --data-set-id ds-chamadas
```

---

## 8. Arquivos de referência

| Arquivo | Descrição |
|---|---|
| `infrastructure/quicksight/01_setup_quicksight.py` | Script boto3 de setup completo |
| `infrastructure/quicksight/iam_quicksight_policy.json` | Policy IAM para a role do QuickSight |
| `sql/athena_kpis/` | 12 queries KPI que alimentam os datasets |

---

## 9. Deletar QuickSight (para economizar)

Após as entrevistas ou quando não precisar mais:

1. Console QuickSight → **Admin** → **Account settings**
2. **Unsubscribe from QuickSight**
3. Confirme digitando `confirm`

> Os dados no S3/Athena não são afetados — apenas o QuickSight é removido.
