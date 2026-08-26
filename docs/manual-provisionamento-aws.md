# Manual de Provisionamento AWS — Contact Center Data Lakehouse

**Autor:** Ilciley Junior  
**GitHub:** [@ilcileyjunior01](https://github.com/ilcileyjunior01)  
**Projeto:** contact-center-data-lakehouse  

---

## Sumário

1. [Pré-requisitos](#1-pré-requisitos)
2. [Criar usuário IAM](#2-criar-usuário-iam)
3. [Configurar credenciais AWS CLI](#3-configurar-credenciais-aws-cli)
4. [Passo 1 — Criar bucket S3](#4-passo-1--criar-bucket-s3)
5. [Passo 2 — Criar IAM Role para o Glue](#5-passo-2--criar-iam-role-para-o-glue)
6. [Passo 3 — Criar Glue Databases e Crawlers](#6-passo-3--criar-glue-databases-e-crawlers)
7. [Passo 4 — Carregar dados no S3 Bronze](#7-passo-4--carregar-dados-no-s3-bronze)
8. [Passo 5 — Executar Crawlers e registrar tabelas](#8-passo-5--executar-crawlers-e-registrar-tabelas)
9. [Passo 6 — Registrar e executar Jobs Glue](#9-passo-6--registrar-e-executar-jobs-glue)
10. [Passo 7 — Consultar com Athena](#10-passo-7--consultar-com-athena)
11. [Custos e alertas](#11-custos-e-alertas)

---

## 1. Pré-requisitos

### O que você precisa ter instalado

| Ferramenta | Verificar com | Instalar |
|---|---|---|
| Python 3.9+ | `python --version` | [python.org](https://python.org) |
| AWS CLI v2 | `aws --version` | [docs.aws.amazon.com/cli](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) |
| Bibliotecas Python | `pip install boto3 pandas pyarrow faker` | — |

### Clonar o repositório

```bash
git clone https://github.com/ilcileyjunior01/contact-center-data-lakehouse.git
cd contact-center-data-lakehouse
pip install -r requirements.txt
```

---

## 2. Criar usuário IAM

> **Por que não usar a conta raiz (root)?**  
> A conta raiz tem poder total sobre a AWS — pode fechar a conta, alterar cobrança, deletar tudo. Se a chave vazar, você perde o controle total. Um IAM User tem permissões limitadas ao que você definir.

### Passo a passo no Console AWS

**1.** Acesse o [Console AWS](https://console.aws.amazon.com) e faça login com sua conta root.

**2.** No campo de busca superior, digite `IAM` e clique em **IAM**.

```
Console AWS > Buscar "IAM" > IAM (Identity and Access Management)
```

**3.** No menu lateral esquerdo, clique em **Users** (Usuários).

**4.** Clique no botão **Create user** (Criar usuário) no canto superior direito.

**5.** Preencha o formulário:
- **User name:** `portfolio-data-engineer`
- Deixe desmarcada a opção "Provide user access to the AWS Management Console"
- Clique **Next**

> *Captura de tela sugerida: Tela de criação de usuário com o nome preenchido*

**6.** Na tela de permissões, selecione **Attach policies directly**.

**7.** No campo de busca, digite `AdministratorAccess` e marque a policy.

> **Nota:** `AdministratorAccess` é suficiente para o portfólio. Em ambiente de produção, você criaria policies com permissões mínimas por serviço (S3, Glue, Lambda, etc.).

**8.** Clique **Next** > **Create user**.

> *Captura de tela sugerida: Tela de seleção de policies com AdministratorAccess marcada*

### Criar Access Key

**9.** Clique no nome do usuário criado (`portfolio-data-engineer`).

**10.** Vá para a aba **Security credentials**.

**11.** Na seção **Access keys**, clique em **Create access key**.

**12.** Em "Use case", selecione **Command Line Interface (CLI)**.

**13.** Marque a confirmação e clique **Next** > **Create access key**.

**14.** **IMPORTANTE:** Copie e salve agora:
- `Access Key ID` (começa com `AKIA...`)
- `Secret Access Key` (sequência longa — aparece só uma vez!)

> *Captura de tela sugerida: Tela mostrando o Access Key ID e o botão "Show" para a Secret Key*

---

## 3. Configurar credenciais AWS CLI

> **O que é o AWS CLI?**  
> É uma ferramenta de linha de comando que permite controlar todos os serviços AWS a partir do terminal. O `boto3` (biblioteca Python) usa as mesmas credenciais configuradas aqui.

Como o terminal do Claude Code não suporta entrada interativa, configure com comandos individuais:

```bash
! aws configure set aws_access_key_id SEU_ACCESS_KEY_ID
! aws configure set aws_secret_access_key SUA_SECRET_ACCESS_KEY
! aws configure set region us-east-1
! aws configure set output json
```

> **Região us-east-1 (N. Virginia):** É a região mais antiga da AWS, tem todos os serviços disponíveis e costuma ser a mais barata. Para um portfólio com dados sintéticos, a diferença de custo vs. sa-east-1 (São Paulo) é de ~$1-2/mês.

### Verificar se funcionou

```bash
! aws sts get-caller-identity
```

Resultado esperado:
```json
{
    "UserId": "AIDA...",
    "Account": "642825398802",
    "Arn": "arn:aws:iam::642825398802:user/portfolio-data-engineer"
}
```

> *Captura de tela sugerida: Terminal mostrando o resultado do get-caller-identity*

---

## 4. Passo 1 — Criar bucket S3

> **O que é o Amazon S3?**  
> Simple Storage Service — armazenamento de objetos na nuvem. É o coração do Data Lake: todas as camadas Bronze, Silver e Gold ficam em arquivos Parquet dentro do S3. Diferente de um banco de dados, o S3 armazena arquivos brutos e é muito mais barato para grandes volumes.

### O que o script cria

| Recurso | Detalhe |
|---|---|
| Bucket | `act-cc-dev-lakehouse` em `us-east-1` |
| Acesso público | Bloqueado (segurança) |
| Versionamento | Ativado (permite recuperar versões anteriores) |
| Criptografia | SSE-S3 AES256 (padrão, sem custo extra) |
| Pastas top-level | bronze/, silver/, gold/, checkpoints/, quarantine/, logs/, scripts/, athena-results/, temp/ |
| Subpastas bronze | 18 pastas organizadas por domínio (operacao, cadastro, qualidade, marketing, suporte) |
| Lifecycle rules | quarantine/: 90 dias; temp/: 7 dias; bronze/: IA após 90d, Glacier após 365d |
| EventBridge | Notificações de eventos S3 habilitadas (para acionar Lambda automaticamente) |

### Executar

```bash
python infrastructure/01_setup_s3.py
```

### Verificar no Console AWS

Após a execução:

```
Console AWS > S3 > act-cc-dev-lakehouse > Explorar
```

Você verá a estrutura de pastas criada.

> *Captura de tela sugerida: S3 Console mostrando o bucket com as pastas bronze/, silver/, gold/, etc.*

**Custo estimado:** ~$0.023/GB/mês de armazenamento + requests negligíveis. Para o portfólio (~50 MB de dados), custo < $0.01/mês.

---

## 5. Passo 2 — Criar IAM Role para o Glue

> **O que é uma IAM Role?**  
> É uma identidade temporária com permissões que você cede a um serviço AWS. O AWS Glue precisa de uma Role para poder: ler arquivos do S3, escrever dados processados de volta no S3, e registrar metadados no Data Catalog.  
>  
> **Analogia:** É como dar um crachá de acesso a um funcionário (Glue) que permite entrar apenas nas áreas necessárias — não em toda a empresa.

### O que o script cria

| Recurso | Detalhe |
|---|---|
| Role | `GlueExecutionRole` |
| Trust Policy | Permite que `glue.amazonaws.com` assuma essa role |
| Policy gerenciada | `AWSGlueServiceRole` (permissões básicas do Glue + CloudWatch Logs) |
| Policy inline | `GlueS3AccessPolicy` — acesso de leitura/escrita SOMENTE no bucket `act-cc-dev-lakehouse` |

### Executar (script inline)

O Passo 2 é executado por um script Python inline dentro do processo de provisionamento. Resultado esperado:

```
[OK] Role criada: arn:aws:iam::642825398802:role/GlueExecutionRole
[OK] AWSGlueServiceRole anexada
[OK] Policy 'GlueS3AccessPolicy' criada e anexada
```

### Verificar no Console AWS

```
Console AWS > IAM > Roles > GlueExecutionRole
```

> *Captura de tela sugerida: IAM Console mostrando a role GlueExecutionRole com as duas policies anexadas*

---

## 6. Passo 3 — Criar Glue Databases e Crawlers

> **O que é o AWS Glue?**  
> Serviço de ETL (Extract, Transform, Load) serverless da AWS. Tem três componentes principais:
>
> - **Data Catalog:** Catálogo centralizado de metadados — registra o esquema (colunas, tipos) de todas as tabelas das três camadas
> - **Crawlers:** Robôs que "vasculham" pastas do S3, descobrem o esquema dos arquivos Parquet e registram no Catalog automaticamente
> - **Jobs:** Scripts PySpark que executam as transformações Bronze->Silver->Gold

### O que o script cria

| Recurso | Quantidade | Detalhe |
|---|---|---|
| Databases | 3 | `db_bronze`, `db_silver`, `db_gold` |
| Crawlers | 18 | Um por tabela, apontando para pasta bronze/ correspondente |
| Workflow | 1 | `wf-cc-pipeline-diario` (orquestra a sequência de jobs) |

### Executar

```bash
python infrastructure/02_setup_glue.py
```

### Verificar no Console AWS

```
Console AWS > AWS Glue > Crawlers
```

Você verá os 18 crawlers com status `Ready`.

> *Captura de tela sugerida: Glue Console mostrando a lista dos 18 crawlers com status Ready*

```
Console AWS > AWS Glue > Databases
```

Você verá os 3 databases: `db_bronze`, `db_silver`, `db_gold`.

---

## 7. Passo 4 — Carregar dados no S3 Bronze

> **Por que não usar o DMS + Kinesis?**  
> Em produção, o AWS DMS captura mudanças no PostgreSQL via WAL e entrega para o Kinesis Firehose, que converte para Parquet e salva no S3. Para o portfólio, esse caminho custaria ~$24/mês.  
>  
> O script `s3_data_loader.py` faz exatamente a mesma coisa — converte CSVs para Parquet com compressão Snappy e envia para o S3 nas mesmas pastas e partições que o Firehose criaria. **Custo: $0.**

### O que o script faz

1. Lê os 18 CSVs sintéticos de `data/synthetic/output/`
2. Converte para Parquet com compressão Snappy
3. Adiciona colunas de partição (`ano`, `mes`, `dia`) baseadas na data de cada registro
4. Faz upload para `s3://act-cc-dev-lakehouse/bronze/{dominio}/{tabela}/ano=YYYY/mes=MM/dia=DD/`

### Executar

```bash
# Gerar dados sintéticos (se ainda não fez)
python data/synthetic/generate_data.py

# Carregar no S3
python src/ingestion/s3_data_loader.py --bucket act-cc-dev-lakehouse --region us-east-1
```

### Verificar no Console AWS

```
Console AWS > S3 > act-cc-dev-lakehouse > bronze/ > operacao/ > chamada/
```

Você verá a estrutura de partições:
```
ano=2024/
  mes=01/
    dia=15/
      tb_chamada_20240115_143022.parquet
```

> *Captura de tela sugerida: S3 Console navegando dentro de bronze/operacao/chamada/ mostrando partições por ano/mes/dia*

---

## 8. Passo 5 — Executar Crawlers e registrar tabelas no Catalog

> **Por que rodar os Crawlers?**  
> Os arquivos Parquet no S3 são só arquivos — o Athena e o Glue não sabem o esquema deles (quais colunas existem, que tipos são). O Crawler lê uma amostra dos arquivos, infere o esquema e registra as tabelas no Data Catalog. Depois disso, você consegue fazer `SELECT * FROM db_bronze.tb_chamada` no Athena.

### Executar todos os 18 crawlers

```bash
! aws glue start-crawler --name crawler-operacao-chamada --region us-east-1
! aws glue start-crawler --name crawler-operacao-ticket --region us-east-1
! aws glue start-crawler --name crawler-cadastro-cliente --region us-east-1
# ... (repetir para todos os 18)
```

Ou via Console:

```
Console AWS > AWS Glue > Crawlers > selecionar todos > Run
```

> *Captura de tela sugerida: Glue Console com os crawlers selecionados e botão Run destacado*

### Monitorar execução

```bash
! aws glue get-crawler --name crawler-operacao-chamada --region us-east-1 --query 'Crawler.State'
```

Aguarde o status mudar de `RUNNING` para `READY`. Cada crawler leva ~2-3 minutos.

### Verificar tabelas registradas

```
Console AWS > AWS Glue > Tables > Database: db_bronze
```

Você verá as tabelas descobertas pelos crawlers.

> *Captura de tela sugerida: Glue Data Catalog mostrando as tabelas registradas em db_bronze*

**Custo estimado:** ~$0.44/DPU-hora × 2 DPUs × ~3 min × 18 crawlers = ~**$0.79**

---

## 9. Passo 6 — Registrar e executar Jobs Glue

> **O que são os Glue Jobs?**  
> São scripts PySpark executados no AWS Glue. Cada job lê dados de uma camada, aplica transformações (limpeza, deduplicação, mascaramento PII, modelagem dimensional) e escreve na próxima camada em formato Apache Iceberg com ACID.

### Fazer upload dos scripts para o S3

```bash
! aws s3 cp src/bronze_to_silver/ s3://act-cc-dev-lakehouse/scripts/bronze_to_silver/ --recursive --region us-east-1
! aws s3 cp src/silver_to_gold/ s3://act-cc-dev-lakehouse/scripts/silver_to_gold/ --recursive --region us-east-1
```

### Registrar um job (exemplo: tb_chamada)

```bash
! aws glue create-job \
  --name "job-tb-chamada-bronze-to-silver" \
  --role "GlueExecutionRole" \
  --command "Name=glueetl,ScriptLocation=s3://act-cc-dev-lakehouse/scripts/bronze_to_silver/job_tb_chamada_bronze_to_silver.py,PythonVersion=3" \
  --glue-version "4.0" \
  --number-of-workers 2 \
  --worker-type G.1X \
  --region us-east-1
```

### Executar o job

```bash
! aws glue start-job-run --job-name "job-tb-chamada-bronze-to-silver" --region us-east-1
```

### Monitorar execução

```
Console AWS > AWS Glue > Jobs > job-tb-chamada-bronze-to-silver > Run history
```

> *Captura de tela sugerida: Glue Console mostrando o job com status Succeeded e duração*

**Custo estimado:** ~$0.044/DPU-min × 2 DPUs × ~3 min = **~$0.26 por job**

---

## 10. Passo 7 — Consultar com Athena

> **O que é o Amazon Athena?**  
> Serviço de query SQL serverless que executa consultas diretamente sobre arquivos no S3. Não há banco de dados para provisionar — você paga apenas pelos dados escaneados. Com Parquet + particionamento, o custo é mínimo.

### Configurar o Athena

```
Console AWS > Amazon Athena > Settings > Manage
Query result location: s3://act-cc-dev-lakehouse/athena-results/
```

> *Captura de tela sugerida: Athena Settings mostrando o campo de query result location preenchido*

### Executar uma query KPI

```
Console AWS > Amazon Athena > Query editor
Database: db_gold
```

Cole e execute uma das queries de `sql/athena_kpis/`:

```sql
-- KPI 1: Volume e TMA de chamadas
SELECT
    d.nr_ano,
    d.nr_mes,
    COUNT(*) AS total_chamadas,
    ROUND(AVG(f.nr_duracao_segundos) / 60.0, 2) AS tma_minutos
FROM db_gold.fato_chamada f
JOIN db_gold.dim_data d ON f.sk_data_inicio = d.sk_data
WHERE f.fl_duracao_valida = 1
GROUP BY d.nr_ano, d.nr_mes
ORDER BY d.nr_ano, d.nr_mes;
```

> *Captura de tela sugerida: Athena Query Editor com uma query KPI e o resultado em tabela abaixo*

**Custo estimado:** ~$5/TB escaneado. Com Parquet (~50 MB de dados), cada query custa **~$0.0003**.

---

## 11. Custos e alertas

### Resumo de custos do portfólio

| Serviço | Recurso | Custo estimado |
|---|---|---|
| S3 | 50 MB de dados | ~$0.01/mês |
| Glue Crawlers | 18 crawlers × 3 min | ~$0.79/rodada |
| Glue Jobs | 40 jobs × 3 min × 2 workers | ~$10.50/pipeline completo |
| Athena | 50 queries/mês em Parquet | ~$0.02/mês |
| Lambda | ~100 invocações | ~$0.00 |
| **TOTAL (uso moderado)** | | **~$15/mês** |

### Configurar alerta de orçamento

```
Console AWS > Billing > Budgets > Create budget
```

Configuração recomendada:
- Budget type: Cost budget
- Period: Monthly
- Budget amount: $20
- Alert: 80% do budget ($16) -> enviar e-mail

> *Captura de tela sugerida: AWS Budgets mostrando o alerta configurado*

### Dicas para reduzir custo

1. **Não deixe jobs Glue rodando acidentalmente** — cada worker G.1X custa $0.44/hora
2. **Use Athena com filtros de partição** — sempre filtre por `ano` e `mes` para reduzir dados escaneados
3. **Delete recursos quando não estiver usando** — crawlers e jobs não têm custo de idle
4. **Redshift Serverless tem auto-pause** — configure para pausar após 30 minutos idle

---

## Recursos criados neste provisionamento

| Serviço | Recurso | ARN / Identificador |
|---|---|---|
| S3 | Bucket | `act-cc-dev-lakehouse` |
| IAM | Role | `arn:aws:iam::642825398802:role/GlueExecutionRole` |
| Glue | Database | `db_bronze`, `db_silver`, `db_gold` |
| Glue | Crawlers | 18 crawlers (`crawler-{dominio}-{tabela}`) |
| Glue | Workflow | `wf-cc-pipeline-diario` |

---

*Documento gerado durante o processo de provisionamento do projeto Contact Center Data Lakehouse.*  
*Adicione capturas de tela das telas AWS nos pontos indicados para completar o manual.*
