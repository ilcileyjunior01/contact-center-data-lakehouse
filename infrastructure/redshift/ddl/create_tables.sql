-- =========================================================
-- DDL Redshift — Contact Center Data Lakehouse
-- Schema: gold (espelha a camada Gold do S3/Glue Catalog)
-- =========================================================
-- Schemas baseados nos tipos reais do Glue Catalog (Iceberg v2)
-- Validados via Athena DESCRIBE em 2026-07-14
-- =========================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- =========================================================
-- DIMENSOES
-- =========================================================

-- dim_data
CREATE TABLE IF NOT EXISTS gold.dim_data (
    sk_data          INTEGER      NOT NULL,
    dt_completa      DATE,
    nr_ano           SMALLINT,
    nr_mes           SMALLINT,
    nr_dia           SMALLINT,
    nr_trimestre     SMALLINT,
    nr_semana_ano    SMALLINT,
    nr_dia_semana    SMALLINT,
    ds_dia_semana    VARCHAR(20),
    ds_mes           VARCHAR(20),
    ds_trimestre     VARCHAR(10),
    fl_fim_de_semana SMALLINT,
    fl_feriado       SMALLINT,
    fl_dia_util      SMALLINT,
    dt_ingestao_gold TIMESTAMP,
    PRIMARY KEY (sk_data)
)
DISTSTYLE ALL
SORTKEY (sk_data);

-- dim_cliente
CREATE TABLE IF NOT EXISTS gold.dim_cliente (
    sk_cliente              INTEGER      NOT NULL,
    nk_cliente              BIGINT,
    nm_cliente              VARCHAR(200),
    nr_documento_mascarado  VARCHAR(30),
    ds_email_mascarado      VARCHAR(200),
    nr_telefone_mascarado   VARCHAR(20),
    dt_cadastro             DATE,
    st_cliente              VARCHAR(50),
    fl_cliente_ativo        SMALLINT,
    ds_cidade               VARCHAR(200),
    ds_estado               VARCHAR(100),
    ds_bairro               VARCHAR(200),
    nr_cep_mascarado        VARCHAR(15),
    dt_ingestao_gold        TIMESTAMP,
    PRIMARY KEY (sk_cliente)
)
DISTSTYLE KEY
DISTKEY (sk_cliente)
SORTKEY (nk_cliente);

-- dim_operador
CREATE TABLE IF NOT EXISTS gold.dim_operador (
    sk_operador         INTEGER      NOT NULL,
    nk_operador         BIGINT,
    nm_operador         VARCHAR(200),
    ds_login_mascarado  VARCHAR(100),
    ds_email_mascarado  VARCHAR(100),
    dt_admissao         DATE,
    st_operador         VARCHAR(30),
    fl_operador_ativo   SMALLINT,
    nr_dias_casa        INTEGER,
    ds_faixa_tempo_casa VARCHAR(50),
    sk_supervisor       INTEGER,
    nk_supervisor       BIGINT,
    dt_ingestao_gold    TIMESTAMP,
    PRIMARY KEY (sk_operador)
)
DISTSTYLE ALL
SORTKEY (nk_operador);

-- dim_fila
CREATE TABLE IF NOT EXISTS gold.dim_fila (
    sk_fila          INTEGER      NOT NULL,
    nk_fila          BIGINT,
    nm_fila          VARCHAR(200),
    ds_tipo_canal    VARCHAR(30),
    nr_sla_segundos  INTEGER,
    nr_sla_minutos   NUMERIC(10,2),
    fl_fila_voz      SMALLINT,
    fl_fila_digital  SMALLINT,
    dt_ingestao_gold TIMESTAMP,
    PRIMARY KEY (sk_fila)
)
DISTSTYLE ALL;

-- dim_canal
CREATE TABLE IF NOT EXISTS gold.dim_canal (
    sk_canal         INTEGER      NOT NULL,
    nm_canal         VARCHAR(50),
    ds_tipo_canal    VARCHAR(30),
    ds_descricao     VARCHAR(200),
    fl_canal_ativo   SMALLINT,
    fl_canal_digital SMALLINT,
    dt_ingestao_gold TIMESTAMP,
    PRIMARY KEY (sk_canal)
)
DISTSTYLE ALL;

-- dim_status_chamada
CREATE TABLE IF NOT EXISTS gold.dim_status_chamada (
    sk_status             INTEGER  NOT NULL,
    ds_status             VARCHAR(50),
    fl_chamada_concluida  SMALLINT,
    fl_chamada_abandonada SMALLINT,
    dt_ingestao_gold      TIMESTAMP,
    PRIMARY KEY (sk_status)
)
DISTSTYLE ALL;

-- dim_status_ticket
CREATE TABLE IF NOT EXISTS gold.dim_status_ticket (
    sk_status           INTEGER  NOT NULL,
    ds_status           VARCHAR(50),
    fl_ticket_aberto    SMALLINT,
    fl_ticket_resolvido SMALLINT,
    fl_ticket_cancelado SMALLINT,
    dt_ingestao_gold    TIMESTAMP,
    PRIMARY KEY (sk_status)
)
DISTSTYLE ALL;

-- dim_campanha
CREATE TABLE IF NOT EXISTS gold.dim_campanha (
    sk_campanha         INTEGER      NOT NULL,
    nk_campanha         BIGINT,
    nm_campanha         VARCHAR(200),
    dt_inicio           DATE,
    dt_fim              DATE,
    st_campanha         VARCHAR(30),
    nr_duracao_dias     INTEGER,
    fl_campanha_ativa   SMALLINT,
    fl_campanha_vigente SMALLINT,
    dt_ingestao_gold    TIMESTAMP,
    PRIMARY KEY (sk_campanha)
)
DISTSTYLE ALL
SORTKEY (dt_inicio);

-- dim_categoria_ticket
CREATE TABLE IF NOT EXISTS gold.dim_categoria_ticket (
    sk_categoria     INTEGER     NOT NULL,
    nm_categoria     VARCHAR(100),
    dt_ingestao_gold TIMESTAMP,
    PRIMARY KEY (sk_categoria)
)
DISTSTYLE ALL;

-- dim_prioridade_ticket
CREATE TABLE IF NOT EXISTS gold.dim_prioridade_ticket (
    sk_prioridade         INTEGER  NOT NULL,
    nm_prioridade         VARCHAR(50),
    nr_ordem_prioridade   SMALLINT,
    fl_prioridade_critica SMALLINT,
    dt_ingestao_gold      TIMESTAMP,
    PRIMARY KEY (sk_prioridade)
)
DISTSTYLE ALL;

-- dim_skill
CREATE TABLE IF NOT EXISTS gold.dim_skill (
    sk_skill         INTEGER      NOT NULL,
    ds_skill         VARCHAR(100),
    nr_nivel         SMALLINT,
    ds_faixa_nivel   VARCHAR(30),
    dt_ingestao_gold TIMESTAMP,
    PRIMARY KEY (sk_skill)
)
DISTSTYLE ALL;

-- =========================================================
-- FATOS
-- =========================================================

-- fato_chamada
CREATE TABLE IF NOT EXISTS gold.fato_chamada (
    sk_chamada          BIGINT  NOT NULL,
    nk_chamada          BIGINT,
    sk_cliente          INTEGER,
    sk_operador         INTEGER,
    sk_fila             INTEGER,
    sk_canal            INTEGER,
    sk_status_chamada   INTEGER,
    sk_data_inicio      INTEGER,
    sk_data_fim         INTEGER,
    tp_chamada          VARCHAR(20),
    nr_duracao_segundos INTEGER,
    nr_duracao_minutos  NUMERIC(10,2),
    fl_duracao_valida   SMALLINT,
    fl_chamada_completa SMALLINT,
    dt_ingestao_gold    TIMESTAMP,
    PRIMARY KEY (sk_chamada)
)
DISTSTYLE KEY
DISTKEY (sk_chamada)
SORTKEY (sk_data_inicio);

-- fato_ticket
CREATE TABLE IF NOT EXISTS gold.fato_ticket (
    sk_ticket              BIGINT       NOT NULL,
    nk_ticket              BIGINT,
    nr_protocolo           VARCHAR(30),
    sk_cliente             INTEGER,
    sk_operador_abertura   INTEGER,
    sk_status_ticket       INTEGER,
    sk_categoria           INTEGER,
    sk_prioridade          INTEGER,
    sk_data_abertura       INTEGER,
    sk_data_fechamento     INTEGER,
    nr_tempo_resolucao_min NUMERIC(10,2),
    fl_ticket_resolvido    SMALLINT,
    fl_dentro_sla          SMALLINT,
    dt_ingestao_gold       TIMESTAMP,
    PRIMARY KEY (sk_ticket)
)
DISTSTYLE KEY
DISTKEY (sk_ticket)
SORTKEY (sk_data_abertura);

-- fato_qualidade
CREATE TABLE IF NOT EXISTS gold.fato_qualidade (
    sk_avaliacao              BIGINT  NOT NULL,
    nk_avaliacao              BIGINT,
    sk_chamada                BIGINT,
    sk_operador_avaliado      INTEGER,
    sk_avaliador              INTEGER,
    sk_data                   INTEGER,
    nr_nota                   NUMERIC(4,1),
    ds_faixa_nota             VARCHAR(20),
    fl_aprovado               SMALLINT,
    fl_critico                SMALLINT,
    fl_tem_feedback           SMALLINT,
    nr_tamanho_feedback_chars INTEGER,
    dt_ingestao_gold          TIMESTAMP,
    PRIMARY KEY (sk_avaliacao)
)
DISTSTYLE KEY
DISTKEY (sk_avaliacao)
SORTKEY (sk_data);

-- fato_discagem
CREATE TABLE IF NOT EXISTS gold.fato_discagem (
    sk_discagem              BIGINT  NOT NULL,
    nk_discagem              BIGINT,
    sk_campanha              INTEGER,
    sk_cliente               INTEGER,
    sk_data                  INTEGER,
    nr_telefone_mascarado    VARCHAR(20),
    st_discagem              VARCHAR(30),
    fl_discagem_atendida     SMALLINT,
    fl_discagem_nao_atendida SMALLINT,
    dt_ingestao_gold         TIMESTAMP,
    PRIMARY KEY (sk_discagem)
)
DISTSTYLE KEY
DISTKEY (sk_discagem)
SORTKEY (sk_data);

-- fato_ura_navegacao
CREATE TABLE IF NOT EXISTS gold.fato_ura_navegacao (
    sk_ura               BIGINT  NOT NULL,
    nk_ura               BIGINT,
    sk_chamada           BIGINT,
    sk_data              INTEGER,
    ds_opcao_selecionada VARCHAR(100),
    nr_duracao_segundos  INTEGER,
    fl_abandonou_ura     SMALLINT,
    ds_faixa_espera      VARCHAR(30),
    dt_ingestao_gold     TIMESTAMP,
    PRIMARY KEY (sk_ura)
)
DISTSTYLE KEY
DISTKEY (sk_ura)
SORTKEY (sk_data);

-- fato_chat
CREATE TABLE IF NOT EXISTS gold.fato_chat (
    sk_chat             BIGINT  NOT NULL,
    nk_chat             BIGINT,
    sk_cliente          INTEGER,
    sk_operador         INTEGER,
    sk_canal            INTEGER,
    sk_data_inicio      INTEGER,
    sk_data_fim         INTEGER,
    nr_duracao_segundos INTEGER,
    nr_duracao_minutos  NUMERIC(10,2),
    fl_chat_completo    SMALLINT,
    st_chat             VARCHAR(30),
    dt_ingestao_gold    TIMESTAMP,
    PRIMARY KEY (sk_chat)
)
DISTSTYLE KEY
DISTKEY (sk_chat)
SORTKEY (sk_data_inicio);

-- fato_whatsapp
CREATE TABLE IF NOT EXISTS gold.fato_whatsapp (
    sk_whatsapp             BIGINT  NOT NULL,
    nk_whatsapp             BIGINT,
    sk_cliente              INTEGER,
    sk_operador             INTEGER,
    sk_canal                INTEGER,
    sk_data_inicio          INTEGER,
    sk_data_fim             INTEGER,
    nr_telefone_mascarado   VARCHAR(20),
    nr_duracao_segundos     INTEGER,
    nr_duracao_minutos      NUMERIC(10,2),
    fl_atendimento_completo SMALLINT,
    st_atendimento          VARCHAR(30),
    dt_ingestao_gold        TIMESTAMP,
    PRIMARY KEY (sk_whatsapp)
)
DISTSTYLE KEY
DISTKEY (sk_whatsapp)
SORTKEY (sk_data_inicio);

-- fato_jornada_operador
CREATE TABLE IF NOT EXISTS gold.fato_jornada_operador (
    sk_jornada            BIGINT  NOT NULL,
    nk_jornada            BIGINT,
    sk_operador           INTEGER,
    sk_data               INTEGER,
    nr_horas_trabalhadas  NUMERIC(5,2),
    nr_chamadas_atendidas INTEGER,
    nr_tickets_resolvidos INTEGER,
    st_presenca           VARCHAR(30),
    fl_presente           SMALLINT,
    dt_ingestao_gold      TIMESTAMP,
    PRIMARY KEY (sk_jornada)
)
DISTSTYLE KEY
DISTKEY (sk_jornada)
SORTKEY (sk_data);

-- fato_metricas_operacionais
CREATE TABLE IF NOT EXISTS gold.fato_metricas_operacionais (
    sk_metrica              BIGINT  NOT NULL,
    nk_metrica              BIGINT,
    sk_fila                 INTEGER,
    sk_data                 INTEGER,
    nr_chamadas_recebidas   INTEGER,
    nr_chamadas_atendidas   INTEGER,
    nr_chamadas_abandonadas INTEGER,
    nr_tma_segundos         INTEGER,
    nr_tma_minutos          NUMERIC(10,2),
    nr_tme_segundos         INTEGER,
    nr_tme_minutos          NUMERIC(10,2),
    nr_nivel_servico        NUMERIC(5,2),
    nr_taxa_atendimento     NUMERIC(5,2),
    nr_taxa_abandono        NUMERIC(5,2),
    fl_meta_nivel_servico   SMALLINT,
    fl_alto_abandono        SMALLINT,
    dt_ingestao_gold        TIMESTAMP,
    PRIMARY KEY (sk_metrica)
)
DISTSTYLE KEY
DISTKEY (sk_metrica)
SORTKEY (sk_data);

-- fato_interacao_ticket
CREATE TABLE IF NOT EXISTS gold.fato_interacao_ticket (
    sk_interacao                BIGINT  NOT NULL,
    nk_interacao                BIGINT,
    sk_ticket                   BIGINT,
    sk_operador                 INTEGER,
    sk_data                     INTEGER,
    ds_canal                    VARCHAR(50),
    nr_tamanho_observacao_chars INTEGER,
    fl_tem_observacao           SMALLINT,
    dt_ingestao_gold            TIMESTAMP,
    PRIMARY KEY (sk_interacao)
)
DISTSTYLE KEY
DISTKEY (sk_interacao)
SORTKEY (sk_data);

-- fato_mensagem_chat
CREATE TABLE IF NOT EXISTS gold.fato_mensagem_chat (
    sk_mensagem          BIGINT  NOT NULL,
    nk_mensagem          BIGINT,
    sk_chat              BIGINT,
    sk_data              INTEGER,
    ds_remetente         VARCHAR(30),
    nr_tamanho_chars     INTEGER,
    fl_mensagem_cliente  SMALLINT,
    fl_mensagem_operador SMALLINT,
    dt_ingestao_gold     TIMESTAMP,
    PRIMARY KEY (sk_mensagem)
)
DISTSTYLE KEY
DISTKEY (sk_mensagem)
SORTKEY (sk_data);
