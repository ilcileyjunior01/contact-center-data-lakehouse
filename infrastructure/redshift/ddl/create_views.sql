-- =========================================================
-- Views analíticas Redshift — Contact Center Data Lakehouse
-- Espelham os 12 KPIs do projeto (sql/athena_kpis/)
-- =========================================================

-- ─── KPI 01 — Volume e Desempenho de Chamadas ──────────────────────────────
CREATE OR REPLACE VIEW gold.vw_kpi_01_volume_chamadas AS
SELECT
    d.nr_ano,
    d.nr_mes,
    d.ds_nome_mes,
    sc.ds_status,
    COUNT(c.sk_chamada)                             AS nr_total_chamadas,
    SUM(c.fl_chamada_completa)                      AS nr_chamadas_completas,
    ROUND(AVG(c.nr_duracao_minutos), 2)             AS nr_tma_medio_minutos,
    ROUND(
        100.0 * SUM(c.fl_chamada_completa) / NULLIF(COUNT(c.sk_chamada), 0),
        2
    )                                               AS pct_taxa_atendimento
FROM gold.fato_chamada c
JOIN gold.dim_data            d  ON c.sk_data_inicio   = d.sk_data
JOIN gold.dim_status_chamada  sc ON c.sk_status_chamada = sc.sk_status
GROUP BY d.nr_ano, d.nr_mes, d.ds_nome_mes, sc.ds_status;

-- ─── KPI 02 — Performance de Operadores ───────────────────────────────────
CREATE OR REPLACE VIEW gold.vw_kpi_02_performance_operadores AS
SELECT
    o.nm_operador,
    o.ds_faixa_tempo_casa,
    o.fl_operador_ativo,
    COUNT(c.sk_chamada)                             AS nr_chamadas_atendidas,
    ROUND(AVG(c.nr_duracao_minutos), 2)             AS nr_tma_medio,
    ROUND(
        100.0 * SUM(c.fl_chamada_completa) / NULLIF(COUNT(c.sk_chamada), 0),
        2
    )                                               AS pct_conclusao
FROM gold.fato_chamada c
JOIN gold.dim_operador o ON c.sk_operador = o.sk_operador
GROUP BY o.nm_operador, o.ds_faixa_tempo_casa, o.fl_operador_ativo;

-- ─── KPI 03 — Qualidade de Atendimento ────────────────────────────────────
CREATE OR REPLACE VIEW gold.vw_kpi_03_qualidade AS
SELECT
    o.nm_operador,
    d.nr_ano,
    d.nr_mes,
    COUNT(q.sk_avaliacao)                           AS nr_avaliacoes,
    ROUND(AVG(q.nr_nota), 2)                        AS nr_nota_media,
    SUM(q.fl_aprovado)                              AS nr_aprovados,
    SUM(q.fl_critico)                               AS nr_criticos,
    ROUND(
        100.0 * SUM(q.fl_aprovado) / NULLIF(COUNT(q.sk_avaliacao), 0),
        2
    )                                               AS pct_aprovacao
FROM gold.fato_qualidade q
JOIN gold.dim_operador o ON q.sk_operador_avaliado = o.sk_operador
JOIN gold.dim_data     d ON q.sk_data              = d.sk_data
GROUP BY o.nm_operador, d.nr_ano, d.nr_mes;

-- ─── KPI 04 — Volume de Tickets ───────────────────────────────────────────
CREATE OR REPLACE VIEW gold.vw_kpi_04_volume_tickets AS
SELECT
    d.nr_ano,
    d.nr_mes,
    st.ds_status,
    cat.nm_categoria,
    pri.nm_prioridade,
    COUNT(t.sk_ticket)                              AS nr_total_tickets,
    SUM(t.fl_ticket_resolvido)                      AS nr_resolvidos,
    SUM(t.fl_dentro_sla)                            AS nr_dentro_sla
FROM gold.fato_ticket t
JOIN gold.dim_data              d   ON t.sk_data_abertura = d.sk_data
JOIN gold.dim_status_ticket     st  ON t.sk_status_ticket = st.sk_status
JOIN gold.dim_categoria_ticket  cat ON t.sk_categoria     = cat.sk_categoria
JOIN gold.dim_prioridade_ticket pri ON t.sk_prioridade    = pri.sk_prioridade
GROUP BY d.nr_ano, d.nr_mes, st.ds_status, cat.nm_categoria, pri.nm_prioridade;

-- ─── KPI 05 — Eficiência de Tickets (SLA) ────────────────────────────────
CREATE OR REPLACE VIEW gold.vw_kpi_05_eficiencia_tickets AS
SELECT
    d.nr_ano,
    d.nr_mes,
    pri.nm_prioridade,
    ROUND(AVG(t.nr_tempo_resolucao_min), 2)         AS nr_trt_medio_minutos,
    ROUND(
        100.0 * SUM(t.fl_dentro_sla) / NULLIF(COUNT(t.sk_ticket), 0),
        2
    )                                               AS pct_sla_cumprido
FROM gold.fato_ticket t
JOIN gold.dim_data              d   ON t.sk_data_abertura = d.sk_data
JOIN gold.dim_prioridade_ticket pri ON t.sk_prioridade    = pri.sk_prioridade
WHERE t.fl_ticket_resolvido = 1
GROUP BY d.nr_ano, d.nr_mes, pri.nm_prioridade;

-- ─── KPI 06 — Volume Chat e WhatsApp ─────────────────────────────────────
CREATE OR REPLACE VIEW gold.vw_kpi_06_volume_digital AS
SELECT
    d.nr_ano,
    d.nr_mes,
    'CHAT'                                          AS ds_canal,
    COUNT(c.sk_chat)                                AS nr_atendimentos,
    ROUND(AVG(c.nr_duracao_minutos), 2)             AS nr_duracao_media,
    SUM(c.fl_chat_completo)                         AS nr_completos
FROM gold.fato_chat c
JOIN gold.dim_data d ON c.sk_data_inicio = d.sk_data
GROUP BY d.nr_ano, d.nr_mes

UNION ALL

SELECT
    d.nr_ano,
    d.nr_mes,
    'WHATSAPP'                                      AS ds_canal,
    COUNT(w.sk_whatsapp)                            AS nr_atendimentos,
    ROUND(AVG(w.nr_duracao_minutos), 2)             AS nr_duracao_media,
    SUM(w.fl_atendimento_completo)                  AS nr_completos
FROM gold.fato_whatsapp w
JOIN gold.dim_data d ON w.sk_data_inicio = d.sk_data
GROUP BY d.nr_ano, d.nr_mes;

-- ─── KPI 08 — Desempenho de Campanhas ────────────────────────────────────
CREATE OR REPLACE VIEW gold.vw_kpi_08_campanhas AS
SELECT
    camp.nm_campanha,
    camp.dt_inicio,
    camp.dt_fim,
    camp.fl_campanha_ativa,
    COUNT(disc.sk_discagem)                         AS nr_total_discagens,
    SUM(disc.fl_discagem_atendida)                  AS nr_atendidas,
    ROUND(
        100.0 * SUM(disc.fl_discagem_atendida) / NULLIF(COUNT(disc.sk_discagem), 0),
        2
    )                                               AS pct_taxa_atendimento
FROM gold.fato_discagem disc
JOIN gold.dim_campanha camp ON disc.sk_campanha = camp.sk_campanha
GROUP BY camp.nm_campanha, camp.dt_inicio, camp.dt_fim, camp.fl_campanha_ativa;

-- ─── KPI 09 — Métricas Operacionais de Fila ──────────────────────────────
CREATE OR REPLACE VIEW gold.vw_kpi_09_metricas_fila AS
SELECT
    f.nm_fila,
    f.ds_tipo_canal,
    d.nr_ano,
    d.nr_mes,
    SUM(m.nr_chamadas_recebidas)                    AS nr_chamadas_recebidas,
    SUM(m.nr_chamadas_atendidas)                    AS nr_chamadas_atendidas,
    ROUND(AVG(m.nr_nivel_servico), 2)               AS nr_nivel_servico_medio,
    ROUND(AVG(m.nr_tma_minutos), 2)                 AS nr_tma_medio,
    ROUND(AVG(m.nr_tme_minutos), 2)                 AS nr_tme_medio,
    ROUND(AVG(m.nr_taxa_abandono), 2)               AS nr_taxa_abandono_media
FROM gold.fato_metricas_operacionais m
JOIN gold.dim_fila f ON m.sk_fila = f.sk_fila
JOIN gold.dim_data d ON m.sk_data = d.sk_data
GROUP BY f.nm_fila, f.ds_tipo_canal, d.nr_ano, d.nr_mes;

-- ─── KPI 12 — Efetividade da URA ─────────────────────────────────────────
CREATE OR REPLACE VIEW gold.vw_kpi_12_ura AS
SELECT
    d.nr_ano,
    d.nr_mes,
    u.ds_opcao_selecionada,
    COUNT(u.sk_ura)                                 AS nr_navegacoes,
    SUM(u.fl_abandonou_ura)                         AS nr_abandonos,
    ROUND(AVG(u.nr_duracao_segundos), 0)            AS nr_duracao_media_segundos,
    ROUND(
        100.0 * SUM(u.fl_abandonou_ura) / NULLIF(COUNT(u.sk_ura), 0),
        2
    )                                               AS pct_abandono
FROM gold.fato_ura_navegacao u
JOIN gold.dim_data d ON u.sk_data = d.sk_data
GROUP BY d.nr_ano, d.nr_mes, u.ds_opcao_selecionada;
