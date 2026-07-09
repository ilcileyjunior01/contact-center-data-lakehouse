-- =============================================================================
-- KPI: Volume de Tickets
-- Area: Suporte / Gestao de Incidentes
-- Data: 2026-07-09
-- Objetivo: Monitorar o volume de tickets abertos por categoria, prioridade
--           e status. Calcular taxa de abertura diaria e conformidade de SLA
--           por nivel de prioridade (ALTA: 4h, MEDIA: 8h, BAIXA: 24h).
--
-- Metricas Calculadas:
--   - Volume de tickets por categoria, prioridade e status
--   - Taxa de abertura diaria de tickets
--   - SLA por prioridade:
--       ALTA:  % resolvidos em menos de 4 horas
--       MEDIA: % resolvidos em menos de 8 horas
--       BAIXA: % resolvidos em menos de 24 horas
--   - Tickets em atraso (fora do SLA)
--   - Distribuicao percentual por status
--
-- Como Interpretar:
--   - SLA abaixo de 80% em qualquer prioridade requer atencao imediata
--   - Concentracao em tickets ALTA prioridade indica urgencia operacional
--   - Aumento no volume diario pode antecipar necessidade de capacidade
--   - Status "EM_ABERTO" antigo indica backlog acumulado
-- =============================================================================

WITH
-- CTE 1: Base de tickets com todas as dimensoes necessarias
base_tickets AS (
    SELECT
        ft.sk_ticket,
        ft.sk_cliente,
        ft.sk_operador,
        ft.sk_categoria,
        ft.sk_prioridade,
        ft.sk_status_ticket,
        ft.sk_data_abertura,
        ft.sk_data_resolucao,
        ft.nr_tempo_resolucao_horas,
        dc.ds_categoria_ticket,
        dp.ds_prioridade_ticket,
        ds.ds_status_ticket,
        da.dt_completa         AS dt_abertura,
        da.nr_ano              AS nr_ano_abertura,
        da.nr_mes              AS nr_mes_abertura,
        da.nr_dia              AS nr_dia_abertura,
        DATE_FORMAT(da.dt_completa, '%Y-%m') AS ds_ano_mes_abertura,
        -- SLA target em horas por prioridade
        CASE
            WHEN dp.ds_prioridade_ticket = 'ALTA'  THEN 4
            WHEN dp.ds_prioridade_ticket = 'MEDIA' THEN 8
            WHEN dp.ds_prioridade_ticket = 'BAIXA' THEN 24
            ELSE NULL
        END AS nr_sla_horas_target,
        -- Flag de SLA cumprido
        CASE
            WHEN dp.ds_prioridade_ticket = 'ALTA'
                 AND ft.nr_tempo_resolucao_horas IS NOT NULL
                 AND ft.nr_tempo_resolucao_horas < 4   THEN 1
            WHEN dp.ds_prioridade_ticket = 'MEDIA'
                 AND ft.nr_tempo_resolucao_horas IS NOT NULL
                 AND ft.nr_tempo_resolucao_horas < 8   THEN 1
            WHEN dp.ds_prioridade_ticket = 'BAIXA'
                 AND ft.nr_tempo_resolucao_horas IS NOT NULL
                 AND ft.nr_tempo_resolucao_horas < 24  THEN 1
            ELSE 0
        END AS fl_sla_cumprido,
        -- Flag de ticket resolvido
        CASE WHEN ds.ds_status_ticket IN ('RESOLVIDO', 'FECHADO') THEN 1 ELSE 0 END AS fl_resolvido,
        -- Flag de ticket em aberto
        CASE WHEN ds.ds_status_ticket IN ('ABERTO', 'EM_ATENDIMENTO', 'PENDENTE') THEN 1 ELSE 0 END AS fl_em_aberto
    FROM db_gold.fato_ticket ft
    INNER JOIN db_gold.dim_categoria_ticket dc
        ON ft.sk_categoria = dc.sk_categoria
    INNER JOIN db_gold.dim_prioridade_ticket dp
        ON ft.sk_prioridade = dp.sk_prioridade
    INNER JOIN db_gold.dim_status_ticket ds
        ON ft.sk_status_ticket = ds.sk_status_ticket
    INNER JOIN db_gold.dim_data da
        ON ft.sk_data_abertura = da.sk_data
),

-- CTE 2: Volume diario de tickets abertos
volume_diario AS (
    SELECT
        dt_abertura,
        nr_ano_abertura,
        nr_mes_abertura,
        nr_dia_abertura,
        ds_ano_mes_abertura,
        COUNT(sk_ticket)                                                          AS nr_tickets_abertos_dia,
        COUNT(CASE WHEN ds_prioridade_ticket = 'ALTA'  THEN 1 END)              AS nr_tickets_alta,
        COUNT(CASE WHEN ds_prioridade_ticket = 'MEDIA' THEN 1 END)              AS nr_tickets_media,
        COUNT(CASE WHEN ds_prioridade_ticket = 'BAIXA' THEN 1 END)              AS nr_tickets_baixa,
        SUM(fl_resolvido)                                                         AS nr_tickets_resolvidos,
        SUM(fl_em_aberto)                                                         AS nr_tickets_em_aberto
    FROM base_tickets
    GROUP BY dt_abertura, nr_ano_abertura, nr_mes_abertura, nr_dia_abertura, ds_ano_mes_abertura
),

-- CTE 3: Metricas de SLA por prioridade
sla_por_prioridade AS (
    SELECT
        ds_prioridade_ticket,
        nr_sla_horas_target,
        COUNT(sk_ticket)                                                          AS nr_total_tickets,
        COUNT(CASE WHEN fl_resolvido = 1 THEN 1 END)                             AS nr_resolvidos,
        SUM(fl_sla_cumprido)                                                      AS nr_dentro_sla,
        -- SLA somente sobre tickets que foram resolvidos
        ROUND(
            100.0 * SUM(fl_sla_cumprido)
            / NULLIF(COUNT(CASE WHEN fl_resolvido = 1 THEN 1 END), 0), 2
        )                                                                         AS pct_sla_cumprido,
        -- Tickets fora do SLA
        COUNT(CASE WHEN fl_resolvido = 1 AND fl_sla_cumprido = 0 THEN 1 END)    AS nr_fora_sla,
        -- Tempo medio de resolucao
        ROUND(AVG(CASE WHEN fl_resolvido = 1
                       THEN CAST(nr_tempo_resolucao_horas AS DOUBLE) END), 2)   AS nr_tempo_medio_resolucao_horas,
        ROUND(MAX(CASE WHEN fl_resolvido = 1
                       THEN nr_tempo_resolucao_horas END), 2)                   AS nr_tempo_max_resolucao_horas
    FROM base_tickets
    WHERE ds_prioridade_ticket IN ('ALTA', 'MEDIA', 'BAIXA')
    GROUP BY ds_prioridade_ticket, nr_sla_horas_target
),

-- CTE 4: Volume por categoria de ticket
volume_por_categoria AS (
    SELECT
        ds_categoria_ticket,
        COUNT(sk_ticket)                                                          AS nr_total_tickets,
        COUNT(CASE WHEN ds_prioridade_ticket = 'ALTA'  THEN 1 END)              AS nr_alta_prioridade,
        COUNT(CASE WHEN fl_resolvido = 1 THEN 1 END)                             AS nr_resolvidos,
        COUNT(CASE WHEN fl_em_aberto = 1 THEN 1 END)                             AS nr_em_aberto,
        ROUND(AVG(CASE WHEN fl_resolvido = 1
                       THEN CAST(nr_tempo_resolucao_horas AS DOUBLE) END), 2)   AS nr_mttr_horas,
        ROUND(
            100.0 * SUM(fl_sla_cumprido)
            / NULLIF(COUNT(CASE WHEN fl_resolvido = 1 THEN 1 END), 0), 2
        )                                                                         AS pct_sla_categoria,
        RANK() OVER (ORDER BY COUNT(sk_ticket) DESC)                             AS rank_volume_categoria
    FROM base_tickets
    GROUP BY ds_categoria_ticket
),

-- CTE 5: Distribuicao por status
distribuicao_status AS (
    SELECT
        ds_status_ticket,
        COUNT(sk_ticket)                                                          AS nr_tickets,
        ROUND(
            100.0 * COUNT(sk_ticket)
            / NULLIF((SELECT COUNT(*) FROM base_tickets), 0), 2
        )                                                                         AS pct_do_total
    FROM base_tickets
    GROUP BY ds_status_ticket
)

-- Resultado principal: Volume diario com indicadores de SLA
SELECT
    vd.dt_abertura,
    vd.nr_ano_abertura,
    vd.nr_mes_abertura,
    vd.nr_dia_abertura,
    vd.ds_ano_mes_abertura,
    vd.nr_tickets_abertos_dia,
    vd.nr_tickets_alta,
    vd.nr_tickets_media,
    vd.nr_tickets_baixa,
    vd.nr_tickets_resolvidos,
    vd.nr_tickets_em_aberto,
    -- Taxa de resolucao diaria
    ROUND(
        100.0 * vd.nr_tickets_resolvidos
        / NULLIF(vd.nr_tickets_abertos_dia, 0), 2
    )                                                                             AS pct_resolucao_dia,
    -- Proporcao por prioridade
    ROUND(100.0 * vd.nr_tickets_alta  / NULLIF(vd.nr_tickets_abertos_dia, 0), 2) AS pct_alta_prioridade,
    ROUND(100.0 * vd.nr_tickets_media / NULLIF(vd.nr_tickets_abertos_dia, 0), 2) AS pct_media_prioridade,
    ROUND(100.0 * vd.nr_tickets_baixa / NULLIF(vd.nr_tickets_abertos_dia, 0), 2) AS pct_baixa_prioridade,
    -- Semaforo de volume (comparado a media movel estimada)
    CASE
        WHEN vd.nr_tickets_alta > 50 THEN 'ATENCAO_ALTA_PRIORIDADE'
        WHEN vd.nr_tickets_alta > 20 THEN 'MONITORAR'
        ELSE 'NORMAL'
    END AS ds_alerta_volume
FROM volume_diario vd
ORDER BY
    vd.dt_abertura DESC,
    vd.nr_tickets_abertos_dia DESC
LIMIT 100;

-- =============================================================================
-- Query complementar 1: SLA por prioridade (descomente para usar)
-- =============================================================================
-- SELECT
--     spp.ds_prioridade_ticket,
--     spp.nr_sla_horas_target,
--     spp.nr_total_tickets,
--     spp.nr_resolvidos,
--     spp.nr_dentro_sla,
--     spp.nr_fora_sla,
--     spp.pct_sla_cumprido,
--     spp.nr_tempo_medio_resolucao_horas,
--     spp.nr_tempo_max_resolucao_horas,
--     CASE
--         WHEN spp.pct_sla_cumprido >= 90 THEN 'VERDE'
--         WHEN spp.pct_sla_cumprido >= 75 THEN 'AMARELO'
--         ELSE 'VERMELHO'
--     END AS semaforo_sla
-- FROM sla_por_prioridade spp
-- ORDER BY
--     CASE spp.ds_prioridade_ticket WHEN 'ALTA' THEN 1 WHEN 'MEDIA' THEN 2 ELSE 3 END;

-- =============================================================================
-- Query complementar 2: Volume por categoria (descomente para usar)
-- =============================================================================
-- SELECT
--     vpc.ds_categoria_ticket,
--     vpc.nr_total_tickets,
--     vpc.nr_alta_prioridade,
--     vpc.nr_resolvidos,
--     vpc.nr_em_aberto,
--     vpc.nr_mttr_horas,
--     vpc.pct_sla_categoria,
--     vpc.rank_volume_categoria
-- FROM volume_por_categoria vpc
-- ORDER BY vpc.rank_volume_categoria
-- LIMIT 20;
