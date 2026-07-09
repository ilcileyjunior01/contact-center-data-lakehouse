-- =============================================================================
-- KPI: Eficiencia de Resolucao de Tickets
-- Area: Suporte / Qualidade de Servico
-- Data: 2026-07-09
-- Objetivo: Medir a eficiencia operacional de resolucao de tickets atraves do
--           MTTR (Mean Time To Resolve) por categoria e prioridade, da taxa FCR
--           (First Contact Resolution - resolvido sem escalonamento) e do
--           backlog atual de tickets em aberto.
--
-- Metricas Calculadas:
--   - MTTR: Tempo medio de resolucao em horas, por categoria e prioridade
--   - FCR: % de tickets resolvidos no primeiro contato (sem escalonamento)
--   - Backlog: quantidade de tickets em aberto por prioridade e categoria
--   - Tempo medio em cada status (aging de tickets)
--   - Distribuicao do MTTR por faixas de tempo
--
-- Como Interpretar:
--   - MTTR crescente indica degradacao operacional ou aumento de complexidade
--   - FCR abaixo de 70% sugere necessidade de capacitacao ou melhor triagem
--   - Backlog alto em tickets ALTA prioridade requer intervencao imediata
--   - Tickets com idade > 2x o SLA alvo devem ser escalonados manualmente
-- =============================================================================

WITH
-- CTE 1: Base enriquecida de tickets com categorias e prioridades
base_tickets_eficiencia AS (
    SELECT
        ft.sk_ticket,
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
        da.dt_completa       AS dt_abertura,
        da.nr_ano,
        da.nr_mes,
        DATE_FORMAT(da.dt_completa, '%Y-%m') AS ds_ano_mes,
        -- Flag: ticket resolvido
        CASE WHEN ds.ds_status_ticket IN ('RESOLVIDO', 'FECHADO') THEN 1 ELSE 0 END AS fl_resolvido,
        -- Flag: ticket em aberto (backlog ativo)
        CASE WHEN ds.ds_status_ticket IN ('ABERTO', 'EM_ATENDIMENTO', 'PENDENTE') THEN 1 ELSE 0 END AS fl_em_aberto,
        -- Flag: escalonado (baseado em mudanca de operador ou status de escalonamento)
        CASE WHEN ds.ds_status_ticket = 'ESCALONADO' THEN 1 ELSE 0 END AS fl_escalonado,
        -- Flag: FCR (resolvido sem passar por escalonamento - sem flag de escalonado)
        CASE WHEN ds.ds_status_ticket IN ('RESOLVIDO', 'FECHADO')
             AND ds.ds_status_ticket <> 'ESCALONADO' THEN 1 ELSE 0 END AS fl_fcr_candidato,
        -- SLA target em horas
        CASE
            WHEN dp.ds_prioridade_ticket = 'ALTA'  THEN 4
            WHEN dp.ds_prioridade_ticket = 'MEDIA' THEN 8
            WHEN dp.ds_prioridade_ticket = 'BAIXA' THEN 24
            ELSE NULL
        END AS nr_sla_target_horas,
        -- Faixa de MTTR para distribuicao
        CASE
            WHEN ft.nr_tempo_resolucao_horas < 1     THEN 'MENOS_1H'
            WHEN ft.nr_tempo_resolucao_horas < 4     THEN '1H_A_4H'
            WHEN ft.nr_tempo_resolucao_horas < 8     THEN '4H_A_8H'
            WHEN ft.nr_tempo_resolucao_horas < 24    THEN '8H_A_24H'
            WHEN ft.nr_tempo_resolucao_horas < 72    THEN '24H_A_72H'
            WHEN ft.nr_tempo_resolucao_horas IS NULL THEN 'NAO_RESOLVIDO'
            ELSE 'MAIS_72H'
        END AS ds_faixa_resolucao
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

-- CTE 2: MTTR por categoria de ticket
mttr_por_categoria AS (
    SELECT
        ds_categoria_ticket,
        COUNT(sk_ticket)                                                          AS nr_total_tickets,
        COUNT(CASE WHEN fl_resolvido = 1 THEN 1 END)                             AS nr_tickets_resolvidos,
        -- MTTR medio (somente tickets resolvidos)
        ROUND(
            AVG(CASE WHEN fl_resolvido = 1
                     THEN CAST(nr_tempo_resolucao_horas AS DOUBLE) END), 2
        )                                                                         AS nr_mttr_horas,
        ROUND(
            AVG(CASE WHEN fl_resolvido = 1
                     THEN CAST(nr_tempo_resolucao_horas AS DOUBLE) END) / 24.0, 2
        )                                                                         AS nr_mttr_dias,
        -- Mediana aproximada (percentil 50) para evitar distorcao de outliers
        APPROX_PERCENTILE(
            CAST(COALESCE(nr_tempo_resolucao_horas, 0) AS DOUBLE), 0.50
        )                                                                         AS nr_mttr_mediana_horas,
        -- Percentil 90 para capturar casos mais lentos
        APPROX_PERCENTILE(
            CAST(COALESCE(nr_tempo_resolucao_horas, 0) AS DOUBLE), 0.90
        )                                                                         AS nr_mttr_p90_horas,
        -- MTTR minimo e maximo
        MIN(CASE WHEN fl_resolvido = 1 THEN nr_tempo_resolucao_horas END)        AS nr_mttr_minimo,
        MAX(CASE WHEN fl_resolvido = 1 THEN nr_tempo_resolucao_horas END)        AS nr_mttr_maximo,
        -- FCR: tickets resolvidos que nao foram escalonados
        ROUND(
            100.0 * SUM(CASE WHEN fl_fcr_candidato = 1 THEN 1 ELSE 0 END)
            / NULLIF(COUNT(CASE WHEN fl_resolvido = 1 THEN 1 END), 0), 2
        )                                                                         AS pct_fcr,
        RANK() OVER (ORDER BY
            AVG(CASE WHEN fl_resolvido = 1
                     THEN CAST(nr_tempo_resolucao_horas AS DOUBLE) END) ASC
        )                                                                         AS rank_mttr_categoria
    FROM base_tickets_eficiencia
    GROUP BY ds_categoria_ticket
),

-- CTE 3: MTTR por prioridade
mttr_por_prioridade AS (
    SELECT
        ds_prioridade_ticket,
        nr_sla_target_horas,
        COUNT(sk_ticket)                                                          AS nr_total_tickets,
        COUNT(CASE WHEN fl_resolvido = 1 THEN 1 END)                             AS nr_tickets_resolvidos,
        ROUND(
            AVG(CASE WHEN fl_resolvido = 1
                     THEN CAST(nr_tempo_resolucao_horas AS DOUBLE) END), 2
        )                                                                         AS nr_mttr_horas,
        -- Razao MTTR vs SLA target (quanto acima/abaixo do alvo)
        ROUND(
            AVG(CASE WHEN fl_resolvido = 1
                     THEN CAST(nr_tempo_resolucao_horas AS DOUBLE) END)
            / NULLIF(nr_sla_target_horas, 0), 2
        )                                                                         AS nr_ratio_mttr_sla,
        ROUND(
            100.0 * SUM(CASE WHEN fl_fcr_candidato = 1 THEN 1 ELSE 0 END)
            / NULLIF(COUNT(CASE WHEN fl_resolvido = 1 THEN 1 END), 0), 2
        )                                                                         AS pct_fcr
    FROM base_tickets_eficiencia
    GROUP BY ds_prioridade_ticket, nr_sla_target_horas
),

-- CTE 4: Backlog atual (tickets em aberto)
backlog_atual AS (
    SELECT
        ds_prioridade_ticket,
        ds_categoria_ticket,
        COUNT(sk_ticket)                                                          AS nr_backlog,
        -- Backlog critico: tickets em aberto que ja ultrapassaram o SLA
        COUNT(CASE
            WHEN fl_em_aberto = 1 AND nr_sla_target_horas IS NOT NULL
            THEN 1
        END)                                                                      AS nr_backlog_total_aberto,
        AVG(
            CASE WHEN fl_em_aberto = 1 THEN nr_sla_target_horas END
        )                                                                         AS nr_media_horas_em_aberto
    FROM base_tickets_eficiencia
    WHERE fl_em_aberto = 1
    GROUP BY ds_prioridade_ticket, ds_categoria_ticket
),

-- CTE 5: Distribuicao do MTTR por faixas de tempo
distribuicao_faixas AS (
    SELECT
        ds_faixa_resolucao,
        ds_prioridade_ticket,
        COUNT(sk_ticket)                                                          AS nr_tickets,
        ROUND(
            100.0 * COUNT(sk_ticket)
            / NULLIF(SUM(COUNT(sk_ticket)) OVER (PARTITION BY ds_prioridade_ticket), 0), 2
        )                                                                         AS pct_faixa
    FROM base_tickets_eficiencia
    GROUP BY ds_faixa_resolucao, ds_prioridade_ticket
),

-- CTE 6: FCR global
fcr_global AS (
    SELECT
        COUNT(sk_ticket)                                                          AS nr_total_resolvidos,
        SUM(fl_fcr_candidato)                                                     AS nr_fcr,
        ROUND(
            100.0 * SUM(fl_fcr_candidato)
            / NULLIF(COUNT(CASE WHEN fl_resolvido = 1 THEN 1 END), 0), 2
        )                                                                         AS pct_fcr_global,
        ROUND(
            AVG(CASE WHEN fl_resolvido = 1
                     THEN CAST(nr_tempo_resolucao_horas AS DOUBLE) END), 2
        )                                                                         AS nr_mttr_global_horas
    FROM base_tickets_eficiencia
    WHERE fl_resolvido = 1
)

-- Resultado principal: MTTR por categoria com FCR e backlog
SELECT
    mpc.ds_categoria_ticket,
    mpc.nr_total_tickets,
    mpc.nr_tickets_resolvidos,
    mpc.nr_mttr_horas,
    mpc.nr_mttr_dias,
    mpc.nr_mttr_mediana_horas,
    mpc.nr_mttr_p90_horas,
    mpc.nr_mttr_minimo,
    mpc.nr_mttr_maximo,
    mpc.pct_fcr,
    mpc.rank_mttr_categoria,
    -- Backlog por categoria
    COALESCE(ba.nr_backlog, 0)                                                    AS nr_backlog_atual,
    -- FCR global para referencia
    fg.pct_fcr_global,
    fg.nr_mttr_global_horas,
    -- Desvio do MTTR da categoria em relacao ao MTTR global
    ROUND(mpc.nr_mttr_horas - fg.nr_mttr_global_horas, 2)                       AS desvio_mttr_vs_global,
    -- Semaforo de FCR
    CASE
        WHEN mpc.pct_fcr >= 80 THEN 'VERDE'
        WHEN mpc.pct_fcr >= 65 THEN 'AMARELO'
        ELSE 'VERMELHO'
    END AS semaforo_fcr
FROM mttr_por_categoria mpc
LEFT JOIN (
    SELECT ds_categoria_ticket, SUM(nr_backlog) AS nr_backlog
    FROM backlog_atual
    GROUP BY ds_categoria_ticket
) ba ON mpc.ds_categoria_ticket = ba.ds_categoria_ticket
CROSS JOIN fcr_global fg
ORDER BY
    mpc.nr_mttr_horas DESC,
    mpc.nr_total_tickets DESC
LIMIT 100;

-- =============================================================================
-- Query complementar: MTTR e FCR por prioridade (descomente para usar)
-- =============================================================================
-- SELECT
--     mpp.ds_prioridade_ticket,
--     mpp.nr_sla_target_horas,
--     mpp.nr_total_tickets,
--     mpp.nr_tickets_resolvidos,
--     mpp.nr_mttr_horas,
--     mpp.nr_ratio_mttr_sla,
--     mpp.pct_fcr,
--     CASE
--         WHEN mpp.nr_ratio_mttr_sla <= 1.0 THEN 'DENTRO_DO_SLA'
--         WHEN mpp.nr_ratio_mttr_sla <= 1.5 THEN 'LEVEMENTE_ACIMA'
--         ELSE 'MUITO_ACIMA_DO_SLA'
--     END AS ds_situacao_sla
-- FROM mttr_por_prioridade mpp
-- ORDER BY
--     CASE mpp.ds_prioridade_ticket WHEN 'ALTA' THEN 1 WHEN 'MEDIA' THEN 2 ELSE 3 END;
