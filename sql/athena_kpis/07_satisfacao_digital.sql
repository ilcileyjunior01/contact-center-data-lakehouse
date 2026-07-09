-- =============================================================================
-- KPI: Satisfacao em Canais Digitais (CSAT)
-- Area: Experiencia do Cliente / Canais Digitais
-- Data: 2026-07-09
-- Objetivo: Calcular o CSAT (Customer Satisfaction Score) medio por canal
--           digital (Chat vs WhatsApp), analisar a distribuicao de notas de
--           satisfacao (escala 1-5) e acompanhar a tendencia mensal de
--           satisfacao ao longo do tempo.
--
-- Metricas Calculadas:
--   - CSAT medio por canal (Chat e WhatsApp separadamente)
--   - CSAT medio geral de canais digitais
--   - Distribuicao de notas 1 a 5 (contagem e percentual)
--   - NPS proxy: % notas 4-5 (promotores) vs % notas 1-2 (detratores)
--   - Tendencia mensal de satisfacao por canal
--   - CSAT por operador (top e bottom performers)
--
-- Como Interpretar:
--   - CSAT acima de 4.0 (escala 1-5) indica satisfacao elevada
--   - Percentual de notas 1-2 acima de 15% exige investigacao de causas
--   - Tendencia declinante de CSAT por 3 meses consecutivos e sinal de alerta
--   - Diferenca CSAT Chat vs WhatsApp indica preferencia de canal do cliente
-- =============================================================================

WITH
-- CTE 1: Base de sessoes Chat com nota de satisfacao
base_csat_chat AS (
    SELECT
        fc.sk_chat,
        fc.sk_cliente,
        fc.sk_operador,
        fc.sk_canal,
        'CHAT' AS ds_tipo_canal,
        dc.ds_canal,
        fc.nr_nota_satisfacao,
        dd.dt_completa,
        dd.nr_ano,
        dd.nr_mes,
        DATE_FORMAT(dd.dt_completa, '%Y-%m') AS ds_ano_mes,
        do2.nm_operador
    FROM db_gold.fato_chat fc
    INNER JOIN db_gold.dim_canal dc
        ON fc.sk_canal = dc.sk_canal
    INNER JOIN db_gold.dim_data dd
        ON fc.sk_data_inicio = dd.sk_data
    INNER JOIN db_gold.dim_operador do2
        ON fc.sk_operador = do2.sk_operador
    WHERE fc.nr_nota_satisfacao IS NOT NULL
      AND fc.nr_nota_satisfacao BETWEEN 1 AND 5
),

-- CTE 2: Base de sessoes WhatsApp com nota de satisfacao
base_csat_whatsapp AS (
    SELECT
        fw.sk_whatsapp  AS sk_chat,
        fw.sk_cliente,
        fw.sk_operador,
        fw.sk_canal,
        'WHATSAPP' AS ds_tipo_canal,
        dc.ds_canal,
        fw.nr_nota_satisfacao,
        dd.dt_completa,
        dd.nr_ano,
        dd.nr_mes,
        DATE_FORMAT(dd.dt_completa, '%Y-%m') AS ds_ano_mes,
        do2.nm_operador
    FROM db_gold.fato_whatsapp fw
    INNER JOIN db_gold.dim_canal dc
        ON fw.sk_canal = dc.sk_canal
    INNER JOIN db_gold.dim_data dd
        ON fw.sk_data_inicio = dd.sk_data
    INNER JOIN db_gold.dim_operador do2
        ON fw.sk_operador = do2.sk_operador
    WHERE fw.nr_nota_satisfacao IS NOT NULL
      AND fw.nr_nota_satisfacao BETWEEN 1 AND 5
),

-- CTE 3: Base unificada de CSAT digital
base_csat_unificada AS (
    SELECT * FROM base_csat_chat
    UNION ALL
    SELECT * FROM base_csat_whatsapp
),

-- CTE 4: CSAT por canal
csat_por_canal AS (
    SELECT
        ds_tipo_canal,
        COUNT(sk_chat)                                                            AS nr_avaliacoes,
        -- CSAT medio
        ROUND(AVG(CAST(nr_nota_satisfacao AS DOUBLE)), 2)                        AS nr_csat_medio,
        -- Distribuicao por nota
        COUNT(CASE WHEN nr_nota_satisfacao = 1 THEN 1 END)                       AS nr_nota_1,
        COUNT(CASE WHEN nr_nota_satisfacao = 2 THEN 1 END)                       AS nr_nota_2,
        COUNT(CASE WHEN nr_nota_satisfacao = 3 THEN 1 END)                       AS nr_nota_3,
        COUNT(CASE WHEN nr_nota_satisfacao = 4 THEN 1 END)                       AS nr_nota_4,
        COUNT(CASE WHEN nr_nota_satisfacao = 5 THEN 1 END)                       AS nr_nota_5,
        -- Percentual por nota
        ROUND(100.0 * COUNT(CASE WHEN nr_nota_satisfacao = 1 THEN 1 END)
              / NULLIF(COUNT(sk_chat), 0), 2)                                    AS pct_nota_1,
        ROUND(100.0 * COUNT(CASE WHEN nr_nota_satisfacao = 2 THEN 1 END)
              / NULLIF(COUNT(sk_chat), 0), 2)                                    AS pct_nota_2,
        ROUND(100.0 * COUNT(CASE WHEN nr_nota_satisfacao = 3 THEN 1 END)
              / NULLIF(COUNT(sk_chat), 0), 2)                                    AS pct_nota_3,
        ROUND(100.0 * COUNT(CASE WHEN nr_nota_satisfacao = 4 THEN 1 END)
              / NULLIF(COUNT(sk_chat), 0), 2)                                    AS pct_nota_4,
        ROUND(100.0 * COUNT(CASE WHEN nr_nota_satisfacao = 5 THEN 1 END)
              / NULLIF(COUNT(sk_chat), 0), 2)                                    AS pct_nota_5,
        -- Promotores (notas 4-5): proxy de NPS positivo
        ROUND(100.0 * COUNT(CASE WHEN nr_nota_satisfacao >= 4 THEN 1 END)
              / NULLIF(COUNT(sk_chat), 0), 2)                                    AS pct_promotores,
        -- Detratores (notas 1-2): proxy de NPS negativo
        ROUND(100.0 * COUNT(CASE WHEN nr_nota_satisfacao <= 2 THEN 1 END)
              / NULLIF(COUNT(sk_chat), 0), 2)                                    AS pct_detratores,
        -- NPS proxy: promotores - detratores (em pontos percentuais)
        ROUND(
            100.0 * COUNT(CASE WHEN nr_nota_satisfacao >= 4 THEN 1 END)
            / NULLIF(COUNT(sk_chat), 0)
            - 100.0 * COUNT(CASE WHEN nr_nota_satisfacao <= 2 THEN 1 END)
            / NULLIF(COUNT(sk_chat), 0), 2
        )                                                                         AS nr_nps_proxy
    FROM base_csat_unificada
    GROUP BY ds_tipo_canal
),

-- CTE 5: Tendencia mensal de CSAT por canal
tendencia_mensal AS (
    SELECT
        ds_ano_mes,
        nr_ano,
        nr_mes,
        ds_tipo_canal,
        COUNT(sk_chat)                                                            AS nr_avaliacoes_mes,
        ROUND(AVG(CAST(nr_nota_satisfacao AS DOUBLE)), 2)                        AS nr_csat_medio_mes,
        ROUND(100.0 * COUNT(CASE WHEN nr_nota_satisfacao >= 4 THEN 1 END)
              / NULLIF(COUNT(sk_chat), 0), 2)                                    AS pct_promotores_mes,
        ROUND(100.0 * COUNT(CASE WHEN nr_nota_satisfacao <= 2 THEN 1 END)
              / NULLIF(COUNT(sk_chat), 0), 2)                                    AS pct_detratores_mes,
        -- CSAT mes anterior (LAG para verificar tendencia)
        LAG(ROUND(AVG(CAST(nr_nota_satisfacao AS DOUBLE)), 2))
            OVER (PARTITION BY ds_tipo_canal ORDER BY nr_ano, nr_mes)            AS nr_csat_mes_anterior,
        -- Variacao mes a mes
        ROUND(
            ROUND(AVG(CAST(nr_nota_satisfacao AS DOUBLE)), 2)
            - LAG(ROUND(AVG(CAST(nr_nota_satisfacao AS DOUBLE)), 2))
                OVER (PARTITION BY ds_tipo_canal ORDER BY nr_ano, nr_mes), 2
        )                                                                         AS nr_variacao_csat_mom
    FROM base_csat_unificada
    GROUP BY ds_ano_mes, nr_ano, nr_mes, ds_tipo_canal
),

-- CTE 6: CSAT por operador (para identificar top/bottom performers)
csat_por_operador AS (
    SELECT
        sk_operador,
        nm_operador,
        ds_tipo_canal,
        COUNT(sk_chat)                                                            AS nr_avaliacoes,
        ROUND(AVG(CAST(nr_nota_satisfacao AS DOUBLE)), 2)                        AS nr_csat_medio,
        ROUND(100.0 * COUNT(CASE WHEN nr_nota_satisfacao >= 4 THEN 1 END)
              / NULLIF(COUNT(sk_chat), 0), 2)                                    AS pct_promotores,
        RANK() OVER (
            PARTITION BY ds_tipo_canal
            ORDER BY AVG(CAST(nr_nota_satisfacao AS DOUBLE)) DESC
        )                                                                         AS rank_csat_canal
    FROM base_csat_unificada
    WHERE nm_operador IS NOT NULL
    GROUP BY sk_operador, nm_operador, ds_tipo_canal
    HAVING COUNT(sk_chat) >= 10  -- Minimo de avaliacoes para representatividade
)

-- Resultado principal: CSAT por canal com distribuicao de notas
SELECT
    cpc.ds_tipo_canal,
    cpc.nr_avaliacoes,
    cpc.nr_csat_medio,
    cpc.nr_nota_1,
    cpc.nr_nota_2,
    cpc.nr_nota_3,
    cpc.nr_nota_4,
    cpc.nr_nota_5,
    cpc.pct_nota_1,
    cpc.pct_nota_2,
    cpc.pct_nota_3,
    cpc.pct_nota_4,
    cpc.pct_nota_5,
    cpc.pct_promotores,
    cpc.pct_detratores,
    cpc.nr_nps_proxy,
    -- Semaforo de CSAT (escala 1-5)
    CASE
        WHEN cpc.nr_csat_medio >= 4.0 THEN 'VERDE_SATISFATORIO'
        WHEN cpc.nr_csat_medio >= 3.0 THEN 'AMARELO_ATENCAO'
        ELSE 'VERMELHO_CRITICO'
    END AS semaforo_csat,
    -- Semaforo de detratores
    CASE
        WHEN cpc.pct_detratores <= 10 THEN 'VERDE'
        WHEN cpc.pct_detratores <= 20 THEN 'AMARELO'
        ELSE 'VERMELHO'
    END AS semaforo_detratores
FROM csat_por_canal cpc
ORDER BY cpc.nr_csat_medio DESC;

-- =============================================================================
-- Query complementar 1: Tendencia mensal (descomente para usar)
-- =============================================================================
-- SELECT
--     tm.ds_ano_mes,
--     tm.ds_tipo_canal,
--     tm.nr_avaliacoes_mes,
--     tm.nr_csat_medio_mes,
--     tm.nr_csat_mes_anterior,
--     tm.nr_variacao_csat_mom,
--     tm.pct_promotores_mes,
--     tm.pct_detratores_mes,
--     CASE
--         WHEN tm.nr_variacao_csat_mom > 0 THEN 'MELHORA'
--         WHEN tm.nr_variacao_csat_mom < 0 THEN 'PIORA'
--         ELSE 'ESTAVEL'
--     END AS ds_tendencia
-- FROM tendencia_mensal tm
-- ORDER BY tm.ds_tipo_canal, tm.nr_ano ASC, tm.nr_mes ASC
-- LIMIT 48;

-- =============================================================================
-- Query complementar 2: Top 10 e Bottom 10 operadores por CSAT (descomente)
-- =============================================================================
-- SELECT *
-- FROM csat_por_operador
-- WHERE rank_csat_canal <= 10
-- ORDER BY ds_tipo_canal, rank_csat_canal
-- LIMIT 20;
