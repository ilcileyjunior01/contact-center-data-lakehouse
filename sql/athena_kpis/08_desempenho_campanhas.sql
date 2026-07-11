-- =============================================================================
-- KPI: Desempenho de Campanhas de Discagem
-- Area: Campanhas Ativas / Outbound
-- Data: 2026-07-09
-- Objetivo: Avaliar o desempenho de cada campanha de discagem ativa atraves
--           de metricas de volume (total de discagens), taxa de contato
--           (percentual de discagens que resultaram em contato real com o
--           cliente) e taxa de conversao (percentual de contatos que geraram
--           o resultado desejado).
--
-- Metricas Calculadas:
--   - Total de discagens por campanha
--   - Total de contatos realizados (fl_contato_realizado=1)
--   - Total de conversoes (fl_convertido=1)
--   - Taxa de contato: (contatos / discagens) * 100
--   - Taxa de conversao: (conversoes / contatos) * 100
--   - Taxa de conversao sobre discagens: (conversoes / discagens) * 100
--   - Media de tentativas por contato realizado
--   - Ranking de campanhas por efetividade
--
-- Como Interpretar:
--   - Taxa de contato abaixo de 30% pode indicar base desatualizada
--   - Taxa de conversao acima de 10% e considerada boa para outbound
--   - Alta media de tentativas aumenta o custo por contato
--   - Campanhas com baixa conversao e alta tentativa devem ser revisadas
-- =============================================================================

WITH
-- CTE 1: Base de discagens enriquecida com dados de campanha
base_discagem AS (
    SELECT
        fd.sk_discagem,
        fd.sk_campanha,
        fd.sk_cliente,
        CAST(1 AS INT)                        AS nr_tentativas,
        fd.fl_discagem_atendida                AS fl_contato_realizado,
        CAST(0 AS INT)                        AS fl_convertido,
        dc.nm_campanha,
        dc.st_campanha                        AS ds_status_campanha,
        dc.dt_inicio                          AS dt_inicio_campanha,
        dc.dt_fim                             AS dt_fim_campanha,
        CAST(NULL AS INT)                     AS nr_meta_contatos
    FROM db_gold.fato_discagem fd
    INNER JOIN db_gold.dim_campanha dc
        ON fd.sk_campanha = dc.sk_campanha
),

-- CTE 2: Metricas agregadas por campanha
metricas_campanha AS (
    SELECT
        sk_campanha,
        nm_campanha,
        CAST(NULL AS VARCHAR) AS ds_tipo_campanha,
        ds_status_campanha,
        dt_inicio_campanha,
        dt_fim_campanha,
        MAX(nr_meta_contatos)                                                     AS nr_meta_contatos,
        -- Volume total de discagens (linhas de fato_discagem)
        COUNT(sk_discagem)                                                        AS nr_total_discagens,
        -- Contatos realizados (numero de registros onde houve contato)
        SUM(fl_contato_realizado)                                                 AS nr_contatos_realizados,
        -- Conversoes
        SUM(fl_convertido)                                                        AS nr_conversoes,
        -- Total de tentativas acumuladas (considerando retentativas)
        SUM(nr_tentativas)                                                        AS nr_total_tentativas,
        -- Media de tentativas por discagem
        ROUND(AVG(CAST(nr_tentativas AS DOUBLE)), 2)                             AS nr_media_tentativas,
        -- Media de tentativas apenas para discagens que resultaram em contato
        ROUND(
            AVG(CASE WHEN fl_contato_realizado = 1
                     THEN CAST(nr_tentativas AS DOUBLE) END), 2
        )                                                                         AS nr_tentativas_ate_contato,
        -- Taxa de contato: % de discagens que geraram contato
        ROUND(
            100.0 * SUM(fl_contato_realizado)
            / NULLIF(COUNT(sk_discagem), 0), 2
        )                                                                         AS pct_taxa_contato,
        -- Taxa de conversao sobre contatos realizados
        ROUND(
            100.0 * SUM(fl_convertido)
            / NULLIF(SUM(fl_contato_realizado), 0), 2
        )                                                                         AS pct_taxa_conversao_sobre_contato,
        -- Taxa de conversao sobre total de discagens
        ROUND(
            100.0 * SUM(fl_convertido)
            / NULLIF(COUNT(sk_discagem), 0), 2
        )                                                                         AS pct_taxa_conversao_sobre_discagens,
        -- Discagens sem contato
        COUNT(sk_discagem) - SUM(fl_contato_realizado)                           AS nr_sem_contato,
        -- Contatos sem conversao
        SUM(fl_contato_realizado) - SUM(fl_convertido)                           AS nr_contatos_sem_conversao
    FROM base_discagem
    GROUP BY
        sk_campanha, nm_campanha,
        ds_status_campanha, dt_inicio_campanha, dt_fim_campanha
),

-- CTE 3: Metricas por cliente dentro de cada campanha (operador removido — nao existe em fato_discagem)
metricas_operador_campanha AS (
    SELECT
        sk_campanha,
        nm_campanha,
        sk_cliente,
        COUNT(sk_discagem)                                                        AS nr_discagens_operador,
        SUM(fl_contato_realizado)                                                 AS nr_contatos_operador,
        SUM(fl_convertido)                                                        AS nr_conversoes_operador,
        ROUND(
            100.0 * SUM(fl_contato_realizado)
            / NULLIF(COUNT(sk_discagem), 0), 2
        )                                                                         AS pct_contato_operador,
        ROUND(
            100.0 * SUM(fl_convertido)
            / NULLIF(SUM(fl_contato_realizado), 0), 2
        )                                                                         AS pct_conversao_operador,
        ROUND(AVG(CAST(nr_tentativas AS DOUBLE)), 2)                             AS nr_media_tentativas_operador,
        RANK() OVER (
            PARTITION BY sk_campanha
            ORDER BY SUM(fl_convertido) DESC
        )                                                                         AS rank_conversao_campanha
    FROM base_discagem
    GROUP BY sk_campanha, nm_campanha, sk_cliente
),

-- CTE 4: Distribuicao de tentativas (agrupamento por faixas)
distribuicao_tentativas AS (
    SELECT
        sk_campanha,
        nm_campanha,
        CASE
            WHEN nr_tentativas = 1       THEN '1_TENTATIVA'
            WHEN nr_tentativas = 2       THEN '2_TENTATIVAS'
            WHEN nr_tentativas BETWEEN 3 AND 5 THEN '3_A_5_TENTATIVAS'
            WHEN nr_tentativas > 5       THEN 'MAIS_DE_5'
            ELSE 'INDEFINIDO'
        END AS ds_faixa_tentativas,
        COUNT(sk_discagem)                                                        AS nr_discagens,
        SUM(fl_contato_realizado)                                                 AS nr_contatos,
        ROUND(
            100.0 * SUM(fl_contato_realizado)
            / NULLIF(COUNT(sk_discagem), 0), 2
        )                                                                         AS pct_contato_faixa
    FROM base_discagem
    GROUP BY sk_campanha, nm_campanha,
        CASE
            WHEN nr_tentativas = 1           THEN '1_TENTATIVA'
            WHEN nr_tentativas = 2           THEN '2_TENTATIVAS'
            WHEN nr_tentativas BETWEEN 3 AND 5 THEN '3_A_5_TENTATIVAS'
            WHEN nr_tentativas > 5           THEN 'MAIS_DE_5'
            ELSE 'INDEFINIDO'
        END
)

-- Resultado principal: Desempenho consolidado por campanha com rankings
SELECT
    mc.sk_campanha,
    mc.nm_campanha,
    mc.ds_tipo_campanha,
    mc.ds_status_campanha,
    mc.dt_inicio_campanha,
    mc.dt_fim_campanha,
    mc.nr_meta_contatos,
    mc.nr_total_discagens,
    mc.nr_contatos_realizados,
    mc.nr_conversoes,
    mc.nr_sem_contato,
    mc.nr_contatos_sem_conversao,
    mc.nr_total_tentativas,
    mc.nr_media_tentativas,
    mc.nr_tentativas_ate_contato,
    mc.pct_taxa_contato,
    mc.pct_taxa_conversao_sobre_contato,
    mc.pct_taxa_conversao_sobre_discagens,
    -- Atingimento da meta de contatos
    ROUND(
        100.0 * mc.nr_contatos_realizados
        / NULLIF(mc.nr_meta_contatos, 0), 2
    )                                                                             AS pct_atingimento_meta,
    -- Rankings globais
    RANK() OVER (ORDER BY mc.pct_taxa_contato DESC)                              AS rank_taxa_contato,
    RANK() OVER (ORDER BY mc.pct_taxa_conversao_sobre_contato DESC)              AS rank_taxa_conversao,
    RANK() OVER (ORDER BY mc.nr_conversoes DESC)                                 AS rank_volume_conversao,
    -- Score composto de efetividade (media ponderada dos rankings)
    ROUND(
        (RANK() OVER (ORDER BY mc.pct_taxa_contato DESC) * 0.35)
        + (RANK() OVER (ORDER BY mc.pct_taxa_conversao_sobre_contato DESC) * 0.50)
        + (RANK() OVER (ORDER BY mc.nr_media_tentativas ASC) * 0.15), 2
    )                                                                             AS score_efetividade_campanha,
    -- Semaforos de desempenho
    CASE
        WHEN mc.pct_taxa_contato >= 50 THEN 'VERDE'
        WHEN mc.pct_taxa_contato >= 30 THEN 'AMARELO'
        ELSE 'VERMELHO'
    END AS semaforo_contato,
    CASE
        WHEN mc.pct_taxa_conversao_sobre_contato >= 15 THEN 'VERDE'
        WHEN mc.pct_taxa_conversao_sobre_contato >= 8  THEN 'AMARELO'
        ELSE 'VERMELHO'
    END AS semaforo_conversao
FROM metricas_campanha mc
ORDER BY
    mc.pct_taxa_conversao_sobre_contato DESC,
    mc.nr_conversoes DESC
LIMIT 100;

-- =============================================================================
-- Query complementar: Top operadores por campanha (descomente para usar)
-- =============================================================================
-- SELECT
--     moc.nm_campanha,
--     moc.nm_operador,
--     moc.nr_discagens_operador,
--     moc.nr_contatos_operador,
--     moc.nr_conversoes_operador,
--     moc.pct_contato_operador,
--     moc.pct_conversao_operador,
--     moc.nr_media_tentativas_operador,
--     moc.rank_conversao_campanha
-- FROM metricas_operador_campanha moc
-- WHERE moc.rank_conversao_campanha <= 10
-- ORDER BY moc.nm_campanha, moc.rank_conversao_campanha
-- LIMIT 100;
