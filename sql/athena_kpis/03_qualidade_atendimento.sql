-- =============================================================================
-- KPI: Qualidade de Atendimento
-- Area: Qualidade / Gestao de Pessoas
-- Data: 2026-07-09
-- Objetivo: Analisar as notas medias de qualidade por operador, por supervisor
--           responsavel e por periodo. Identificar distribuicao por bandas de
--           qualidade e destacar operadores abaixo da media da equipe.
--
-- Metricas Calculadas:
--   - Nota media geral, de comunicacao e resolucao por operador
--   - Nota media por supervisor (sua equipe)
--   - Distribuicao por bandas: Ruim (<6), Regular (6-7), Bom (8-9), Excelente (=10)
--   - Operadores abaixo da media da equipe (necessitam atencao)
--   - Tendencia mensal de qualidade
--   - Percentual de conformidade (nota >= 7)
--
-- Como Interpretar:
--   - Nota media da equipe abaixo de 7 indica problema estrutural de qualidade
--   - Alta concentracao em "Ruim" exige revisao de treinamento e processos
--   - Supervisores com equipe abaixo da media podem precisar de suporte gerencial
--   - Acompanhar tendencia mensal para verificar evolucao apos intervencoes
-- =============================================================================

WITH
-- CTE 1: Base de avaliacoes de qualidade com dados de operador e supervisor
base_qualidade AS (
    SELECT
        fq.sk_avaliacao AS sk_qualidade,
        fq.sk_chamada,
        fq.sk_operador_avaliado AS sk_operador,
        fq.sk_avaliador AS sk_supervisor,
        fq.nr_nota AS nr_nota_geral,
        CAST(NULL AS DOUBLE) AS nr_nota_comunicacao,
        CAST(NULL AS DOUBLE) AS nr_nota_resolucao,
        -- Identifica a banda de qualidade para cada avaliacao
        CASE
            WHEN fq.nr_nota < 6         THEN 'RUIM'
            WHEN fq.nr_nota BETWEEN 6 AND 7.99 THEN 'REGULAR'
            WHEN fq.nr_nota BETWEEN 8 AND 9.99 THEN 'BOM'
            WHEN fq.nr_nota = 10        THEN 'EXCELENTE'
            ELSE 'NAO_CLASSIFICADO'
        END AS ds_banda_qualidade,
        -- Indica se a avaliacao esta em conformidade (nota >= 7)
        CASE WHEN fq.nr_nota >= 7 THEN 1 ELSE 0 END AS fl_conforme,
        do2.nm_operador,
        do2.ds_faixa_tempo_casa AS ds_cargo,
        CAST(0 AS INT) AS fl_e_supervisor,
        -- Dados do supervisor responsavel
        ds.nm_supervisor,
        dd.dt_completa,
        dd.nr_ano,
        dd.nr_mes,
        dd.nr_dia,
        DATE_FORMAT(dd.dt_completa, '%Y-%m') AS ds_ano_mes
    FROM db_gold.fato_qualidade fq
    INNER JOIN db_gold.dim_operador do2
        ON fq.sk_operador_avaliado = do2.sk_operador
    LEFT JOIN db_gold.dim_supervisor ds
        ON fq.sk_avaliador = ds.sk_supervisor
    INNER JOIN db_gold.fato_chamada fc
        ON fq.sk_chamada = fc.sk_chamada
    INNER JOIN db_gold.dim_data dd
        ON fc.sk_data_inicio = dd.sk_data
    WHERE do2.fl_operador_ativo = 1  -- Apenas agentes avaliados
),

-- CTE 2: Metricas agregadas por operador
qualidade_por_operador AS (
    SELECT
        sk_operador,
        nm_operador,
        ds_cargo,
        nm_supervisor,
        COUNT(sk_qualidade)                                                        AS nr_total_avaliacoes,
        ROUND(AVG(nr_nota_geral), 2)                                               AS nr_nota_geral_media,
        ROUND(AVG(nr_nota_comunicacao), 2)                                         AS nr_nota_comunicacao_media,
        ROUND(AVG(nr_nota_resolucao), 2)                                           AS nr_nota_resolucao_media,
        -- Distribuicao por bandas (contagem absoluta)
        COUNT(CASE WHEN ds_banda_qualidade = 'EXCELENTE' THEN 1 END)              AS nr_excelente,
        COUNT(CASE WHEN ds_banda_qualidade = 'BOM'       THEN 1 END)              AS nr_bom,
        COUNT(CASE WHEN ds_banda_qualidade = 'REGULAR'   THEN 1 END)              AS nr_regular,
        COUNT(CASE WHEN ds_banda_qualidade = 'RUIM'      THEN 1 END)              AS nr_ruim,
        -- Distribuicao por bandas (percentual)
        ROUND(100.0 * COUNT(CASE WHEN ds_banda_qualidade = 'EXCELENTE' THEN 1 END)
              / NULLIF(COUNT(sk_qualidade), 0), 2)                                AS pct_excelente,
        ROUND(100.0 * COUNT(CASE WHEN ds_banda_qualidade = 'BOM'       THEN 1 END)
              / NULLIF(COUNT(sk_qualidade), 0), 2)                                AS pct_bom,
        ROUND(100.0 * COUNT(CASE WHEN ds_banda_qualidade = 'REGULAR'   THEN 1 END)
              / NULLIF(COUNT(sk_qualidade), 0), 2)                                AS pct_regular,
        ROUND(100.0 * COUNT(CASE WHEN ds_banda_qualidade = 'RUIM'      THEN 1 END)
              / NULLIF(COUNT(sk_qualidade), 0), 2)                                AS pct_ruim,
        -- Taxa de conformidade (nota >= 7)
        ROUND(100.0 * SUM(fl_conforme)
              / NULLIF(COUNT(sk_qualidade), 0), 2)                                AS pct_conformidade,
        -- Notas extremas
        MIN(nr_nota_geral)                                                         AS nr_nota_minima,
        MAX(nr_nota_geral)                                                         AS nr_nota_maxima
    FROM base_qualidade
    GROUP BY sk_operador, nm_operador, ds_cargo, nm_supervisor
),

-- CTE 3: Media global da equipe (referencia para comparativo)
media_global AS (
    SELECT
        ROUND(AVG(nr_nota_geral), 2)       AS media_equipe_geral,
        ROUND(AVG(nr_nota_comunicacao), 2) AS media_equipe_comunicacao,
        ROUND(AVG(nr_nota_resolucao), 2)   AS media_equipe_resolucao
    FROM base_qualidade
),

-- CTE 4: Qualidade por supervisor (desempenho da equipe de cada supervisor)
qualidade_por_supervisor AS (
    SELECT
        nm_supervisor,
        COUNT(DISTINCT sk_operador)                                               AS nr_agentes_equipe,
        COUNT(sk_qualidade)                                                       AS nr_total_avaliacoes_equipe,
        ROUND(AVG(nr_nota_geral), 2)                                              AS nr_nota_media_equipe,
        ROUND(AVG(nr_nota_comunicacao), 2)                                        AS nr_nota_comunicacao_equipe,
        ROUND(AVG(nr_nota_resolucao), 2)                                          AS nr_nota_resolucao_equipe,
        ROUND(100.0 * SUM(fl_conforme)
              / NULLIF(COUNT(sk_qualidade), 0), 2)                               AS pct_conformidade_equipe,
        COUNT(CASE WHEN ds_banda_qualidade = 'RUIM' THEN 1 END)                  AS nr_avaliacoes_ruins,
        ROUND(100.0 * COUNT(CASE WHEN ds_banda_qualidade = 'RUIM' THEN 1 END)
              / NULLIF(COUNT(sk_qualidade), 0), 2)                               AS pct_ruins_equipe
    FROM base_qualidade
    WHERE nm_supervisor IS NOT NULL
    GROUP BY nm_supervisor
),

-- CTE 5: Tendencia mensal de qualidade
tendencia_mensal AS (
    SELECT
        ds_ano_mes,
        nr_ano,
        nr_mes,
        COUNT(sk_qualidade)                                                       AS nr_avaliacoes_mes,
        ROUND(AVG(nr_nota_geral), 2)                                              AS nr_nota_media_mes,
        ROUND(AVG(nr_nota_comunicacao), 2)                                        AS nr_nota_comunicacao_mes,
        ROUND(AVG(nr_nota_resolucao), 2)                                          AS nr_nota_resolucao_mes,
        ROUND(100.0 * SUM(fl_conforme)
              / NULLIF(COUNT(sk_qualidade), 0), 2)                               AS pct_conformidade_mes,
        COUNT(CASE WHEN ds_banda_qualidade = 'EXCELENTE' THEN 1 END)             AS nr_excelente_mes,
        COUNT(CASE WHEN ds_banda_qualidade = 'RUIM'      THEN 1 END)             AS nr_ruim_mes
    FROM base_qualidade
    GROUP BY ds_ano_mes, nr_ano, nr_mes
)

-- Resultado principal: Operadores com notas, bandas e comparativo com media
SELECT
    qpo.sk_operador,
    qpo.nm_operador,
    qpo.ds_cargo,
    qpo.nm_supervisor,
    qpo.nr_total_avaliacoes,
    qpo.nr_nota_geral_media,
    qpo.nr_nota_comunicacao_media,
    qpo.nr_nota_resolucao_media,
    mg.media_equipe_geral,
    -- Desvio em relacao a media da equipe
    ROUND(qpo.nr_nota_geral_media - mg.media_equipe_geral, 2)                    AS desvio_vs_media,
    -- Sinaliza operadores abaixo da media
    CASE
        WHEN qpo.nr_nota_geral_media < mg.media_equipe_geral THEN 'ABAIXO_DA_MEDIA'
        ELSE 'NA_MEDIA_OU_ACIMA'
    END AS ds_situacao_vs_media,
    -- Distribuicao por bandas
    qpo.nr_excelente,
    qpo.nr_bom,
    qpo.nr_regular,
    qpo.nr_ruim,
    qpo.pct_excelente,
    qpo.pct_bom,
    qpo.pct_regular,
    qpo.pct_ruim,
    qpo.pct_conformidade,
    qpo.nr_nota_minima,
    qpo.nr_nota_maxima,
    -- Banda predominante do operador
    CASE
        WHEN qpo.pct_excelente >= 50 THEN 'PREDOMINANTEMENTE_EXCELENTE'
        WHEN qpo.pct_bom       >= 50 THEN 'PREDOMINANTEMENTE_BOM'
        WHEN qpo.pct_regular   >= 50 THEN 'PREDOMINANTEMENTE_REGULAR'
        WHEN qpo.pct_ruim      >= 50 THEN 'PREDOMINANTEMENTE_RUIM'
        ELSE 'MISTO'
    END AS ds_perfil_qualidade,
    -- Ranking de qualidade
    RANK() OVER (ORDER BY qpo.nr_nota_geral_media DESC)                          AS rank_qualidade
FROM qualidade_por_operador qpo
CROSS JOIN media_global mg
ORDER BY
    qpo.nr_nota_geral_media DESC,
    qpo.nr_total_avaliacoes DESC
LIMIT 100;

-- =============================================================================
-- Query complementar 1: Desempenho por supervisor (descomente para usar)
-- =============================================================================
-- SELECT
--     qps.nm_supervisor,
--     qps.nr_agentes_equipe,
--     qps.nr_total_avaliacoes_equipe,
--     qps.nr_nota_media_equipe,
--     qps.nr_nota_comunicacao_equipe,
--     qps.nr_nota_resolucao_equipe,
--     qps.pct_conformidade_equipe,
--     qps.nr_avaliacoes_ruins,
--     qps.pct_ruins_equipe,
--     RANK() OVER (ORDER BY qps.nr_nota_media_equipe DESC) AS rank_supervisor
-- FROM qualidade_por_supervisor qps
-- ORDER BY qps.nr_nota_media_equipe DESC
-- LIMIT 50;

-- =============================================================================
-- Query complementar 2: Tendencia mensal (descomente para usar)
-- =============================================================================
-- SELECT
--     tm.ds_ano_mes,
--     tm.nr_avaliacoes_mes,
--     tm.nr_nota_media_mes,
--     tm.nr_nota_comunicacao_mes,
--     tm.nr_nota_resolucao_mes,
--     tm.pct_conformidade_mes,
--     tm.nr_excelente_mes,
--     tm.nr_ruim_mes
-- FROM tendencia_mensal tm
-- ORDER BY tm.nr_ano ASC, tm.nr_mes ASC
-- LIMIT 24;
