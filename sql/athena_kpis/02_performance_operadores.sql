-- =============================================================================
-- KPI: Performance de Operadores
-- Area: Gestao de Pessoas / Operacoes
-- Data: 2026-07-09
-- Objetivo: Avaliar o desempenho individual dos operadores por volume de
--           chamadas atendidas, TMA (Tempo Medio de Atendimento), nota de
--           qualidade media e produtividade (chamadas por hora trabalhada).
--           Exclui supervisores do ranking (fl_supervisor=0).
--
-- Metricas Calculadas:
--   - Total de chamadas atendidas por operador
--   - TMA individual (somente fl_duracao_valida=1)
--   - Nota media de qualidade (cruzamento com fato_qualidade)
--   - Calls por hora trabalhada (cruzamento com fato_jornada_operador)
--   - Ranking geral de produtividade
--   - Desvio em relacao a media da equipe
--
-- Como Interpretar:
--   - Operadores com TMA muito acima da media precisam de coaching
--   - Operadores com alta producao mas baixa qualidade requerem calibragem
--   - Calls/hora: benchmarks variam por segmento (media tipica: 8-12 calls/hora)
--   - Ranking ajuda a identificar top performers e casos criticos
-- =============================================================================

WITH
-- CTE 1: Filtra apenas agentes (exclui supervisores) e enriquece com dimensoes
operadores_agentes AS (
    SELECT
        do2.sk_operador,
        do2.nm_operador,
        do2.ds_cargo,
        do2.fl_supervisor
    FROM db_gold.dim_operador do2
    WHERE do2.fl_supervisor = 0  -- Apenas agentes, sem supervisores
),

-- CTE 2: Chamadas atendidas por operador com filtro de duracao valida
chamadas_operador AS (
    SELECT
        fc.sk_operador,
        dd.nr_ano,
        dd.nr_mes,
        -- Total de chamadas atendidas (sem filtro de duracao para volume real)
        COUNT(fc.sk_chamada)                                                       AS nr_total_chamadas,
        -- Chamadas com duracao valida para calculo de TMA
        COUNT(CASE WHEN fc.fl_duracao_valida = 1 THEN fc.sk_chamada END)          AS nr_chamadas_validas,
        -- TMA em segundos usando apenas chamadas com duracao valida
        ROUND(
            AVG(CASE WHEN fc.fl_duracao_valida = 1
                     THEN CAST(fc.nr_duracao_segundos AS DOUBLE) END), 2
        )                                                                          AS nr_tma_segundos,
        ROUND(
            AVG(CASE WHEN fc.fl_duracao_valida = 1
                     THEN CAST(fc.nr_duracao_minutos AS DOUBLE) END), 2
        )                                                                          AS nr_tma_minutos,
        -- Chamadas completas (qualidade de entrega)
        SUM(fc.fl_chamada_completa)                                                AS nr_chamadas_completas,
        ROUND(
            100.0 * SUM(fc.fl_chamada_completa) / NULLIF(COUNT(fc.sk_chamada), 0), 2
        )                                                                          AS pct_chamadas_completas
    FROM db_gold.fato_chamada fc
    INNER JOIN db_gold.dim_data dd
        ON fc.sk_data_inicio = dd.sk_data
    INNER JOIN operadores_agentes oa
        ON fc.sk_operador = oa.sk_operador
    GROUP BY fc.sk_operador, dd.nr_ano, dd.nr_mes
),

-- CTE 3: Notas de qualidade por operador
qualidade_operador AS (
    SELECT
        fq.sk_operador,
        dd.nr_ano,
        dd.nr_mes,
        COUNT(fq.sk_qualidade)                                                     AS nr_avaliacoes,
        ROUND(AVG(fq.nr_nota_geral), 2)                                            AS nr_nota_geral_media,
        ROUND(AVG(fq.nr_nota_comunicacao), 2)                                      AS nr_nota_comunicacao_media,
        ROUND(AVG(fq.nr_nota_resolucao), 2)                                        AS nr_nota_resolucao_media,
        -- Percentual de avaliacoes excelentes (nota = 10)
        ROUND(
            100.0 * COUNT(CASE WHEN fq.nr_nota_geral = 10 THEN 1 END)
            / NULLIF(COUNT(fq.sk_qualidade), 0), 2
        )                                                                          AS pct_excelentes,
        -- Percentual de avaliacoes criticas (nota < 6)
        ROUND(
            100.0 * COUNT(CASE WHEN fq.nr_nota_geral < 6 THEN 1 END)
            / NULLIF(COUNT(fq.sk_qualidade), 0), 2
        )                                                                          AS pct_criticas
    FROM db_gold.fato_qualidade fq
    INNER JOIN db_gold.dim_data dd
        ON fq.sk_chamada IN (
            SELECT fc2.sk_chamada
            FROM db_gold.fato_chamada fc2
            WHERE fc2.sk_data_inicio = dd.sk_data
        )
    INNER JOIN operadores_agentes oa
        ON fq.sk_operador = oa.sk_operador
    GROUP BY fq.sk_operador, dd.nr_ano, dd.nr_mes
),

-- CTE 3b: Simplificada para qualidade sem subquery correlacionada
qualidade_operador_simples AS (
    SELECT
        fq.sk_operador,
        COUNT(fq.sk_qualidade)                                                     AS nr_avaliacoes,
        ROUND(AVG(fq.nr_nota_geral), 2)                                            AS nr_nota_geral_media,
        ROUND(AVG(fq.nr_nota_comunicacao), 2)                                      AS nr_nota_comunicacao_media,
        ROUND(AVG(fq.nr_nota_resolucao), 2)                                        AS nr_nota_resolucao_media,
        ROUND(
            100.0 * COUNT(CASE WHEN fq.nr_nota_geral = 10 THEN 1 END)
            / NULLIF(COUNT(fq.sk_qualidade), 0), 2
        )                                                                          AS pct_excelentes,
        ROUND(
            100.0 * COUNT(CASE WHEN fq.nr_nota_geral < 6 THEN 1 END)
            / NULLIF(COUNT(fq.sk_qualidade), 0), 2
        )                                                                          AS pct_criticas
    FROM db_gold.fato_qualidade fq
    INNER JOIN operadores_agentes oa
        ON fq.sk_operador = oa.sk_operador
    GROUP BY fq.sk_operador
),

-- CTE 4: Jornada e horas trabalhadas por operador
jornada_operador AS (
    SELECT
        fjo.sk_operador,
        SUM(fjo.nr_horas_trabalhadas)                                              AS nr_horas_total,
        SUM(fjo.nr_chamadas_atendidas)                                             AS nr_chamadas_jornada,
        SUM(fjo.nr_tickets_resolvidos)                                             AS nr_tickets_resolvidos,
        -- Dias com presenca confirmada
        COUNT(CASE WHEN fjo.st_presenca = 'PRESENTE' THEN 1 END)                  AS nr_dias_presente,
        COUNT(fjo.sk_jornada)                                                      AS nr_dias_total,
        ROUND(
            100.0 * COUNT(CASE WHEN fjo.st_presenca = 'PRESENTE' THEN 1 END)
            / NULLIF(COUNT(fjo.sk_jornada), 0), 2
        )                                                                          AS pct_presenca
    FROM db_gold.fato_jornada_operador fjo
    INNER JOIN operadores_agentes oa
        ON fjo.sk_operador = oa.sk_operador
    GROUP BY fjo.sk_operador
),

-- CTE 5: Consolidacao de todas as metricas por operador
consolidado_operador AS (
    SELECT
        oa.sk_operador,
        oa.nm_operador,
        oa.ds_cargo,
        -- Volume de chamadas
        COALESCE(co.nr_total_chamadas, 0)                                          AS nr_total_chamadas,
        COALESCE(co.nr_chamadas_validas, 0)                                        AS nr_chamadas_validas,
        COALESCE(co.nr_tma_segundos, 0)                                            AS nr_tma_segundos,
        COALESCE(co.nr_tma_minutos, 0)                                             AS nr_tma_minutos,
        COALESCE(co.pct_chamadas_completas, 0)                                     AS pct_chamadas_completas,
        -- Qualidade
        COALESCE(qo.nr_avaliacoes, 0)                                              AS nr_avaliacoes,
        COALESCE(qo.nr_nota_geral_media, 0)                                        AS nr_nota_geral_media,
        COALESCE(qo.nr_nota_comunicacao_media, 0)                                  AS nr_nota_comunicacao_media,
        COALESCE(qo.nr_nota_resolucao_media, 0)                                    AS nr_nota_resolucao_media,
        COALESCE(qo.pct_excelentes, 0)                                             AS pct_excelentes,
        COALESCE(qo.pct_criticas, 0)                                               AS pct_criticas,
        -- Jornada e produtividade
        COALESCE(jo.nr_horas_total, 0)                                             AS nr_horas_trabalhadas,
        COALESCE(jo.pct_presenca, 0)                                               AS pct_presenca,
        -- Calls por hora: produtividade principal
        ROUND(
            COALESCE(co.nr_total_chamadas, 0)
            / NULLIF(jo.nr_horas_total, 0), 2
        )                                                                          AS nr_calls_por_hora
    FROM operadores_agentes oa
    LEFT JOIN (
        SELECT sk_operador,
               SUM(nr_total_chamadas)     AS nr_total_chamadas,
               SUM(nr_chamadas_validas)   AS nr_chamadas_validas,
               ROUND(AVG(nr_tma_segundos), 2) AS nr_tma_segundos,
               ROUND(AVG(nr_tma_minutos), 2)  AS nr_tma_minutos,
               ROUND(AVG(pct_chamadas_completas), 2) AS pct_chamadas_completas
        FROM chamadas_operador
        GROUP BY sk_operador
    ) co ON oa.sk_operador = co.sk_operador
    LEFT JOIN qualidade_operador_simples qo
        ON oa.sk_operador = qo.sk_operador
    LEFT JOIN jornada_operador jo
        ON oa.sk_operador = jo.sk_operador
),

-- CTE 6: Medias da equipe para comparativo de desvio
medias_equipe AS (
    SELECT
        AVG(nr_total_chamadas)   AS media_chamadas_equipe,
        AVG(nr_tma_segundos)     AS media_tma_equipe,
        AVG(nr_nota_geral_media) AS media_nota_equipe,
        AVG(nr_calls_por_hora)   AS media_calls_hora_equipe
    FROM consolidado_operador
    WHERE nr_total_chamadas > 0
)

-- Resultado final: Ranking de operadores com desvio em relacao a media
SELECT
    co.sk_operador,
    co.nm_operador,
    co.ds_cargo,
    co.nr_total_chamadas,
    co.nr_tma_segundos,
    co.nr_tma_minutos,
    co.nr_nota_geral_media,
    co.nr_nota_comunicacao_media,
    co.nr_nota_resolucao_media,
    co.nr_avaliacoes,
    co.pct_excelentes,
    co.pct_criticas,
    co.nr_horas_trabalhadas,
    co.pct_presenca,
    co.nr_calls_por_hora,
    co.pct_chamadas_completas,
    -- Desvio percentual do TMA em relacao a media da equipe
    ROUND(
        100.0 * (co.nr_tma_segundos - me.media_tma_equipe)
        / NULLIF(me.media_tma_equipe, 0), 2
    )                                                                              AS pct_desvio_tma,
    -- Desvio percentual da nota em relacao a media da equipe
    ROUND(
        100.0 * (co.nr_nota_geral_media - me.media_nota_equipe)
        / NULLIF(me.media_nota_equipe, 0), 2
    )                                                                              AS pct_desvio_nota,
    -- Ranking por volume de chamadas
    RANK() OVER (ORDER BY co.nr_total_chamadas DESC)                               AS rank_volume,
    -- Ranking por qualidade
    RANK() OVER (ORDER BY co.nr_nota_geral_media DESC)                             AS rank_qualidade,
    -- Ranking por produtividade (calls/hora)
    RANK() OVER (ORDER BY co.nr_calls_por_hora DESC)                               AS rank_produtividade,
    -- Score composto: media ponderada de volume(30%) + qualidade(40%) + produtividade(30%)
    ROUND(
        (RANK() OVER (ORDER BY co.nr_total_chamadas DESC) * 0.30)
        + (RANK() OVER (ORDER BY co.nr_nota_geral_media DESC) * 0.40)
        + (RANK() OVER (ORDER BY co.nr_calls_por_hora DESC) * 0.30), 2
    )                                                                              AS score_ranking_composto
FROM consolidado_operador co
CROSS JOIN medias_equipe me
WHERE co.nr_total_chamadas > 0
ORDER BY
    rank_qualidade ASC,
    co.nr_total_chamadas DESC
LIMIT 100;
