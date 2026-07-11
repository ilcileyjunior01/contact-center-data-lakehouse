-- =============================================================================
-- KPI: Jornada e Absenteismo de Operadores
-- Area: Gestao de Pessoas / Workforce Management
-- Data: 2026-07-09
-- Objetivo: Analisar a presenca e o absenteismo de cada operador, calcular
--           a produtividade horaria (chamadas atendidas por hora trabalhada)
--           e acompanhar a tendencia mensal de ausencias para identificar
--           padroes e subsidiar decisoes de gestao de equipe.
--
-- Metricas Calculadas:
--   - Total de dias presentes e ausentes por operador
--   - Taxa de presenca (%) e absenteismo (%)
--   - Total de horas trabalhadas no periodo
--   - Chamadas por hora (produtividade horaria)
--   - Tickets resolvidos por hora
--   - Tendencia mensal de absenteismo por operador e equipe
--   - Distribuicao de ausencias por dia da semana
--
-- Como Interpretar:
--   - Absenteismo acima de 10% ao mes requer acompanhamento gerencial
--   - Quedas repentinas na produtividade horaria indicam dificuldades tecnicas
--   - Ausencias concentradas nas segundas ou sextas podem indicar padrao comportamental
--   - Comparar produtividade vs presenca para avaliar engajamento real
-- =============================================================================

WITH
-- CTE 1: Base de jornada com dados enriquecidos do operador e data
base_jornada AS (
    SELECT
        fjo.sk_jornada,
        fjo.sk_operador,
        fjo.sk_data,
        fjo.nr_horas_trabalhadas,
        fjo.nr_chamadas_atendidas,
        fjo.nr_tickets_resolvidos,
        fjo.st_presenca,
        do2.nm_operador,
        do2.ds_faixa_tempo_casa AS ds_cargo,
        CAST(0 AS INT) AS fl_supervisor,
        dd.dt_completa,
        dd.nr_ano,
        dd.nr_mes,
        dd.nr_dia,
        dd.ds_dia_semana,
        dd.fl_fim_de_semana,
        DATE_FORMAT(dd.dt_completa, '%Y-%m')  AS ds_ano_mes,
        -- Classifica o tipo de ocorrencia de presenca
        CASE
            WHEN fjo.st_presenca = 'PRESENTE'   THEN 'PRESENTE'
            WHEN fjo.st_presenca = 'AUSENTE'    THEN 'AUSENTE'
            WHEN fjo.st_presenca = 'FERIAS'     THEN 'FERIAS'
            WHEN fjo.st_presenca = 'AFASTADO'   THEN 'AFASTADO'
            WHEN fjo.st_presenca = 'FOLGA'      THEN 'FOLGA'
            ELSE 'OUTRO'
        END AS ds_tipo_ocorrencia,
        -- Flag de dia util (presenca esperada)
        CASE WHEN dd.fl_fim_de_semana = 0 THEN 1 ELSE 0 END AS fl_dia_util
    FROM db_gold.fato_jornada_operador fjo
    INNER JOIN db_gold.dim_operador do2
        ON fjo.sk_operador = do2.sk_operador
    INNER JOIN db_gold.dim_data dd
        ON fjo.sk_data = dd.sk_data
    WHERE do2.fl_operador_ativo = 1  -- Apenas agentes (exclui supervisores)
),

-- CTE 2: Resumo de presenca e produtividade por operador (geral)
presenca_geral_operador AS (
    SELECT
        sk_operador,
        nm_operador,
        ds_cargo,
        -- Contagens de status de presenca
        COUNT(sk_jornada)                                                         AS nr_dias_registrados,
        COUNT(CASE WHEN fl_dia_util = 1 THEN 1 END)                              AS nr_dias_uteis,
        COUNT(CASE WHEN ds_tipo_ocorrencia = 'PRESENTE'  THEN 1 END)             AS nr_dias_presente,
        COUNT(CASE WHEN ds_tipo_ocorrencia = 'AUSENTE'   THEN 1 END)             AS nr_dias_ausente,
        COUNT(CASE WHEN ds_tipo_ocorrencia = 'FERIAS'    THEN 1 END)             AS nr_dias_ferias,
        COUNT(CASE WHEN ds_tipo_ocorrencia = 'AFASTADO'  THEN 1 END)             AS nr_dias_afastado,
        COUNT(CASE WHEN ds_tipo_ocorrencia = 'FOLGA'     THEN 1 END)             AS nr_dias_folga,
        -- Taxa de presenca: dias presente / dias uteis esperados
        ROUND(
            100.0 * COUNT(CASE WHEN ds_tipo_ocorrencia = 'PRESENTE' THEN 1 END)
            / NULLIF(COUNT(CASE WHEN fl_dia_util = 1 THEN 1 END), 0), 2
        )                                                                         AS pct_taxa_presenca,
        -- Taxa de absenteismo (exclui ferias e afastamentos medicos)
        ROUND(
            100.0 * COUNT(CASE WHEN ds_tipo_ocorrencia = 'AUSENTE' THEN 1 END)
            / NULLIF(COUNT(CASE WHEN fl_dia_util = 1 THEN 1 END), 0), 2
        )                                                                         AS pct_taxa_absenteismo,
        -- Horas e produtividade
        ROUND(SUM(CASE WHEN ds_tipo_ocorrencia = 'PRESENTE'
                       THEN CAST(nr_horas_trabalhadas AS DOUBLE) END), 2)        AS nr_horas_totais,
        SUM(CASE WHEN ds_tipo_ocorrencia = 'PRESENTE'
                  THEN nr_chamadas_atendidas END)                                 AS nr_chamadas_totais,
        SUM(CASE WHEN ds_tipo_ocorrencia = 'PRESENTE'
                  THEN nr_tickets_resolvidos END)                                 AS nr_tickets_totais,
        -- Produtividade horaria: chamadas / hora
        ROUND(
            CAST(SUM(CASE WHEN ds_tipo_ocorrencia = 'PRESENTE'
                          THEN nr_chamadas_atendidas END) AS DOUBLE)
            / NULLIF(SUM(CASE WHEN ds_tipo_ocorrencia = 'PRESENTE'
                              THEN CAST(nr_horas_trabalhadas AS DOUBLE) END), 0), 2
        )                                                                         AS nr_chamadas_por_hora,
        -- Tickets por hora
        ROUND(
            CAST(SUM(CASE WHEN ds_tipo_ocorrencia = 'PRESENTE'
                          THEN nr_tickets_resolvidos END) AS DOUBLE)
            / NULLIF(SUM(CASE WHEN ds_tipo_ocorrencia = 'PRESENTE'
                              THEN CAST(nr_horas_trabalhadas AS DOUBLE) END), 0), 2
        )                                                                         AS nr_tickets_por_hora,
        -- Media de horas por dia presente
        ROUND(
            SUM(CASE WHEN ds_tipo_ocorrencia = 'PRESENTE'
                     THEN CAST(nr_horas_trabalhadas AS DOUBLE) END)
            / NULLIF(COUNT(CASE WHEN ds_tipo_ocorrencia = 'PRESENTE' THEN 1 END), 0), 2
        )                                                                         AS nr_horas_medias_por_dia
    FROM base_jornada
    GROUP BY sk_operador, nm_operador, ds_cargo
),

-- CTE 3: Tendencia mensal de absenteismo por operador
tendencia_mensal_operador AS (
    SELECT
        sk_operador,
        nm_operador,
        ds_ano_mes,
        nr_ano,
        nr_mes,
        COUNT(CASE WHEN fl_dia_util = 1 THEN 1 END)                              AS nr_dias_uteis_mes,
        COUNT(CASE WHEN ds_tipo_ocorrencia = 'PRESENTE'  THEN 1 END)             AS nr_presente_mes,
        COUNT(CASE WHEN ds_tipo_ocorrencia = 'AUSENTE'   THEN 1 END)             AS nr_ausente_mes,
        ROUND(
            100.0 * COUNT(CASE WHEN ds_tipo_ocorrencia = 'AUSENTE' THEN 1 END)
            / NULLIF(COUNT(CASE WHEN fl_dia_util = 1 THEN 1 END), 0), 2
        )                                                                         AS pct_absenteismo_mes,
        ROUND(SUM(CASE WHEN ds_tipo_ocorrencia = 'PRESENTE'
                       THEN CAST(nr_horas_trabalhadas AS DOUBLE) END), 2)        AS nr_horas_mes,
        SUM(CASE WHEN ds_tipo_ocorrencia = 'PRESENTE'
                  THEN nr_chamadas_atendidas END)                                 AS nr_chamadas_mes
    FROM base_jornada
    GROUP BY sk_operador, nm_operador, ds_ano_mes, nr_ano, nr_mes
),

-- CTE 4: Distribuicao de ausencias por dia da semana
ausencias_dia_semana AS (
    SELECT
        ds_dia_semana,
        COUNT(CASE WHEN ds_tipo_ocorrencia = 'AUSENTE' THEN 1 END)               AS nr_ausencias,
        COUNT(sk_jornada)                                                         AS nr_registros_dia,
        ROUND(
            100.0 * COUNT(CASE WHEN ds_tipo_ocorrencia = 'AUSENTE' THEN 1 END)
            / NULLIF(COUNT(sk_jornada), 0), 2
        )                                                                         AS pct_ausencia_dia_semana
    FROM base_jornada
    WHERE fl_dia_util = 1
    GROUP BY ds_dia_semana
),

-- CTE 5: Medias da equipe para referencia
medias_equipe AS (
    SELECT
        ROUND(AVG(pct_taxa_presenca), 2)    AS media_presenca_equipe,
        ROUND(AVG(pct_taxa_absenteismo), 2) AS media_absenteismo_equipe,
        ROUND(AVG(nr_chamadas_por_hora), 2) AS media_calls_hora_equipe,
        ROUND(AVG(nr_horas_totais), 2)      AS media_horas_equipe
    FROM presenca_geral_operador
    WHERE nr_dias_registrados > 0
)

-- Resultado principal: Presenca e produtividade por operador
SELECT
    pgo.sk_operador,
    pgo.nm_operador,
    pgo.ds_cargo,
    pgo.nr_dias_registrados,
    pgo.nr_dias_uteis,
    pgo.nr_dias_presente,
    pgo.nr_dias_ausente,
    pgo.nr_dias_ferias,
    pgo.nr_dias_afastado,
    pgo.nr_dias_folga,
    pgo.pct_taxa_presenca,
    pgo.pct_taxa_absenteismo,
    pgo.nr_horas_totais,
    pgo.nr_chamadas_totais,
    pgo.nr_tickets_totais,
    pgo.nr_chamadas_por_hora,
    pgo.nr_tickets_por_hora,
    pgo.nr_horas_medias_por_dia,
    -- Medias da equipe para comparativo
    me.media_presenca_equipe,
    me.media_absenteismo_equipe,
    me.media_calls_hora_equipe,
    -- Desvio vs media da equipe
    ROUND(pgo.pct_taxa_presenca - me.media_presenca_equipe, 2)                   AS desvio_presenca_vs_media,
    ROUND(pgo.nr_chamadas_por_hora - me.media_calls_hora_equipe, 2)              AS desvio_produtividade_vs_media,
    -- Rankings
    RANK() OVER (ORDER BY pgo.pct_taxa_presenca DESC)                            AS rank_presenca,
    RANK() OVER (ORDER BY pgo.pct_taxa_absenteismo ASC)                          AS rank_menor_absenteismo,
    RANK() OVER (ORDER BY pgo.nr_chamadas_por_hora DESC)                         AS rank_produtividade_hora,
    -- Semaforos
    CASE
        WHEN pgo.pct_taxa_absenteismo <= 5  THEN 'VERDE'
        WHEN pgo.pct_taxa_absenteismo <= 10 THEN 'AMARELO'
        ELSE 'VERMELHO'
    END AS semaforo_absenteismo,
    CASE
        WHEN pgo.nr_chamadas_por_hora >= me.media_calls_hora_equipe * 1.1 THEN 'ACIMA_DA_MEDIA'
        WHEN pgo.nr_chamadas_por_hora >= me.media_calls_hora_equipe * 0.9 THEN 'NA_MEDIA'
        ELSE 'ABAIXO_DA_MEDIA'
    END AS ds_situacao_produtividade
FROM presenca_geral_operador pgo
CROSS JOIN medias_equipe me
WHERE pgo.nr_dias_registrados > 0
ORDER BY
    pgo.pct_taxa_absenteismo DESC,
    pgo.nr_chamadas_por_hora DESC
LIMIT 100;

-- =============================================================================
-- Query complementar 1: Tendencia mensal de absenteismo (descomente para usar)
-- =============================================================================
-- SELECT
--     tmo.nm_operador,
--     tmo.ds_ano_mes,
--     tmo.nr_dias_uteis_mes,
--     tmo.nr_presente_mes,
--     tmo.nr_ausente_mes,
--     tmo.pct_absenteismo_mes,
--     tmo.nr_horas_mes,
--     tmo.nr_chamadas_mes
-- FROM tendencia_mensal_operador tmo
-- ORDER BY tmo.nm_operador, tmo.nr_ano ASC, tmo.nr_mes ASC
-- LIMIT 200;

-- =============================================================================
-- Query complementar 2: Ausencias por dia da semana (descomente para usar)
-- =============================================================================
-- SELECT
--     ads.ds_dia_semana,
--     ads.nr_ausencias,
--     ads.nr_registros_dia,
--     ads.pct_ausencia_dia_semana
-- FROM ausencias_dia_semana ads
-- ORDER BY ads.pct_ausencia_dia_semana DESC;
