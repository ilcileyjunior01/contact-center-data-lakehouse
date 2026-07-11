-- =============================================================================
-- KPI: Volume e Desempenho de Chamadas
-- Area: Operacoes de Voz / Contact Center
-- Data: 2026-07-09
-- Objetivo: Monitorar o volume diario de chamadas, taxa de atendimento,
--           Tempo Medio de Atendimento (TMA), Tempo Medio de Espera (TME),
--           e taxa de abandono segmentados por canal e fila.
--
-- Metricas Calculadas:
--   - Volume total de chamadas recebidas por dia
--   - Taxa de atendimento (% chamadas atendidas vs recebidas)
--   - TMA: Tempo Medio de Atendimento em segundos/minutos (somente fl_duracao_valida=1)
--   - TME: Tempo Medio de Espera na fila antes do atendimento
--   - Taxa de abandono: % de chamadas encerradas antes do atendimento
--   - Breakdowns por canal (VOZ, URAS, etc.) e fila
--
-- Como Interpretar:
--   - Taxa de atendimento ideal: acima de 95%
--   - TMA elevado pode indicar processos complexos ou baixa capacitacao
--   - Taxa de abandono acima de 5% indica problema de capacidade ou longa espera
--   - Comparar TME com benchmarks do setor (meta tipica: abaixo de 60 segundos)
-- =============================================================================

WITH
-- CTE 1: Base de chamadas com dimensoes de data, canal e fila
base_chamadas AS (
    SELECT
        fc.sk_chamada,
        fc.sk_fila,
        fc.sk_canal,
        fc.sk_status_chamada,
        fc.nr_duracao_segundos,
        fc.nr_duracao_minutos,
        fc.fl_duracao_valida,
        fc.fl_chamada_completa,
        dd.dt_completa,
        dd.nr_ano,
        dd.nr_mes,
        dd.nr_dia,
        dd.ds_dia_semana,
        dd.fl_fim_de_semana,
        dc.nm_canal AS ds_canal,
        df.nm_fila AS ds_fila,
        dsc.ds_status AS ds_status_chamada,
        -- Classifica se a chamada foi atendida com base no status
        CASE
            WHEN dsc.ds_status IN ('ATENDIDA', 'COMPLETADA', 'TRANSFERIDA') THEN 1
            ELSE 0
        END AS fl_atendida,
        -- Classifica se foi abandono
        CASE
            WHEN dsc.ds_status IN ('ABANDONADA', 'ABANDONADA_FILA') THEN 1
            ELSE 0
        END AS fl_abandonada
    FROM db_gold.fato_chamada fc
    INNER JOIN db_gold.dim_data dd
        ON fc.sk_data_inicio = dd.sk_data
    INNER JOIN db_gold.dim_canal dc
        ON fc.sk_canal = dc.sk_canal
    INNER JOIN db_gold.dim_fila df
        ON fc.sk_fila = df.sk_fila
    LEFT JOIN db_gold.dim_status_chamada dsc
        ON fc.sk_status_chamada = dsc.sk_status
    WHERE dd.nr_ano >= 2025
),

-- CTE 2: Metricas agregadas por data e canal
metricas_por_canal AS (
    SELECT
        dt_completa,
        nr_ano,
        nr_mes,
        nr_dia,
        ds_dia_semana,
        fl_fim_de_semana,
        ds_canal,
        -- Volume total de chamadas
        COUNT(sk_chamada)                                                          AS nr_total_chamadas,
        -- Chamadas atendidas
        SUM(fl_atendida)                                                           AS nr_chamadas_atendidas,
        -- Chamadas abandonadas
        SUM(fl_abandonada)                                                         AS nr_chamadas_abandonadas,
        -- TMA: Tempo Medio de Atendimento apenas para chamadas com duracao valida
        ROUND(
            AVG(CASE WHEN fl_duracao_valida = 1 AND fl_atendida = 1
                     THEN CAST(nr_duracao_segundos AS DOUBLE) END), 2
        )                                                                          AS nr_tma_segundos,
        ROUND(
            AVG(CASE WHEN fl_duracao_valida = 1 AND fl_atendida = 1
                     THEN CAST(nr_duracao_minutos AS DOUBLE) END), 2
        )                                                                          AS nr_tma_minutos,
        -- Duracao maxima e minima para identificar outliers
        MAX(CASE WHEN fl_duracao_valida = 1 THEN nr_duracao_segundos END)         AS nr_duracao_max_seg,
        MIN(CASE WHEN fl_duracao_valida = 1 AND fl_atendida = 1
                 THEN nr_duracao_segundos END)                                    AS nr_duracao_min_seg,
        -- Taxa de atendimento (%)
        ROUND(
            100.0 * SUM(fl_atendida) / NULLIF(COUNT(sk_chamada), 0), 2
        )                                                                          AS pct_taxa_atendimento,
        -- Taxa de abandono (%)
        ROUND(
            100.0 * SUM(fl_abandonada) / NULLIF(COUNT(sk_chamada), 0), 2
        )                                                                          AS pct_taxa_abandono,
        -- Chamadas completas (sem interrupcoes tecnicas)
        SUM(fl_chamada_completa)                                                   AS nr_chamadas_completas,
        ROUND(
            100.0 * SUM(fl_chamada_completa) / NULLIF(COUNT(sk_chamada), 0), 2
        )                                                                          AS pct_chamadas_completas
    FROM base_chamadas
    GROUP BY
        dt_completa, nr_ano, nr_mes, nr_dia,
        ds_dia_semana, fl_fim_de_semana, ds_canal
),

-- CTE 3: Metricas agregadas por data e fila
metricas_por_fila AS (
    SELECT
        dt_completa,
        ds_fila,
        ds_canal,
        COUNT(sk_chamada)                                                          AS nr_total_chamadas_fila,
        SUM(fl_atendida)                                                           AS nr_atendidas_fila,
        SUM(fl_abandonada)                                                         AS nr_abandonadas_fila,
        ROUND(
            100.0 * SUM(fl_atendida) / NULLIF(COUNT(sk_chamada), 0), 2
        )                                                                          AS pct_atendimento_fila,
        ROUND(
            100.0 * SUM(fl_abandonada) / NULLIF(COUNT(sk_chamada), 0), 2
        )                                                                          AS pct_abandono_fila,
        ROUND(
            AVG(CASE WHEN fl_duracao_valida = 1 AND fl_atendida = 1
                     THEN CAST(nr_duracao_segundos AS DOUBLE) END), 2
        )                                                                          AS nr_tma_fila_segundos
    FROM base_chamadas
    GROUP BY dt_completa, ds_fila, ds_canal
),

-- CTE 4: Ranking de filas mais sobrecarregadas no ultimo periodo
ranking_filas AS (
    SELECT
        ds_fila,
        ds_canal,
        SUM(nr_total_chamadas_fila)                                               AS nr_total_geral,
        ROUND(AVG(pct_abandono_fila), 2)                                          AS pct_abandono_medio,
        ROUND(AVG(nr_tma_fila_segundos), 2)                                       AS nr_tma_medio_segundos,
        RANK() OVER (ORDER BY SUM(nr_total_chamadas_fila) DESC)                   AS rank_volume
    FROM metricas_por_fila
    GROUP BY ds_fila, ds_canal
)

-- Resultado final: Volume e desempenho diario por canal
SELECT
    mpc.dt_completa,
    mpc.nr_ano,
    mpc.nr_mes,
    mpc.nr_dia,
    mpc.ds_dia_semana,
    mpc.fl_fim_de_semana,
    mpc.ds_canal,
    mpc.nr_total_chamadas,
    mpc.nr_chamadas_atendidas,
    mpc.nr_chamadas_abandonadas,
    mpc.pct_taxa_atendimento,
    mpc.pct_taxa_abandono,
    mpc.nr_tma_segundos,
    mpc.nr_tma_minutos,
    mpc.nr_duracao_max_seg,
    mpc.nr_duracao_min_seg,
    mpc.nr_chamadas_completas,
    mpc.pct_chamadas_completas,
    -- Indicadores de alerta baseados em thresholds operacionais
    CASE
        WHEN mpc.pct_taxa_atendimento >= 95 THEN 'VERDE'
        WHEN mpc.pct_taxa_atendimento >= 85 THEN 'AMARELO'
        ELSE 'VERMELHO'
    END AS semaforo_atendimento,
    CASE
        WHEN mpc.pct_taxa_abandono <= 5  THEN 'VERDE'
        WHEN mpc.pct_taxa_abandono <= 10 THEN 'AMARELO'
        ELSE 'VERMELHO'
    END AS semaforo_abandono,
    CASE
        WHEN mpc.nr_tma_segundos <= 180  THEN 'VERDE'
        WHEN mpc.nr_tma_segundos <= 300  THEN 'AMARELO'
        ELSE 'VERMELHO'
    END AS semaforo_tma
FROM metricas_por_canal mpc
ORDER BY
    mpc.dt_completa DESC,
    mpc.nr_total_chamadas DESC
LIMIT 100;

-- =============================================================================
-- Query complementar: Detalhamento por fila (descomente para usar)
-- =============================================================================
-- SELECT
--     rf.ds_fila,
--     rf.ds_canal,
--     rf.nr_total_geral,
--     rf.pct_abandono_medio,
--     rf.nr_tma_medio_segundos,
--     rf.rank_volume,
--     CASE
--         WHEN rf.pct_abandono_medio <= 5  THEN 'VERDE'
--         WHEN rf.pct_abandono_medio <= 10 THEN 'AMARELO'
--         ELSE 'VERMELHO'
--     END AS semaforo_fila
-- FROM ranking_filas rf
-- ORDER BY rf.rank_volume
-- LIMIT 50;
