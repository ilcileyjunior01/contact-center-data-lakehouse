-- =============================================================================
-- KPI: Ocupacao e Sobrecarga de Filas
-- Area: Gestao de Filas / Workforce Planning
-- Data: 2026-07-09
-- Objetivo: Monitorar a taxa de ocupacao de cada fila de atendimento,
--           analisar a distribuicao de chamadas por hora do dia (perfil
--           intraday) e identificar as filas mais sobrecarregadas para
--           subsidiar o dimensionamento de equipes.
--
-- Metricas Calculadas:
--   - Taxa de ocupacao por fila: (chamadas recebidas / capacidade maxima)
--   - Volume de chamadas por hora do dia (distribuicao intraday)
--   - Filas com maior concentracao de volume (ranking)
--   - Pico horario por fila
--   - Comparativo entre dias da semana (padroes de volume)
--   - Indice de congestionamento por hora
--
-- Como Interpretar:
--   - Taxa de ocupacao acima de 85% indica sobrecarga critica
--   - Identificar horarios de pico permite escalonar mais agentes no periodo
--   - Filas com pico muito concentrado (ex: 1 hora com 40% do volume) exigem
--     planejamento especifico de turnos
--   - Comparar dias da semana ajuda a identificar sazonalidade semanal
-- =============================================================================

WITH
-- CTE 1: Base de chamadas com data/hora de inicio e dados de fila
base_chamadas_fila AS (
    SELECT
        fc.sk_chamada,
        fc.sk_fila,
        fc.sk_canal,
        fc.sk_data_inicio,
        fc.nr_duracao_segundos,
        fc.fl_chamada_completa,
        fc.fl_duracao_valida,
        df.nm_fila AS ds_fila,
        CAST(NULL AS INT) AS nr_capacidade_maxima,  -- Capacidade configurada da fila (ex: max agentes)
        dc.nm_canal AS ds_canal,
        dd.dt_completa,
        dd.nr_ano,
        dd.nr_mes,
        dd.nr_dia,
        dd.ds_dia_semana,
        dd.fl_fim_de_semana,
        DATE_FORMAT(dd.dt_completa, '%Y-%m')  AS ds_ano_mes,
        -- Hora do dia (extraida da chamada via campo de horario ou aproximada via data)
        -- Athena: caso haja coluna de hora, use HOUR(dt_hora_inicio)
        -- nr_hora_inicio nao existe em fato_chamada, substituido por 0
        CAST(NULL AS INT)                     AS nr_hora_inicio,
        -- Classificacao do turno (nr_hora_inicio nao existe em fato_chamada; sempre MADRUGADA como placeholder)
        CASE
            WHEN COALESCE(CAST(NULL AS INT), 0) BETWEEN 6  AND 11 THEN 'MANHA'
            WHEN COALESCE(CAST(NULL AS INT), 0) BETWEEN 12 AND 17 THEN 'TARDE'
            WHEN COALESCE(CAST(NULL AS INT), 0) BETWEEN 18 AND 21 THEN 'NOITE'
            ELSE 'MADRUGADA'
        END AS ds_turno,
        -- Status da chamada
        dsc.ds_status AS ds_status_chamada,
        CASE
            WHEN dsc.ds_status IN ('ATENDIDA', 'COMPLETADA', 'TRANSFERIDA') THEN 1
            ELSE 0
        END AS fl_atendida,
        CASE
            WHEN dsc.ds_status IN ('ABANDONADA', 'ABANDONADA_FILA') THEN 1
            ELSE 0
        END AS fl_abandonada
    FROM db_gold.fato_chamada fc
    INNER JOIN db_gold.dim_fila df
        ON fc.sk_fila = df.sk_fila
    INNER JOIN db_gold.dim_canal dc
        ON fc.sk_canal = dc.sk_canal
    INNER JOIN db_gold.dim_data dd
        ON fc.sk_data_inicio = dd.sk_data
    LEFT JOIN db_gold.dim_status_chamada dsc
        ON fc.sk_status_chamada = dsc.sk_status
),

-- CTE 2: Ocupacao diaria por fila
ocupacao_diaria_fila AS (
    SELECT
        dt_completa,
        nr_ano,
        nr_mes,
        nr_dia,
        ds_dia_semana,
        fl_fim_de_semana,
        sk_fila,
        ds_fila,
        ds_canal,
        MAX(nr_capacidade_maxima)                                                 AS nr_capacidade_maxima,
        COUNT(sk_chamada)                                                         AS nr_chamadas_recebidas,
        SUM(fl_atendida)                                                          AS nr_chamadas_atendidas,
        SUM(fl_abandonada)                                                        AS nr_chamadas_abandonadas,
        -- Taxa de ocupacao: volume vs capacidade maxima diaria
        -- Capacidade maxima = nr_capacidade_maxima agentes * nr_horas_operacao (ex: 8h)
        ROUND(
            100.0 * COUNT(sk_chamada)
            / NULLIF(MAX(nr_capacidade_maxima) * 8, 0), 2  -- 8h = horas de operacao estimadas
        )                                                                         AS pct_ocupacao_fila,
        ROUND(
            100.0 * SUM(fl_atendida)
            / NULLIF(COUNT(sk_chamada), 0), 2
        )                                                                         AS pct_taxa_atendimento,
        ROUND(
            AVG(CASE WHEN fl_duracao_valida = 1 AND fl_atendida = 1
                     THEN CAST(nr_duracao_segundos AS DOUBLE) END), 2
        )                                                                         AS nr_tma_segundos
    FROM base_chamadas_fila
    GROUP BY
        dt_completa, nr_ano, nr_mes, nr_dia,
        ds_dia_semana, fl_fim_de_semana,
        sk_fila, ds_fila, ds_canal
),

-- CTE 3: Distribuicao intraday (chamadas por hora do dia)
distribuicao_intraday AS (
    SELECT
        ds_fila,
        ds_canal,
        nr_hora_inicio,
        ds_turno,
        COUNT(sk_chamada)                                                         AS nr_chamadas_hora,
        SUM(fl_atendida)                                                          AS nr_atendidas_hora,
        SUM(fl_abandonada)                                                        AS nr_abandonadas_hora,
        ROUND(AVG(CAST(nr_duracao_segundos AS DOUBLE)), 2)                       AS nr_duracao_media_hora,
        -- Volume acumulado para identificar pico
        ROUND(
            100.0 * COUNT(sk_chamada)
            / NULLIF(SUM(COUNT(sk_chamada)) OVER (PARTITION BY ds_fila), 0), 2
        )                                                                         AS pct_volume_hora_vs_total,
        -- Rank do horario de maior volume por fila
        RANK() OVER (
            PARTITION BY ds_fila
            ORDER BY COUNT(sk_chamada) DESC
        )                                                                         AS rank_horario_volume
    FROM base_chamadas_fila
    GROUP BY ds_fila, ds_canal, nr_hora_inicio, ds_turno
),

-- CTE 4: Ranking de filas mais sobrecarregadas (media de ocupacao)
ranking_filas_sobrecarga AS (
    SELECT
        sk_fila,
        ds_fila,
        ds_canal,
        ROUND(AVG(nr_chamadas_recebidas), 2)                                     AS nr_media_chamadas_dia,
        ROUND(AVG(pct_ocupacao_fila), 2)                                         AS nr_ocupacao_media,
        MAX(pct_ocupacao_fila)                                                   AS nr_ocupacao_maxima,
        MIN(pct_ocupacao_fila)                                                   AS nr_ocupacao_minima,
        ROUND(AVG(pct_taxa_atendimento), 2)                                      AS nr_atendimento_medio,
        ROUND(AVG(nr_tma_segundos), 2)                                           AS nr_tma_medio,
        COUNT(CASE WHEN pct_ocupacao_fila > 85 THEN 1 END)                       AS nr_dias_sobrecarga_critica,
        RANK() OVER (ORDER BY AVG(pct_ocupacao_fila) DESC)                       AS rank_sobrecarga
    FROM ocupacao_diaria_fila
    GROUP BY sk_fila, ds_fila, ds_canal
),

-- CTE 5: Padroes por dia da semana (sazonalidade semanal)
padrao_semanal AS (
    SELECT
        ds_dia_semana,
        ds_fila,
        ROUND(AVG(nr_chamadas_recebidas), 2)                                     AS nr_media_chamadas,
        ROUND(AVG(pct_ocupacao_fila), 2)                                         AS nr_ocupacao_media_semana,
        ROUND(AVG(pct_taxa_atendimento), 2)                                      AS nr_atendimento_medio_semana
    FROM ocupacao_diaria_fila
    GROUP BY ds_dia_semana, ds_fila
),

-- CTE 6: Pico horario por fila (hora com mais chamadas)
pico_horario_fila AS (
    SELECT
        ds_fila,
        nr_hora_inicio,
        ds_turno,
        nr_chamadas_hora,
        pct_volume_hora_vs_total,
        rank_horario_volume
    FROM distribuicao_intraday
    WHERE rank_horario_volume = 1
)

-- Resultado principal: Ocupacao diaria por fila com indicadores de sobrecarga
SELECT
    odf.dt_completa,
    odf.nr_ano,
    odf.nr_mes,
    odf.nr_dia,
    odf.ds_dia_semana,
    odf.fl_fim_de_semana,
    odf.ds_fila,
    odf.ds_canal,
    odf.nr_capacidade_maxima,
    odf.nr_chamadas_recebidas,
    odf.nr_chamadas_atendidas,
    odf.nr_chamadas_abandonadas,
    odf.pct_ocupacao_fila,
    odf.pct_taxa_atendimento,
    odf.nr_tma_segundos,
    -- Ranking de sobrecarga (referencia do periodo)
    rfs.rank_sobrecarga,
    rfs.nr_ocupacao_media,
    rfs.nr_dias_sobrecarga_critica,
    -- Pico horario da fila
    phf.nr_hora_inicio                                                            AS nr_hora_pico,
    phf.ds_turno                                                                  AS ds_turno_pico,
    phf.nr_chamadas_hora                                                          AS nr_chamadas_hora_pico,
    phf.pct_volume_hora_vs_total                                                  AS pct_volume_hora_pico,
    -- Semaforos de ocupacao
    CASE
        WHEN odf.pct_ocupacao_fila > 100 THEN 'SATURADA'
        WHEN odf.pct_ocupacao_fila > 85  THEN 'SOBRECARGA_CRITICA'
        WHEN odf.pct_ocupacao_fila > 70  THEN 'ATENCAO'
        ELSE 'NORMAL'
    END AS ds_situacao_ocupacao,
    -- Semaforo de atendimento
    CASE
        WHEN odf.pct_taxa_atendimento >= 95 THEN 'VERDE'
        WHEN odf.pct_taxa_atendimento >= 85 THEN 'AMARELO'
        ELSE 'VERMELHO'
    END AS semaforo_atendimento
FROM ocupacao_diaria_fila odf
LEFT JOIN ranking_filas_sobrecarga rfs
    ON odf.sk_fila = rfs.sk_fila
LEFT JOIN pico_horario_fila phf
    ON odf.ds_fila = phf.ds_fila
ORDER BY
    odf.dt_completa DESC,
    odf.pct_ocupacao_fila DESC
LIMIT 100;

-- =============================================================================
-- Query complementar 1: Distribuicao intraday por fila (descomente para usar)
-- =============================================================================
-- SELECT
--     di.ds_fila,
--     di.nr_hora_inicio,
--     di.ds_turno,
--     di.nr_chamadas_hora,
--     di.nr_atendidas_hora,
--     di.nr_abandonadas_hora,
--     di.nr_duracao_media_hora,
--     di.pct_volume_hora_vs_total,
--     di.rank_horario_volume
-- FROM distribuicao_intraday di
-- WHERE di.rank_horario_volume <= 5  -- Top 5 horarios por fila
-- ORDER BY di.ds_fila, di.nr_hora_inicio
-- LIMIT 200;

-- =============================================================================
-- Query complementar 2: Ranking de filas sobrecarregadas (descomente para usar)
-- =============================================================================
-- SELECT
--     rfs.ds_fila,
--     rfs.ds_canal,
--     rfs.nr_media_chamadas_dia,
--     rfs.nr_ocupacao_media,
--     rfs.nr_ocupacao_maxima,
--     rfs.nr_ocupacao_minima,
--     rfs.nr_atendimento_medio,
--     rfs.nr_tma_medio,
--     rfs.nr_dias_sobrecarga_critica,
--     rfs.rank_sobrecarga
-- FROM ranking_filas_sobrecarga rfs
-- ORDER BY rfs.rank_sobrecarga
-- LIMIT 20;
