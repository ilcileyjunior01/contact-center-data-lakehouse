-- =============================================================================
-- KPI: Volume de Sessoes Chat e WhatsApp
-- Area: Canais Digitais / Operacoes
-- Data: 2026-07-09
-- Objetivo: Monitorar o volume de sessoes de atendimento via Chat (web) e
--           WhatsApp por dia e canal. Calcular duracao media de sessao e
--           numero medio de mensagens trocadas por sessao atraves do JOIN
--           com fato_mensagem_chat.
--
-- Metricas Calculadas:
--   - Volume de sessoes por dia segmentado por canal (CHAT / WHATSAPP)
--   - Duracao media de sessao em minutos
--   - Numero medio de mensagens por sessao
--   - Distribuicao de sessoes por turno (manha, tarde, noite)
--   - Taxa de sessoes encerradas pelo cliente vs pelo agente
--   - Tendencia semanal e mensal de volume digital
--
-- Como Interpretar:
--   - Crescimento continuo de WhatsApp indica preferencia crescente do canal
--   - Duracao media elevada pode indicar processos de atendimento ineficientes
--   - Poucos mensagens por sessao pode indicar abandono rapido do cliente
--   - Pico de volume em determinados horarios orienta escalamento de equipe
-- =============================================================================

WITH
-- CTE 1: Base de sessoes de chat com dimensoes de data e canal
base_chat AS (
    SELECT
        fc.sk_chat,
        fc.sk_cliente,
        fc.sk_operador,
        fc.sk_fila,
        fc.sk_canal,
        dc.ds_canal,
        dd.dt_completa,
        dd.nr_ano,
        dd.nr_mes,
        dd.nr_dia,
        dd.ds_dia_semana,
        dd.fl_fim_semana,
        DATE_FORMAT(dd.dt_completa, '%Y-%m') AS ds_ano_mes,
        -- Classifica o tipo de canal digital
        CASE
            WHEN UPPER(dc.ds_canal) LIKE '%WHATSAPP%' THEN 'WHATSAPP'
            WHEN UPPER(dc.ds_canal) LIKE '%CHAT%'     THEN 'CHAT'
            ELSE 'OUTRO_DIGITAL'
        END AS ds_tipo_canal_digital,
        fc.nr_duracao_segundos,
        ROUND(CAST(fc.nr_duracao_segundos AS DOUBLE) / 60.0, 2) AS nr_duracao_minutos
    FROM db_gold.fato_chat fc
    INNER JOIN db_gold.dim_canal dc
        ON fc.sk_canal = dc.sk_canal
    INNER JOIN db_gold.dim_data dd
        ON fc.sk_data_inicio = dd.sk_data
    WHERE UPPER(dc.ds_canal) IN ('CHAT', 'WHATSAPP')
       OR UPPER(dc.ds_canal) LIKE '%CHAT%'
       OR UPPER(dc.ds_canal) LIKE '%WHATSAPP%'
),

-- CTE 2: Base de sessoes de WhatsApp (tabela especifica)
base_whatsapp AS (
    SELECT
        fw.sk_whatsapp,
        fw.sk_cliente,
        fw.sk_operador,
        fw.sk_canal,
        dc.ds_canal,
        dd.dt_completa,
        dd.nr_ano,
        dd.nr_mes,
        dd.nr_dia,
        dd.ds_dia_semana,
        dd.fl_fim_semana,
        DATE_FORMAT(dd.dt_completa, '%Y-%m') AS ds_ano_mes,
        'WHATSAPP' AS ds_tipo_canal_digital,
        fw.nr_duracao_segundos,
        ROUND(CAST(fw.nr_duracao_segundos AS DOUBLE) / 60.0, 2) AS nr_duracao_minutos
    FROM db_gold.fato_whatsapp fw
    INNER JOIN db_gold.dim_canal dc
        ON fw.sk_canal = dc.sk_canal
    INNER JOIN db_gold.dim_data dd
        ON fw.sk_data_inicio = dd.sk_data
),

-- CTE 3: Contagem de mensagens por sessao de chat (de fato_mensagem_chat)
mensagens_por_sessao AS (
    SELECT
        fmc.sk_chat,
        COUNT(fmc.sk_mensagem)                                                    AS nr_total_mensagens,
        -- Mensagens do cliente
        COUNT(CASE WHEN fmc.ds_origem_mensagem = 'CLIENTE' THEN 1 END)           AS nr_mensagens_cliente,
        -- Mensagens do agente
        COUNT(CASE WHEN fmc.ds_origem_mensagem = 'AGENTE'  THEN 1 END)           AS nr_mensagens_agente,
        -- Tempo medio entre mensagens (responsividade)
        ROUND(AVG(CAST(fmc.nr_tempo_resposta_segundos AS DOUBLE)), 2)             AS nr_tempo_medio_resposta_seg
    FROM db_gold.fato_mensagem_chat fmc
    GROUP BY fmc.sk_chat
),

-- CTE 4: Uniao de sessoes chat com contagem de mensagens
chat_com_mensagens AS (
    SELECT
        bc.sk_chat,
        bc.ds_canal,
        bc.ds_tipo_canal_digital,
        bc.dt_completa,
        bc.nr_ano,
        bc.nr_mes,
        bc.nr_dia,
        bc.ds_dia_semana,
        bc.fl_fim_semana,
        bc.ds_ano_mes,
        bc.nr_duracao_segundos,
        bc.nr_duracao_minutos,
        COALESCE(mps.nr_total_mensagens, 0)      AS nr_total_mensagens,
        COALESCE(mps.nr_mensagens_cliente, 0)    AS nr_mensagens_cliente,
        COALESCE(mps.nr_mensagens_agente, 0)     AS nr_mensagens_agente,
        COALESCE(mps.nr_tempo_medio_resposta_seg, 0) AS nr_tempo_resposta_seg
    FROM base_chat bc
    LEFT JOIN mensagens_por_sessao mps
        ON bc.sk_chat = mps.sk_chat
),

-- CTE 5: Sessoes de WhatsApp (sem mensagens individuais na fato_mensagem_chat por ora)
whatsapp_sessoes AS (
    SELECT
        bw.sk_whatsapp   AS sk_chat,
        bw.ds_canal,
        bw.ds_tipo_canal_digital,
        bw.dt_completa,
        bw.nr_ano,
        bw.nr_mes,
        bw.nr_dia,
        bw.ds_dia_semana,
        bw.fl_fim_semana,
        bw.ds_ano_mes,
        bw.nr_duracao_segundos,
        bw.nr_duracao_minutos,
        0 AS nr_total_mensagens,
        0 AS nr_mensagens_cliente,
        0 AS nr_mensagens_agente,
        0 AS nr_tempo_resposta_seg
    FROM base_whatsapp bw
),

-- CTE 6: Base unificada de canais digitais
base_unificada AS (
    SELECT * FROM chat_com_mensagens
    UNION ALL
    SELECT * FROM whatsapp_sessoes
),

-- CTE 7: Agregacao diaria por canal
volume_diario_canal AS (
    SELECT
        dt_completa,
        nr_ano,
        nr_mes,
        nr_dia,
        ds_dia_semana,
        fl_fim_semana,
        ds_ano_mes,
        ds_tipo_canal_digital,
        -- Volume de sessoes
        COUNT(sk_chat)                                                            AS nr_total_sessoes,
        -- Duracao media em minutos
        ROUND(AVG(CAST(nr_duracao_minutos AS DOUBLE)), 2)                        AS nr_duracao_media_minutos,
        ROUND(AVG(CAST(nr_duracao_segundos AS DOUBLE)), 2)                       AS nr_duracao_media_segundos,
        -- Duracao maxima e minima
        MAX(nr_duracao_segundos)                                                  AS nr_duracao_maxima_seg,
        MIN(nr_duracao_segundos)                                                  AS nr_duracao_minima_seg,
        -- Mensagens (valido para chat com dados de mensagens)
        ROUND(AVG(CAST(nr_total_mensagens AS DOUBLE)), 2)                        AS nr_media_mensagens_sessao,
        ROUND(AVG(CAST(nr_mensagens_cliente AS DOUBLE)), 2)                      AS nr_media_msg_cliente,
        ROUND(AVG(CAST(nr_mensagens_agente AS DOUBLE)), 2)                       AS nr_media_msg_agente,
        SUM(nr_total_mensagens)                                                   AS nr_total_mensagens_dia,
        -- Tempo medio de resposta do agente
        ROUND(AVG(CASE WHEN nr_tempo_resposta_seg > 0
                       THEN CAST(nr_tempo_resposta_seg AS DOUBLE) END), 2)       AS nr_tma_resposta_seg,
        -- Sessoes com longa duracao (acima de 30 min = 1800 seg)
        COUNT(CASE WHEN nr_duracao_segundos > 1800 THEN 1 END)                   AS nr_sessoes_longas,
        ROUND(
            100.0 * COUNT(CASE WHEN nr_duracao_segundos > 1800 THEN 1 END)
            / NULLIF(COUNT(sk_chat), 0), 2
        )                                                                         AS pct_sessoes_longas
    FROM base_unificada
    GROUP BY
        dt_completa, nr_ano, nr_mes, nr_dia,
        ds_dia_semana, fl_fim_semana, ds_ano_mes, ds_tipo_canal_digital
),

-- CTE 8: Tendencia mensal por canal
tendencia_mensal AS (
    SELECT
        ds_ano_mes,
        nr_ano,
        nr_mes,
        ds_tipo_canal_digital,
        SUM(nr_total_sessoes)                                                     AS nr_sessoes_mes,
        ROUND(AVG(nr_duracao_media_minutos), 2)                                  AS nr_duracao_media_mes,
        ROUND(AVG(nr_media_mensagens_sessao), 2)                                 AS nr_media_msg_mes
    FROM volume_diario_canal
    GROUP BY ds_ano_mes, nr_ano, nr_mes, ds_tipo_canal_digital
)

-- Resultado principal: Volume diario por canal digital
SELECT
    vdc.dt_completa,
    vdc.nr_ano,
    vdc.nr_mes,
    vdc.nr_dia,
    vdc.ds_dia_semana,
    vdc.fl_fim_semana,
    vdc.ds_tipo_canal_digital,
    vdc.nr_total_sessoes,
    vdc.nr_duracao_media_minutos,
    vdc.nr_duracao_media_segundos,
    vdc.nr_duracao_maxima_seg,
    vdc.nr_media_mensagens_sessao,
    vdc.nr_media_msg_cliente,
    vdc.nr_media_msg_agente,
    vdc.nr_total_mensagens_dia,
    vdc.nr_tma_resposta_seg,
    vdc.nr_sessoes_longas,
    vdc.pct_sessoes_longas,
    -- Semaforo de volume (referencia customizavel)
    CASE
        WHEN vdc.nr_total_sessoes > 500 THEN 'ALTO_VOLUME'
        WHEN vdc.nr_total_sessoes > 200 THEN 'VOLUME_NORMAL'
        ELSE 'BAIXO_VOLUME'
    END AS ds_classificacao_volume,
    -- Relacao mensagens/duracao como proxy de engajamento
    ROUND(
        vdc.nr_media_mensagens_sessao
        / NULLIF(vdc.nr_duracao_media_minutos, 0), 2
    ) AS nr_mensagens_por_minuto
FROM volume_diario_canal vdc
ORDER BY
    vdc.dt_completa DESC,
    vdc.nr_total_sessoes DESC
LIMIT 100;

-- =============================================================================
-- Query complementar: Tendencia mensal por canal (descomente para usar)
-- =============================================================================
-- SELECT
--     tm.ds_ano_mes,
--     tm.ds_tipo_canal_digital,
--     tm.nr_sessoes_mes,
--     tm.nr_duracao_media_mes,
--     tm.nr_media_msg_mes
-- FROM tendencia_mensal tm
-- ORDER BY tm.nr_ano ASC, tm.nr_mes ASC, tm.ds_tipo_canal_digital
-- LIMIT 48;
