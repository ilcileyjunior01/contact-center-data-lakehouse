-- =============================================================================
-- KPI: Efetividade da URA (Unidade de Resposta Audivel)
-- Area: Autoatendimento / Canais de Voz
-- Data: 2026-07-09
-- Objetivo: Avaliar a efetividade da URA (IVR) medindo a taxa de auto-
--           atendimento (clientes resolvidos sem transferencia para humano),
--           os fluxos mais acessados (top 10 opcoes), a taxa de abandono
--           dentro da URA e o tempo medio de navegacao na arvore de menus.
--
-- Metricas Calculadas:
--   - Taxa de auto-atendimento: % de sessoes onde fl_transferiu_humano=0
--   - Top 10 opcoes/fluxos mais selecionados pelos clientes
--   - Taxa de abandono na URA (saiu sem completar nem transferir)
--   - Tempo medio de navegacao na URA em segundos
--   - Nivel medio de profundidade de navegacao no menu
--   - Pontos de abandono mais frequentes (nodes criticos)
--
-- Como Interpretar:
--   - Taxa de auto-atendimento acima de 40% e considerada boa para contact centers
--   - Alta taxa de abandono em opcoes especificas indica problemas de UX do menu
--   - Tempo medio de navegacao acima de 120 segundos pode frustrar o cliente
--   - Fluxos com poucos acessos podem ser simplificados ou removidos
--   - Comparar auto-atendimento por periodo para medir impacto de mudancas na URA
-- =============================================================================

WITH
-- CTE 1: Base de navegacoes na URA com todas as dimensoes
base_ura AS (
    SELECT
        fun.sk_ura_navegacao,
        fun.sk_chamada,
        fun.sk_cliente,
        fun.sk_canal,
        fun.nr_opcao_selecionada,
        fun.ds_opcao_selecionada,
        fun.ds_fluxo_ura,
        fun.nr_nivel_menu,
        fun.nr_tempo_navegacao_segundos,
        fun.fl_transferiu_humano,
        fun.fl_autoatendimento_concluido,
        fun.fl_abandono_ura,
        -- Classifica o resultado da sessao URA
        CASE
            WHEN fun.fl_autoatendimento_concluido = 1 THEN 'AUTO_ATENDIDO'
            WHEN fun.fl_transferiu_humano = 1         THEN 'TRANSFERIDO_HUMANO'
            WHEN fun.fl_abandono_ura = 1              THEN 'ABANDONO_URA'
            ELSE 'OUTRO'
        END AS ds_resultado_ura,
        dc.ds_canal,
        dd.dt_completa,
        dd.nr_ano,
        dd.nr_mes,
        dd.nr_dia,
        dd.ds_dia_semana,
        DATE_FORMAT(dd.dt_completa, '%Y-%m') AS ds_ano_mes
    FROM db_gold.fato_ura_navegacao fun
    INNER JOIN db_gold.dim_canal dc
        ON fun.sk_canal = dc.sk_canal
    INNER JOIN db_gold.dim_data dd
        ON fun.sk_data_inicio = dd.sk_data
),

-- CTE 2: Taxa de auto-atendimento geral e por periodo
taxa_autoatendimento AS (
    SELECT
        dt_completa,
        nr_ano,
        nr_mes,
        ds_ano_mes,
        -- Volume total de sessoes na URA
        COUNT(sk_ura_navegacao)                                                   AS nr_total_sessoes_ura,
        -- Auto-atendimentos concluidos (sem transferencia para humano)
        COUNT(CASE WHEN fl_autoatendimento_concluido = 1 THEN 1 END)             AS nr_auto_atendidos,
        -- Transferencias para humano
        COUNT(CASE WHEN fl_transferiu_humano = 1 THEN 1 END)                     AS nr_transferidos_humano,
        -- Abandonos na URA
        COUNT(CASE WHEN fl_abandono_ura = 1 THEN 1 END)                          AS nr_abandonos_ura,
        -- Taxa de auto-atendimento (%)
        ROUND(
            100.0 * COUNT(CASE WHEN fl_autoatendimento_concluido = 1 THEN 1 END)
            / NULLIF(COUNT(sk_ura_navegacao), 0), 2
        )                                                                         AS pct_taxa_autoatendimento,
        -- Taxa de transferencia para humano (%)
        ROUND(
            100.0 * COUNT(CASE WHEN fl_transferiu_humano = 1 THEN 1 END)
            / NULLIF(COUNT(sk_ura_navegacao), 0), 2
        )                                                                         AS pct_transferencia_humano,
        -- Taxa de abandono na URA (%)
        ROUND(
            100.0 * COUNT(CASE WHEN fl_abandono_ura = 1 THEN 1 END)
            / NULLIF(COUNT(sk_ura_navegacao), 0), 2
        )                                                                         AS pct_abandono_ura,
        -- Tempo medio de navegacao em segundos
        ROUND(
            AVG(CAST(nr_tempo_navegacao_segundos AS DOUBLE)), 2
        )                                                                         AS nr_tempo_medio_navegacao_seg,
        -- Tempo medio em minutos
        ROUND(
            AVG(CAST(nr_tempo_navegacao_segundos AS DOUBLE)) / 60.0, 2
        )                                                                         AS nr_tempo_medio_navegacao_min,
        -- Nivel medio de profundidade de navegacao
        ROUND(AVG(CAST(nr_nivel_menu AS DOUBLE)), 2)                             AS nr_nivel_medio_menu,
        -- Tempo maximo de navegacao
        MAX(nr_tempo_navegacao_segundos)                                          AS nr_tempo_max_navegacao_seg
    FROM base_ura
    GROUP BY dt_completa, nr_ano, nr_mes, ds_ano_mes
),

-- CTE 3: Top 10 fluxos/opcoes mais selecionados
top_fluxos_ura AS (
    SELECT
        ds_fluxo_ura,
        nr_opcao_selecionada,
        ds_opcao_selecionada,
        COUNT(sk_ura_navegacao)                                                   AS nr_acessos,
        -- Percentual sobre total de navegacoes
        ROUND(
            100.0 * COUNT(sk_ura_navegacao)
            / NULLIF((SELECT COUNT(*) FROM base_ura), 0), 2
        )                                                                         AS pct_acessos_total,
        -- Taxa de auto-atendimento neste fluxo especifico
        ROUND(
            100.0 * COUNT(CASE WHEN fl_autoatendimento_concluido = 1 THEN 1 END)
            / NULLIF(COUNT(sk_ura_navegacao), 0), 2
        )                                                                         AS pct_autoatendimento_fluxo,
        -- Taxa de abandono neste fluxo (ponto critico de abandono)
        ROUND(
            100.0 * COUNT(CASE WHEN fl_abandono_ura = 1 THEN 1 END)
            / NULLIF(COUNT(sk_ura_navegacao), 0), 2
        )                                                                         AS pct_abandono_fluxo,
        -- Taxa de transferencia para humano neste fluxo
        ROUND(
            100.0 * COUNT(CASE WHEN fl_transferiu_humano = 1 THEN 1 END)
            / NULLIF(COUNT(sk_ura_navegacao), 0), 2
        )                                                                         AS pct_transferencia_fluxo,
        -- Tempo medio de permanencia neste fluxo
        ROUND(
            AVG(CAST(nr_tempo_navegacao_segundos AS DOUBLE)), 2
        )                                                                         AS nr_tempo_medio_seg,
        -- Ranking por volume de acesso
        RANK() OVER (ORDER BY COUNT(sk_ura_navegacao) DESC)                      AS rank_acesso
    FROM base_ura
    GROUP BY ds_fluxo_ura, nr_opcao_selecionada, ds_opcao_selecionada
),

-- CTE 4: Pontos criticos de abandono (fluxos com maior taxa de abandono)
pontos_abandono AS (
    SELECT
        ds_fluxo_ura,
        ds_opcao_selecionada,
        nr_nivel_menu,
        COUNT(sk_ura_navegacao)                                                   AS nr_total_chegadas,
        COUNT(CASE WHEN fl_abandono_ura = 1 THEN 1 END)                          AS nr_abandonos,
        ROUND(
            100.0 * COUNT(CASE WHEN fl_abandono_ura = 1 THEN 1 END)
            / NULLIF(COUNT(sk_ura_navegacao), 0), 2
        )                                                                         AS pct_abandono_ponto,
        RANK() OVER (ORDER BY
            100.0 * COUNT(CASE WHEN fl_abandono_ura = 1 THEN 1 END)
            / NULLIF(COUNT(sk_ura_navegacao), 0) DESC
        )                                                                         AS rank_abandono
    FROM base_ura
    GROUP BY ds_fluxo_ura, ds_opcao_selecionada, nr_nivel_menu
    HAVING COUNT(sk_ura_navegacao) >= 10  -- Minimo de acessos para relevancia
),

-- CTE 5: Tendencia mensal de auto-atendimento
tendencia_mensal_ura AS (
    SELECT
        ds_ano_mes,
        nr_ano,
        nr_mes,
        COUNT(sk_ura_navegacao)                                                   AS nr_sessoes_mes,
        ROUND(
            100.0 * COUNT(CASE WHEN fl_autoatendimento_concluido = 1 THEN 1 END)
            / NULLIF(COUNT(sk_ura_navegacao), 0), 2
        )                                                                         AS pct_autoatendimento_mes,
        ROUND(
            100.0 * COUNT(CASE WHEN fl_abandono_ura = 1 THEN 1 END)
            / NULLIF(COUNT(sk_ura_navegacao), 0), 2
        )                                                                         AS pct_abandono_mes,
        ROUND(
            AVG(CAST(nr_tempo_navegacao_segundos AS DOUBLE)), 2
        )                                                                         AS nr_tempo_medio_mes_seg,
        -- Variacao mensal de auto-atendimento
        LAG(ROUND(
            100.0 * COUNT(CASE WHEN fl_autoatendimento_concluido = 1 THEN 1 END)
            / NULLIF(COUNT(sk_ura_navegacao), 0), 2
        )) OVER (ORDER BY nr_ano, nr_mes)                                        AS pct_autoatendimento_mes_anterior
    FROM base_ura
    GROUP BY ds_ano_mes, nr_ano, nr_mes
),

-- CTE 6: Resumo global para referencia nas queries principais
resumo_global_ura AS (
    SELECT
        COUNT(sk_ura_navegacao)                                                   AS nr_total_sessoes,
        ROUND(
            100.0 * COUNT(CASE WHEN fl_autoatendimento_concluido = 1 THEN 1 END)
            / NULLIF(COUNT(sk_ura_navegacao), 0), 2
        )                                                                         AS pct_autoatendimento_global,
        ROUND(
            100.0 * COUNT(CASE WHEN fl_abandono_ura = 1 THEN 1 END)
            / NULLIF(COUNT(sk_ura_navegacao), 0), 2
        )                                                                         AS pct_abandono_global,
        ROUND(AVG(CAST(nr_tempo_navegacao_segundos AS DOUBLE)), 2)               AS nr_tempo_medio_global_seg
    FROM base_ura
)

-- Resultado principal: Taxa de auto-atendimento diaria com contexto global
SELECT
    ta.dt_completa,
    ta.nr_ano,
    ta.nr_mes,
    ta.nr_dia,
    ta.ds_ano_mes,
    ta.nr_total_sessoes_ura,
    ta.nr_auto_atendidos,
    ta.nr_transferidos_humano,
    ta.nr_abandonos_ura,
    ta.pct_taxa_autoatendimento,
    ta.pct_transferencia_humano,
    ta.pct_abandono_ura,
    ta.nr_tempo_medio_navegacao_seg,
    ta.nr_tempo_medio_navegacao_min,
    ta.nr_nivel_medio_menu,
    ta.nr_tempo_max_navegacao_seg,
    -- Referencia global
    rgu.pct_autoatendimento_global,
    rgu.pct_abandono_global,
    rgu.nr_tempo_medio_global_seg,
    -- Desvio do dia vs media global
    ROUND(ta.pct_taxa_autoatendimento - rgu.pct_autoatendimento_global, 2)       AS desvio_autoatendimento_vs_global,
    -- Semaforos
    CASE
        WHEN ta.pct_taxa_autoatendimento >= 50 THEN 'VERDE_EXCELENTE'
        WHEN ta.pct_taxa_autoatendimento >= 35 THEN 'VERDE'
        WHEN ta.pct_taxa_autoatendimento >= 20 THEN 'AMARELO'
        ELSE 'VERMELHO_CRITICO'
    END AS semaforo_autoatendimento,
    CASE
        WHEN ta.pct_abandono_ura <= 5  THEN 'VERDE'
        WHEN ta.pct_abandono_ura <= 10 THEN 'AMARELO'
        ELSE 'VERMELHO'
    END AS semaforo_abandono,
    CASE
        WHEN ta.nr_tempo_medio_navegacao_seg <= 60  THEN 'RAPIDO'
        WHEN ta.nr_tempo_medio_navegacao_seg <= 120 THEN 'NORMAL'
        WHEN ta.nr_tempo_medio_navegacao_seg <= 180 THEN 'LENTO'
        ELSE 'MUITO_LENTO'
    END AS ds_classificacao_tempo
FROM taxa_autoatendimento ta
CROSS JOIN resumo_global_ura rgu
ORDER BY
    ta.dt_completa DESC,
    ta.nr_total_sessoes_ura DESC
LIMIT 100;

-- =============================================================================
-- Query complementar 1: Top 10 fluxos da URA (descomente para usar)
-- =============================================================================
-- SELECT
--     tfu.ds_fluxo_ura,
--     tfu.nr_opcao_selecionada,
--     tfu.ds_opcao_selecionada,
--     tfu.nr_acessos,
--     tfu.pct_acessos_total,
--     tfu.pct_autoatendimento_fluxo,
--     tfu.pct_abandono_fluxo,
--     tfu.pct_transferencia_fluxo,
--     tfu.nr_tempo_medio_seg,
--     tfu.rank_acesso
-- FROM top_fluxos_ura tfu
-- WHERE tfu.rank_acesso <= 10
-- ORDER BY tfu.rank_acesso;

-- =============================================================================
-- Query complementar 2: Pontos criticos de abandono (descomente para usar)
-- =============================================================================
-- SELECT
--     pa.ds_fluxo_ura,
--     pa.ds_opcao_selecionada,
--     pa.nr_nivel_menu,
--     pa.nr_total_chegadas,
--     pa.nr_abandonos,
--     pa.pct_abandono_ponto,
--     pa.rank_abandono
-- FROM pontos_abandono pa
-- WHERE pa.rank_abandono <= 10
-- ORDER BY pa.rank_abandono;

-- =============================================================================
-- Query complementar 3: Tendencia mensal da URA (descomente para usar)
-- =============================================================================
-- SELECT
--     tmu.ds_ano_mes,
--     tmu.nr_sessoes_mes,
--     tmu.pct_autoatendimento_mes,
--     tmu.pct_abandono_mes,
--     tmu.nr_tempo_medio_mes_seg,
--     tmu.pct_autoatendimento_mes_anterior,
--     ROUND(tmu.pct_autoatendimento_mes - tmu.pct_autoatendimento_mes_anterior, 2) AS variacao_mom,
--     CASE
--         WHEN tmu.pct_autoatendimento_mes > tmu.pct_autoatendimento_mes_anterior THEN 'MELHORA'
--         WHEN tmu.pct_autoatendimento_mes < tmu.pct_autoatendimento_mes_anterior THEN 'PIORA'
--         ELSE 'ESTAVEL'
--     END AS ds_tendencia
-- FROM tendencia_mensal_ura tmu
-- ORDER BY tmu.nr_ano ASC, tmu.nr_mes ASC
-- LIMIT 24;
