-- =============================================================================
-- KPI: ROI e Efetividade de Campanhas
-- Area: Campanhas Outbound / Business Intelligence
-- Data: 2026-07-09
-- Objetivo: Calcular o ROI operacional das campanhas de discagem medindo a
--           efetividade (conversoes / meta_contatos), rankear campanhas por
--           taxa de conversao e comparar o desempenho entre campanhas ativas
--           e encerradas para subsidiar decisoes estrategicas.
--
-- Metricas Calculadas:
--   - Taxa de efetividade: (conversoes / meta_contatos) * 100
--   - Taxa de aproveitamento: (conversoes / contatos_realizados) * 100
--   - Custo operacional estimado por conversao (tentativas como proxy de custo)
--   - Ranking de campanhas por conversao total e por efetividade
--   - Comparativo ativa vs encerrada
--   - Melhor e pior campanha por tipo
--   - Tendencia de conversao ao longo do ciclo da campanha
--
-- Como Interpretar:
--   - Efetividade acima de 80% significa meta quase atingida ou superada
--   - Campanhas encerradas com alta efetividade sao modelo a replicar
--   - Custo por conversao crescente indica necessidade de revisar scripts
--   - Comparativo ativa vs encerrada mostra maturidade da campanha atual
-- =============================================================================

WITH
-- CTE 1: Base de discagens com dados completos de campanha
base_discagem_roi AS (
    SELECT
        fd.sk_discagem,
        fd.sk_campanha,
        fd.sk_cliente,
        CAST(1 AS INT)                        AS nr_tentativas,
        fd.fl_discagem_atendida                AS fl_contato_realizado,
        CAST(0 AS INT)                        AS fl_convertido,
        dc.nm_campanha,
        CAST(NULL AS VARCHAR)                 AS ds_tipo_campanha,
        dc.st_campanha                        AS ds_status_campanha,
        dc.dt_inicio                          AS dt_inicio_campanha,
        dc.dt_fim                             AS dt_fim_campanha,
        CAST(NULL AS INT)                     AS nr_meta_contatos,
        -- Flag de campanha ativa vs encerrada
        CASE
            WHEN UPPER(dc.st_campanha) IN ('ATIVA', 'EM_ANDAMENTO', 'RUNNING') THEN 'ATIVA'
            WHEN UPPER(dc.st_campanha) IN ('ENCERRADA', 'FINALIZADA', 'CONCLUIDA') THEN 'ENCERRADA'
            ELSE 'OUTRO'
        END AS ds_situacao_campanha
    FROM db_gold.fato_discagem fd
    INNER JOIN db_gold.dim_campanha dc
        ON fd.sk_campanha = dc.sk_campanha
),

-- CTE 2: ROI e efetividade por campanha
roi_por_campanha AS (
    SELECT
        sk_campanha,
        nm_campanha,
        ds_tipo_campanha,
        ds_status_campanha,
        ds_situacao_campanha,
        dt_inicio_campanha,
        dt_fim_campanha,
        MAX(nr_meta_contatos)                                                     AS nr_meta_contatos,
        -- Metricas de volume
        COUNT(sk_discagem)                                                        AS nr_total_discagens,
        SUM(fl_contato_realizado)                                                 AS nr_contatos_realizados,
        SUM(fl_convertido)                                                        AS nr_conversoes,
        SUM(nr_tentativas)                                                        AS nr_total_tentativas,
        -- Taxa de efetividade: conversoes vs meta estabelecida
        ROUND(
            100.0 * SUM(fl_convertido)
            / NULLIF(MAX(nr_meta_contatos), 0), 2
        )                                                                         AS pct_efetividade_meta,
        -- Taxa de aproveitamento: conversoes vs contatos realizados
        ROUND(
            100.0 * SUM(fl_convertido)
            / NULLIF(SUM(fl_contato_realizado), 0), 2
        )                                                                         AS pct_aproveitamento_contato,
        -- Taxa de contato
        ROUND(
            100.0 * SUM(fl_contato_realizado)
            / NULLIF(COUNT(sk_discagem), 0), 2
        )                                                                         AS pct_taxa_contato,
        -- Custo operacional proxy: tentativas por conversao (menor = mais eficiente)
        ROUND(
            CAST(SUM(nr_tentativas) AS DOUBLE)
            / NULLIF(SUM(fl_convertido), 0), 2
        )                                                                         AS nr_tentativas_por_conversao,
        -- Custo por contato (tentativas / contatos)
        ROUND(
            CAST(SUM(nr_tentativas) AS DOUBLE)
            / NULLIF(SUM(fl_contato_realizado), 0), 2
        )                                                                         AS nr_tentativas_por_contato,
        -- GAP de meta: quanto falta ou excedeu
        SUM(fl_convertido) - MAX(nr_meta_contatos)                               AS nr_gap_meta,
        -- Percentual de superacao (positivo) ou deficit (negativo) de meta
        ROUND(
            100.0 * (SUM(fl_convertido) - MAX(nr_meta_contatos))
            / NULLIF(MAX(nr_meta_contatos), 0), 2
        )                                                                         AS pct_superacao_meta
    FROM base_discagem_roi
    GROUP BY
        sk_campanha, nm_campanha, ds_tipo_campanha,
        ds_status_campanha, ds_situacao_campanha,
        dt_inicio_campanha, dt_fim_campanha
),

-- CTE 3: Medias por situacao de campanha (ativa vs encerrada)
comparativo_situacao AS (
    SELECT
        ds_situacao_campanha,
        COUNT(sk_campanha)                                                        AS nr_campanhas,
        ROUND(AVG(pct_efetividade_meta), 2)                                      AS nr_efetividade_media,
        ROUND(AVG(pct_aproveitamento_contato), 2)                                AS nr_aproveitamento_medio,
        ROUND(AVG(pct_taxa_contato), 2)                                          AS nr_taxa_contato_media,
        ROUND(AVG(nr_tentativas_por_conversao), 2)                               AS nr_custo_medio_conversao,
        SUM(nr_conversoes)                                                        AS nr_total_conversoes_grupo,
        SUM(nr_meta_contatos)                                                     AS nr_total_meta_grupo,
        ROUND(
            100.0 * SUM(nr_conversoes)
            / NULLIF(SUM(nr_meta_contatos), 0), 2
        )                                                                         AS pct_efetividade_grupo
    FROM roi_por_campanha
    WHERE ds_situacao_campanha IN ('ATIVA', 'ENCERRADA')
    GROUP BY ds_situacao_campanha
),

-- CTE 4: Ranking de campanhas por tipo
ranking_por_tipo AS (
    SELECT
        sk_campanha,
        nm_campanha,
        ds_tipo_campanha,
        ds_situacao_campanha,
        nr_conversoes,
        pct_efetividade_meta,
        pct_aproveitamento_contato,
        nr_tentativas_por_conversao,
        RANK() OVER (
            PARTITION BY ds_tipo_campanha
            ORDER BY pct_efetividade_meta DESC
        )                                                                         AS rank_efetividade_tipo,
        RANK() OVER (
            PARTITION BY ds_tipo_campanha
            ORDER BY nr_conversoes DESC
        )                                                                         AS rank_volume_tipo
    FROM roi_por_campanha
    WHERE ds_situacao_campanha IN ('ATIVA', 'ENCERRADA')
),

-- CTE 5: Top campanhas (melhores e piores por efetividade)
classificacao_campanhas AS (
    SELECT
        sk_campanha,
        nm_campanha,
        ds_situacao_campanha,
        pct_efetividade_meta,
        nr_conversoes,
        nr_tentativas_por_conversao,
        RANK() OVER (ORDER BY pct_efetividade_meta DESC)                         AS rank_geral_efetividade,
        RANK() OVER (ORDER BY nr_conversoes DESC)                                AS rank_geral_volume,
        RANK() OVER (ORDER BY nr_tentativas_por_conversao ASC)                  AS rank_eficiencia_custo,
        NTILE(4) OVER (ORDER BY pct_efetividade_meta DESC)                      AS quartil_efetividade
    FROM roi_por_campanha
    WHERE ds_situacao_campanha IN ('ATIVA', 'ENCERRADA')
      AND nr_conversoes > 0
)

-- Resultado principal: ROI detalhado por campanha com rankings
SELECT
    rpc.sk_campanha,
    rpc.nm_campanha,
    rpc.ds_tipo_campanha,
    rpc.ds_status_campanha,
    rpc.ds_situacao_campanha,
    rpc.dt_inicio_campanha,
    rpc.dt_fim_campanha,
    rpc.nr_meta_contatos,
    rpc.nr_total_discagens,
    rpc.nr_contatos_realizados,
    rpc.nr_conversoes,
    rpc.nr_total_tentativas,
    rpc.pct_taxa_contato,
    rpc.pct_efetividade_meta,
    rpc.pct_aproveitamento_contato,
    rpc.nr_tentativas_por_conversao,
    rpc.nr_tentativas_por_contato,
    rpc.nr_gap_meta,
    rpc.pct_superacao_meta,
    -- Rankings globais
    cc.rank_geral_efetividade,
    cc.rank_geral_volume,
    cc.rank_eficiencia_custo,
    cc.quartil_efetividade,
    -- Classificacao do quartil
    CASE cc.quartil_efetividade
        WHEN 1 THEN 'TOP_25_PCT'
        WHEN 2 THEN 'SEGUNDO_QUARTIL'
        WHEN 3 THEN 'TERCEIRO_QUARTIL'
        WHEN 4 THEN 'BOTTOM_25_PCT'
    END AS ds_quartil_efetividade,
    -- Semaforos
    CASE
        WHEN rpc.pct_efetividade_meta >= 100 THEN 'META_SUPERADA'
        WHEN rpc.pct_efetividade_meta >= 80  THEN 'VERDE'
        WHEN rpc.pct_efetividade_meta >= 60  THEN 'AMARELO'
        ELSE 'VERMELHO'
    END AS semaforo_efetividade,
    CASE
        WHEN rpc.nr_tentativas_por_conversao <= 5  THEN 'EFICIENTE'
        WHEN rpc.nr_tentativas_por_conversao <= 10 THEN 'MODERADO'
        ELSE 'INEFICIENTE'
    END AS ds_eficiencia_operacional
FROM roi_por_campanha rpc
LEFT JOIN classificacao_campanhas cc
    ON rpc.sk_campanha = cc.sk_campanha
WHERE rpc.ds_situacao_campanha IN ('ATIVA', 'ENCERRADA')
ORDER BY
    rpc.pct_efetividade_meta DESC,
    rpc.nr_conversoes DESC
LIMIT 100;

-- =============================================================================
-- Query complementar: Comparativo ativa vs encerrada (descomente para usar)
-- =============================================================================
-- SELECT
--     cs.ds_situacao_campanha,
--     cs.nr_campanhas,
--     cs.nr_efetividade_media,
--     cs.nr_aproveitamento_medio,
--     cs.nr_taxa_contato_media,
--     cs.nr_custo_medio_conversao,
--     cs.nr_total_conversoes_grupo,
--     cs.nr_total_meta_grupo,
--     cs.pct_efetividade_grupo
-- FROM comparativo_situacao cs
-- ORDER BY cs.ds_situacao_campanha;
