"""
test_sql_kpis.py
================
Valida estrutura e integridade dos 12 arquivos SQL de KPIs (sql/athena_kpis/).

Não executa as queries no Athena — verifica:
  - Contagem e nomes de arquivos
  - Estrutura mínima de cada query (SELECT, FROM db_gold, WHERE, GROUP BY)
  - Referência às tabelas Gold corretas
  - Ausência de nomes de colunas inválidos (colunas que não existem no schema)
  - Padrões de qualidade SQL (NULLIF para evitar divisão por zero, etc.)
"""

import re
import pytest
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
SQL_DIR   = REPO_ROOT / "sql" / "athena_kpis"

# Todos os 12 arquivos esperados
EXPECTED_FILES = [
    "01_volume_desempenho_chamadas.sql",
    "02_performance_operadores.sql",
    "03_qualidade_atendimento.sql",
    "04_volume_tickets.sql",
    "05_eficiencia_tickets.sql",
    "06_volume_chat_whatsapp.sql",
    "07_satisfacao_digital.sql",
    "08_desempenho_campanhas.sql",
    "09_roi_campanhas.sql",
    "10_jornada_operador.sql",
    "11_ocupacao_filas.sql",
    "12_efetividade_ura.sql",
]

# Tabelas Gold reais do projeto (schema validado via Athena em 2026-07-10)
GOLD_TABLES = [
    "fato_chamada", "fato_chat", "fato_ticket", "fato_whatsapp",
    "fato_discagem", "fato_jornada_operador", "fato_metricas_operacionais",
    "fato_qualidade", "fato_mensagem_chat", "fato_interacao_ticket",
    "fato_ura_navegacao",
    "dim_data", "dim_cliente", "dim_operador", "dim_fila", "dim_campanha",
    "dim_canal", "dim_skill", "dim_status_chamada", "dim_status_ticket",
    "dim_categoria_ticket", "dim_prioridade_ticket",
    "dim_supervisor",   # derivada de dim_operador (job_dim_operador_gold.py)
]

# Padrões de coluna que NÃO existem no schema Gold lidas de tabelas base
# (ex: do2.fl_fim_semana). Usa word boundary + prefixo de alias de tabela
# para evitar falsos positivos com aliases de CTE criados na própria query.
INVALID_COLUMN_PATTERNS = [
    # fl_fim_semana lida de uma tabela (ex: dd.fl_fim_semana) — correto é fl_fim_de_semana
    (r"\b\w+\.fl_fim_semana\b", "correto: fl_fim_de_semana (dim_data)"),
]


# ─── Helpers ─────────────────────────────────────────────────────────────────

def read_sql(filename: str) -> str:
    path = SQL_DIR / filename
    assert path.exists(), f"Arquivo SQL não encontrado: {path}"
    return path.read_text(encoding="utf-8")


def strip_comments(sql: str) -> str:
    """Remove comentários SQL (-- e /* */) para análise limpa."""
    sql = re.sub(r"--[^\n]*", "", sql)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    return sql


# ─── Testes de contagem ───────────────────────────────────────────────────────

class TestSQLFileCount:

    def test_todos_12_arquivos_existem(self):
        """Os 12 arquivos KPI devem existir em sql/athena_kpis/."""
        for filename in EXPECTED_FILES:
            path = SQL_DIR / filename
            assert path.exists(), f"Arquivo ausente: {filename}"

    def test_nenhum_arquivo_extra(self):
        """Não deve existir arquivo SQL além dos 12 esperados."""
        found = {f.name for f in SQL_DIR.glob("*.sql")}
        expected = set(EXPECTED_FILES)
        extra = found - expected
        assert not extra, (
            f"Arquivos SQL não esperados em sql/athena_kpis/: {sorted(extra)}"
        )

    def test_arquivos_numerados_em_sequencia(self):
        """Os arquivos devem ser numerados de 01 a 12 sem lacunas."""
        nums = sorted(
            int(f.name[:2])
            for f in SQL_DIR.glob("*.sql")
            if f.name[:2].isdigit()
        )
        assert nums == list(range(1, 13)), (
            f"Numeração incorreta: {nums}"
        )


# ─── Testes de estrutura por arquivo ─────────────────────────────────────────

@pytest.mark.parametrize("filename", EXPECTED_FILES)
class TestSQLStructure:

    def test_arquivo_nao_esta_vazio(self, filename):
        sql = read_sql(filename)
        assert len(sql.strip()) > 200, (
            f"{filename}: arquivo parece vazio ou muito curto ({len(sql)} bytes)"
        )

    def test_contem_select(self, filename):
        sql = strip_comments(read_sql(filename)).upper()
        assert "SELECT" in sql, f"{filename}: não contém SELECT"

    def test_referencia_db_gold(self, filename):
        sql = strip_comments(read_sql(filename)).lower()
        assert "db_gold." in sql, (
            f"{filename}: não referencia db_gold — deve usar 'db_gold.<tabela>'"
        )

    def test_contem_where_com_filtro_de_ano(self, filename):
        """
        Todas as queries devem filtrar por alguma coluna de data/período
        para evitar full scans desnecessários nas partições Gold.

        Aceita: nr_ano (padrão), dt_inicio_campanha (KPIs 08/09 filtram por
        data da campanha) ou dt_inicio (filtros diretos de timestamp).
        """
        sql = strip_comments(read_sql(filename)).lower()
        filtros_aceitos = ["nr_ano", "dt_inicio_campanha", "dt_inicio", "dt_completa"]
        has_filter = any(f in sql for f in filtros_aceitos)
        assert has_filter, (
            f"{filename}: não filtra por período — risco de full scan nas partições. "
            f"Use nr_ano, dt_inicio_campanha ou outra coluna de data."
        )

    def test_nao_usa_select_star_em_tabelas_base(self, filename):
        """
        SELECT * de tabelas Gold não é permitido — prejudica performance e clareza.

        Permitido: SELECT * FROM cte_name (CTE previamente definida na mesma query),
        pois o schema já está explicitamente definido na CTE.
        Não permitido: SELECT * FROM db_gold.<tabela>
        """
        sql = strip_comments(read_sql(filename))
        # Detecta SELECT * seguido de FROM db_gold (tabela base, não CTE)
        pattern = re.compile(r"SELECT\s+\*\s+FROM\s+db_gold\.", re.IGNORECASE)
        match = pattern.search(sql)
        assert not match, (
            f"{filename}: contém 'SELECT * FROM db_gold.<tabela>' — "
            f"liste colunas explicitamente para evitar full scans"
        )

    def test_nao_usa_colunas_invalidas(self, filename):
        """
        Nenhuma query deve referenciar colunas que não existem no schema Gold.
        Usa word boundary (\\b) para evitar falsos positivos com substrings.
        """
        sql = strip_comments(read_sql(filename))
        for pattern, descricao in INVALID_COLUMN_PATTERNS:
            match = re.search(pattern, sql, re.IGNORECASE)
            assert not match, (
                f"{filename}: referencia coluna inválida '{match.group()}' — {descricao}"
            )


# ─── Testes de qualidade SQL ─────────────────────────────────────────────────

class TestSQLQuality:

    def test_queries_com_divisao_usam_nullif(self):
        """
        Queries que calculam percentuais devem usar NULLIF para
        evitar divisão por zero.
        """
        queries_com_pct = [
            f for f in EXPECTED_FILES
            if any(kw in f for kw in ["desempenho", "performance", "satisfacao",
                                       "eficiencia", "roi", "efetividade"])
        ]
        for filename in queries_com_pct:
            sql = strip_comments(read_sql(filename)).upper()
            if "/" in sql and ("100" in sql or "PCT" in sql or "TAXA" in sql):
                assert "NULLIF" in sql, (
                    f"{filename}: contém divisão sem NULLIF — risco de divisão por zero"
                )

    def test_queries_usam_round_em_metricas_numericas(self):
        """
        Métricas numéricas (médias, taxas) devem usar ROUND para
        evitar ruído de ponto flutuante nos dashboards.
        """
        for filename in EXPECTED_FILES:
            sql = strip_comments(read_sql(filename)).upper()
            if "AVG(" in sql:
                assert "ROUND(" in sql, (
                    f"{filename}: usa AVG sem ROUND — adicione ROUND para "
                    f"controlar casas decimais nos dashboards"
                )

    def test_query_kpi01_tem_metricas_obrigatorias(self):
        """KPI 01 deve calcular TMA, taxa de atendimento e taxa de abandono."""
        sql = read_sql("01_volume_desempenho_chamadas.sql").lower()
        for metrica in ["tma", "atendimento", "abandono"]:
            assert metrica in sql, (
                f"01_volume_desempenho_chamadas.sql: "
                f"métrica obrigatória ausente: '{metrica}'"
            )

    def test_query_kpi09_tem_roi(self):
        """KPI 09 deve calcular ROI de campanhas."""
        sql = read_sql("09_roi_campanhas.sql").lower()
        assert "roi" in sql, (
            "09_roi_campanhas.sql: não calcula ROI"
        )

    def test_query_kpi12_referencia_fato_ura(self):
        """KPI 12 deve referenciar fato_ura_navegacao."""
        sql = read_sql("12_efetividade_ura.sql").lower()
        assert "fato_ura_navegacao" in sql, (
            "12_efetividade_ura.sql: não referencia fato_ura_navegacao"
        )

    def test_todas_queries_referenciam_tabelas_gold_validas(self):
        """
        Cada tabela referenciada nas queries deve existir no schema Gold.
        """
        for filename in EXPECTED_FILES:
            sql = strip_comments(read_sql(filename)).lower()
            # Extrai referências db_gold.tabela
            refs = re.findall(r"db_gold\.(\w+)", sql)
            for ref in refs:
                assert ref in GOLD_TABLES, (
                    f"{filename}: referencia tabela inexistente 'db_gold.{ref}'. "
                    f"Tabelas válidas: {sorted(GOLD_TABLES)}"
                )
