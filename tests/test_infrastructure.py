"""
test_infrastructure.py
======================
Valida os scripts e configs de infraestrutura sem fazer chamadas AWS.

Cobertura:
  - IAM policies: estrutura JSON, campos obrigatórios, Effect válido
  - Scripts de setup: constantes obrigatórias, presença de --dry-run
  - QuickSight: datasets definidos, queries não vazias
  - Pipeline runner: lista de jobs, constantes de configuração
"""

import json
import re
import ast
import pytest
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
INFRA_DIR = REPO_ROOT / "infrastructure"
QS_DIR    = INFRA_DIR / "quicksight"


# ─── IAM Policies ─────────────────────────────────────────────────────────────

class TestIAMPolicies:

    def _load_policy(self, path: Path) -> dict:
        assert path.exists(), f"Policy não encontrada: {path}"
        with open(path) as f:
            return json.load(f)

    @pytest.fixture
    def qs_policy(self):
        return self._load_policy(QS_DIR / "iam_quicksight_policy.json")

    def test_quicksight_policy_tem_version(self, qs_policy):
        assert "Version" in qs_policy, "Policy sem campo 'Version'"
        assert qs_policy["Version"] == "2012-10-17"

    def test_quicksight_policy_tem_statements(self, qs_policy):
        assert "Statement" in qs_policy
        assert isinstance(qs_policy["Statement"], list)
        assert len(qs_policy["Statement"]) > 0

    def test_quicksight_policy_effects_validos(self, qs_policy):
        for i, stmt in enumerate(qs_policy["Statement"]):
            assert "Effect" in stmt, f"Statement[{i}] sem 'Effect'"
            assert stmt["Effect"] in ("Allow", "Deny"), (
                f"Statement[{i}]: Effect inválido: {stmt['Effect']}"
            )

    def test_quicksight_policy_tem_acao_athena(self, qs_policy):
        """Policy deve incluir permissões Athena."""
        all_actions = []
        for stmt in qs_policy["Statement"]:
            actions = stmt.get("Action", [])
            if isinstance(actions, str):
                actions = [actions]
            all_actions.extend(actions)

        athena_actions = [a for a in all_actions if a.startswith("athena:")]
        assert len(athena_actions) > 0, (
            "Policy QuickSight não inclui ações Athena — "
            "QuickSight não conseguirá executar queries"
        )

    def test_quicksight_policy_tem_acao_s3(self, qs_policy):
        """Policy deve incluir permissões S3 para leitura dos dados."""
        all_actions = []
        for stmt in qs_policy["Statement"]:
            actions = stmt.get("Action", [])
            if isinstance(actions, str):
                actions = [actions]
            all_actions.extend(actions)

        s3_actions = [a for a in all_actions if a.startswith("s3:")]
        assert len(s3_actions) > 0, (
            "Policy QuickSight não inclui ações S3 — "
            "QuickSight não conseguirá ler os arquivos Parquet"
        )

    def test_quicksight_policy_referencia_bucket_correto(self, qs_policy):
        """A policy deve referenciar o bucket act-cc-dev-lakehouse."""
        policy_str = json.dumps(qs_policy)
        assert "act-cc-dev-lakehouse" in policy_str, (
            "Policy não referencia o bucket 'act-cc-dev-lakehouse'"
        )

    def test_quicksight_policy_tem_acesso_glue(self, qs_policy):
        """Policy deve incluir permissões Glue para o Athena acessar o catálogo."""
        all_actions = []
        for stmt in qs_policy["Statement"]:
            actions = stmt.get("Action", [])
            if isinstance(actions, str):
                actions = [actions]
            all_actions.extend(actions)

        glue_actions = [a for a in all_actions if a.startswith("glue:")]
        assert len(glue_actions) > 0, (
            "Policy não inclui ações Glue — Athena usa o Glue Catalog"
        )


# ─── Script QuickSight ────────────────────────────────────────────────────────

class TestQuickSightScript:

    @pytest.fixture(scope="class")
    def qs_source(self):
        path = QS_DIR / "01_setup_quicksight.py"
        assert path.exists(), f"Script não encontrado: {path}"
        return path.read_text(encoding="utf-8")

    def test_define_datasets(self, qs_source):
        """O script deve definir exatamente 5 datasets SPICE."""
        # Conta entradas na lista DATASETS
        matches = re.findall(r'"id"\s*:\s*"ds-', qs_source)
        assert len(matches) == 5, (
            f"Esperados 5 datasets, encontrados {len(matches)}"
        )

    def test_todos_datasets_tem_sql(self, qs_source):
        """Cada dataset deve ter um campo 'sql' não vazio."""
        # Verifica que existem strings longas de SQL
        sql_blocks = re.findall(r'"sql"\s*:\s*"""(.*?)"""', qs_source, re.DOTALL)
        assert len(sql_blocks) == 5, (
            f"Esperados 5 blocos SQL, encontrados {len(sql_blocks)}"
        )
        for i, block in enumerate(sql_blocks):
            assert len(block.strip()) > 100, (
                f"Dataset {i+1}: SQL parece vazio ou muito curto"
            )

    def test_datasets_cobrem_todos_kpis(self, qs_source):
        """Os datasets devem referenciar as principais tabelas fato."""
        tabelas_esperadas = [
            "fato_chamada", "fato_ticket", "fato_discagem",
            "fato_chat", "fato_whatsapp",
        ]
        for tabela in tabelas_esperadas:
            assert tabela in qs_source, (
                f"Script QuickSight não referencia '{tabela}' — "
                f"KPI relacionado não estará disponível no dashboard"
            )

    def test_constantes_aws_definidas(self, qs_source):
        """As constantes AWS devem estar definidas no script."""
        constantes = [
            "DEFAULT_REGION",
            "DEFAULT_BUCKET",
            "DEFAULT_ATHENA_WG",
            "DATASOURCE_ID",
            "QUICKSIGHT_ROLE_NAME",
        ]
        for const in constantes:
            assert const in qs_source, (
                f"Constante '{const}' não encontrada em 01_setup_quicksight.py"
            )

    def test_bucket_correto(self, qs_source):
        """O bucket padrão deve ser act-cc-dev-lakehouse."""
        assert "act-cc-dev-lakehouse" in qs_source

    def test_suporta_dry_run(self, qs_source):
        """O script deve suportar --dry-run para teste sem criar recursos."""
        assert "dry_run" in qs_source.lower() or "dry-run" in qs_source.lower(), (
            "Script não suporta --dry-run — dificulta testes sem custos AWS"
        )


# ─── Pipeline Runner ──────────────────────────────────────────────────────────

class TestPipelineRunner:

    @pytest.fixture(scope="class")
    def runner_source(self):
        path = INFRA_DIR / "06_run_pipeline.py"
        assert path.exists()
        return path.read_text(encoding="utf-8")

    def test_referencia_40_jobs(self, runner_source):
        """O runner deve orquestrar os 40 jobs (18 B→S + 22 S→G).

        Jobs são nomeados com hífens no estilo Glue:
        ex: "job-tb-chamada-bronze-to-silver", "job-dim-data-gold"
        """
        # Padrão com hífens: job-tb-*, job-dim-*, job-fato-*
        job_refs = re.findall(
            r'"job-(?:tb|dim|fato)-[\w-]+"',
            runner_source,
            re.IGNORECASE
        )
        assert len(job_refs) >= 40, (
            f"Runner referencia apenas {len(job_refs)} jobs — esperados pelo menos 40"
        )

    def test_suporta_dry_run(self, runner_source):
        assert "dry_run" in runner_source.lower() or "dry-run" in runner_source.lower()

    def test_suporta_max_parallel(self, runner_source):
        """Runner deve suportar controle de paralelismo."""
        assert "max_parallel" in runner_source.lower() or "max-parallel" in runner_source.lower(), (
            "Runner não suporta --max-parallel — sem controle de paralelismo"
        )


# ─── Estrutura do Repositório ─────────────────────────────────────────────────

class TestRepositoryStructure:
    """Valida que os arquivos e diretórios obrigatórios existem."""

    def test_readme_existe(self):
        assert (REPO_ROOT / "README.md").exists()

    def test_requirements_existe(self):
        assert (REPO_ROOT / "requirements.txt").exists()

    def test_requirements_tem_boto3(self):
        content = (REPO_ROOT / "requirements.txt").read_text()
        assert "boto3" in content

    def test_requirements_tem_pyspark(self):
        content = (REPO_ROOT / "requirements.txt").read_text()
        assert "pyspark" in content

    def test_requirements_tem_pytest(self):
        content = (REPO_ROOT / "requirements.txt").read_text()
        assert "pytest" in content

    def test_docs_existem(self):
        docs_dir = REPO_ROOT / "docs"
        assert docs_dir.exists()
        docs = list(docs_dir.glob("*.md"))
        assert len(docs) >= 3, f"Esperados >= 3 docs, encontrados {len(docs)}"

    def test_github_actions_workflow_existe(self):
        workflow = REPO_ROOT / ".github" / "workflows" / "ci.yml"
        assert workflow.exists(), "Workflow de CI não encontrado"

    def test_flake8_config_existe(self):
        assert (REPO_ROOT / ".flake8").exists(), ".flake8 não encontrado"

    def test_pipeline_dirs_existem(self):
        for d in ["bronze_to_silver", "silver_to_gold", "ingestion"]:
            path = REPO_ROOT / "pipeline" / d
            assert path.exists(), f"Diretório de pipeline ausente: pipeline/{d}"

    def test_sql_kpis_dir_existe(self):
        assert (REPO_ROOT / "sql" / "athena_kpis").exists()

    def test_quicksight_infra_existe(self):
        assert (REPO_ROOT / "infrastructure" / "quicksight").exists()
