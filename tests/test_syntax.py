"""
test_syntax.py
==============
Valida a sintaxe Python de todos os jobs PySpark do pipeline.

Usa py_compile para verificar sem executar — funciona mesmo com
imports de awsglue que não estão disponíveis no ambiente de CI.
"""

import os
import py_compile
import pytest
from pathlib import Path

# Raiz do repositório (dois níveis acima de tests/)
REPO_ROOT = Path(__file__).parent.parent

PIPELINE_DIRS = [
    REPO_ROOT / "pipeline" / "bronze_to_silver",
    REPO_ROOT / "pipeline" / "silver_to_gold",
    REPO_ROOT / "pipeline" / "ingestion",
]

INFRA_DIRS = [
    REPO_ROOT / "infrastructure",
    REPO_ROOT / "lambda",
]


def collect_py_files(directories: list) -> list:
    """Coleta todos os .py de uma lista de diretórios, recursivamente."""
    files = []
    for d in directories:
        if d.exists():
            files.extend(sorted(d.rglob("*.py")))
    return files


# ─── Parametrização dinâmica ──────────────────────────────────────────────────

PIPELINE_FILES = collect_py_files(PIPELINE_DIRS)
INFRA_FILES    = collect_py_files(INFRA_DIRS)

# Ids legíveis no relatório de testes
def file_id(path: Path) -> str:
    return str(path.relative_to(REPO_ROOT))


# ─── Testes ───────────────────────────────────────────────────────────────────

class TestPipelineSyntax:
    """Valida todos os jobs do pipeline Bronze→Silver e Silver→Gold."""

    @pytest.mark.parametrize("py_file", PIPELINE_FILES, ids=[file_id(f) for f in PIPELINE_FILES])
    def test_job_syntax_is_valid(self, py_file: Path):
        """
        Cada job PySpark deve ter sintaxe Python válida.
        py_compile detecta erros sem executar o arquivo.
        """
        try:
            py_compile.compile(str(py_file), doraise=True)
        except py_compile.PyCompileError as e:
            pytest.fail(f"Erro de sintaxe em {file_id(py_file)}:\n{e}")

    def test_bronze_to_silver_job_count(self):
        """Deve existir exatamente 18 jobs Bronze→Silver."""
        bronze_dir = REPO_ROOT / "pipeline" / "bronze_to_silver"
        jobs = [f for f in bronze_dir.glob("job_*.py") if f.is_file()]
        assert len(jobs) == 18, (
            f"Esperados 18 jobs Bronze→Silver, encontrados {len(jobs)}.\n"
            f"Jobs: {sorted(j.name for j in jobs)}"
        )

    def test_silver_to_gold_job_count(self):
        """Deve existir exatamente 22 jobs Silver→Gold (11 dims + 11 fatos)."""
        gold_dir = REPO_ROOT / "pipeline" / "silver_to_gold"
        jobs = [f for f in gold_dir.glob("job_*.py") if f.is_file()]
        assert len(jobs) == 22, (
            f"Esperados 22 jobs Silver→Gold, encontrados {len(jobs)}.\n"
            f"Jobs: {sorted(j.name for j in jobs)}"
        )

    def test_all_bronze_jobs_have_required_patterns(self):
        """
        Todo job Bronze→Silver deve referenciar:
        - QUARANTINE_PATH (quarentena automática)
        - hash_registro (hash de integridade)
        - MERGE INTO (idempotência Iceberg)
        """
        bronze_dir = REPO_ROOT / "pipeline" / "bronze_to_silver"
        required_patterns = ["QUARANTINE_PATH", "hash_registro", "MERGE INTO"]

        for job_file in sorted(bronze_dir.glob("job_*.py")):
            content = job_file.read_text(encoding="utf-8")
            for pattern in required_patterns:
                assert pattern in content, (
                    f"{job_file.name}: padrão obrigatório ausente: '{pattern}'"
                )

    def test_all_gold_jobs_have_required_patterns(self):
        """
        Todo job Silver→Gold deve ter MERGE INTO e escrever em db_gold.

        Estratégia de surrogate key por tipo:
        - Dimensões: row_number() over Window — tabelas pequenas, sem risco de OOM
        - Fatos: monotonically_increasing_id() — tabelas grandes, distribuído sem shuffle
        - Exceções (sem SK gerada): dim_data (SK = date int) e dim_canal (SKs hardcoded)
        """
        gold_dir = REPO_ROOT / "pipeline" / "silver_to_gold"

        # Todos os jobs devem ter MERGE INTO e escrever em db_gold
        for job_file in sorted(gold_dir.glob("job_*.py")):
            content = job_file.read_text(encoding="utf-8")
            for pattern in ["MERGE INTO", "db_gold"]:
                assert pattern in content, (
                    f"{job_file.name}: padrão obrigatório ausente: '{pattern}'"
                )

        # Jobs de dimensão devem usar row_number() para gerar SKs
        # (tabelas pequenas — row_number é seguro e sequencial)
        dim_exceptions = {"job_dim_data_gold.py", "job_dim_canal_gold.py"}
        for job_file in sorted(gold_dir.glob("job_dim_*.py")):
            if job_file.name in dim_exceptions:
                continue
            content = job_file.read_text(encoding="utf-8")
            assert "row_number" in content, (
                f"{job_file.name}: dimensão sem row_number() para surrogate key"
            )

        # Jobs de fato devem usar monotonically_increasing_id()
        # (tabelas grandes — evita OOM de row_number sem partitionBy)
        for job_file in sorted(gold_dir.glob("job_fato_*.py")):
            content = job_file.read_text(encoding="utf-8")
            assert "monotonically_increasing_id" in content, (
                f"{job_file.name}: fato sem monotonically_increasing_id() — "
                f"risco de OOM com row_number em tabelas grandes"
            )


class TestInfrastructureSyntax:
    """Valida sintaxe dos scripts de infraestrutura."""

    @pytest.mark.parametrize("py_file", INFRA_FILES, ids=[file_id(f) for f in INFRA_FILES])
    def test_infra_syntax_is_valid(self, py_file: Path):
        try:
            py_compile.compile(str(py_file), doraise=True)
        except py_compile.PyCompileError as e:
            pytest.fail(f"Erro de sintaxe em {file_id(py_file)}:\n{e}")

    def test_setup_scripts_exist(self):
        """Os 6 scripts de setup da infraestrutura devem existir."""
        infra_dir = REPO_ROOT / "infrastructure"
        expected = [
            "01_setup_s3.py",
            "02_setup_glue.py",
            "03_setup_lambda.py",
            "04_setup_redshift.py",
            "05_setup_emr_serverless.py",
            "06_run_pipeline.py",
        ]
        for script in expected:
            path = infra_dir / script
            assert path.exists(), f"Script de infra ausente: {script}"

    def test_quicksight_setup_exists(self):
        """Script e policy IAM do QuickSight devem existir."""
        qs_dir = REPO_ROOT / "infrastructure" / "quicksight"
        assert (qs_dir / "01_setup_quicksight.py").exists(), \
            "infrastructure/quicksight/01_setup_quicksight.py não encontrado"
        assert (qs_dir / "iam_quicksight_policy.json").exists(), \
            "infrastructure/quicksight/iam_quicksight_policy.json não encontrado"
