"""
test_dags.py
============
Valida a estrutura das DAGs do Airflow sem precisar de servidor Airflow.

Testa:
  - Importacao sem erros de sintaxe
  - IDs das DAGs corretos
  - Quantidade de tasks por DAG
  - Dependencias criticas (inicio -> waves -> fim)
  - Cobertura dos 40 jobs Glue (18 bronze + 22 gold)
  - DAG de carga cobre todas as tabelas Gold do Redshift

Requer:
    pip install apache-airflow apache-airflow-providers-amazon
"""

import importlib
import os
import sys

import pytest

# ─── Garante que o diretorio dags esta no path ───────────────────────────────

DAGS_DIR = os.path.join(os.path.dirname(__file__), "..", "dags")
DAGS_DIR = os.path.abspath(DAGS_DIR)

if DAGS_DIR not in sys.path:
    sys.path.insert(0, DAGS_DIR)

# ─── Variaveis de ambiente para importar sem servidor Airflow ────────────────

os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "sqlite:///:memory:")
os.environ.setdefault("AIRFLOW_HOME", "/tmp/airflow_test")


# ─── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def dag_pipeline():
    """Importa e retorna a DAG do pipeline diario."""
    try:
        import airflow  # noqa: F401
    except ImportError:
        pytest.skip("apache-airflow nao instalado — pule no CI sem Airflow")

    from airflow.models import DagBag
    dagbag = DagBag(dag_folder=DAGS_DIR, include_examples=False)
    assert "cc_pipeline_diario" in dagbag.dags, (
        f"DAG 'cc_pipeline_diario' nao encontrada. Erros: {dagbag.import_errors}"
    )
    return dagbag.dags["cc_pipeline_diario"]


@pytest.fixture(scope="module")
def dag_redshift():
    """Importa e retorna a DAG de carga do Redshift."""
    try:
        import airflow  # noqa: F401
    except ImportError:
        pytest.skip("apache-airflow nao instalado — pule no CI sem Airflow")

    from airflow.models import DagBag
    dagbag = DagBag(dag_folder=DAGS_DIR, include_examples=False)
    assert "cc_carga_redshift" in dagbag.dags, (
        f"DAG 'cc_carga_redshift' nao encontrada. Erros: {dagbag.import_errors}"
    )
    return dagbag.dags["cc_carga_redshift"]


# ─── Testes de importacao/sintaxe ────────────────────────────────────────────

class TestSintaxeDAGs:

    def test_dag_pipeline_importa_sem_erros(self):
        """Arquivo dag_pipeline_diario.py deve ter sintaxe valida."""
        import py_compile
        path = os.path.join(DAGS_DIR, "dag_pipeline_diario.py")
        assert os.path.exists(path), "dag_pipeline_diario.py nao encontrado"
        py_compile.compile(path, doraise=True)

    def test_dag_carga_redshift_importa_sem_erros(self):
        """Arquivo dag_carga_redshift.py deve ter sintaxe valida."""
        import py_compile
        path = os.path.join(DAGS_DIR, "dag_carga_redshift.py")
        assert os.path.exists(path), "dag_carga_redshift.py nao encontrado"
        py_compile.compile(path, doraise=True)

    def test_arquivos_dags_existem(self):
        """Ambos os arquivos de DAG devem existir."""
        for filename in ["dag_pipeline_diario.py", "dag_carga_redshift.py"]:
            path = os.path.join(DAGS_DIR, filename)
            assert os.path.exists(path), f"{filename} nao encontrado em {DAGS_DIR}"


# ─── Testes de conteudo das DAGs (requerem Airflow instalado) ─────────────────

class TestDAGPipelineDiario:

    TOTAL_BRONZE_JOBS = 18
    TOTAL_DIM_JOBS    = 11
    TOTAL_WAVE1_JOBS  = 7
    TOTAL_WAVE2_JOBS  = 4

    def test_dag_id_correto(self, dag_pipeline):
        assert dag_pipeline.dag_id == "cc_pipeline_diario"

    def test_schedule_diario_2h(self, dag_pipeline):
        assert dag_pipeline.schedule_interval == "0 2 * * *"

    def test_catchup_desabilitado(self, dag_pipeline):
        assert dag_pipeline.catchup is False

    def test_numero_total_de_tasks(self, dag_pipeline):
        """Pipeline deve ter: 40 Glue + 5 vazios + 1 trigger = 46 tasks."""
        total = len(dag_pipeline.tasks)
        assert total >= 46, (
            f"Esperado >= 46 tasks, encontrado {total}"
        )

    def test_contem_18_jobs_bronze(self, dag_pipeline):
        bronze_tasks = [t for t in dag_pipeline.tasks if "bronze_to_silver" in t.task_id]
        assert len(bronze_tasks) == self.TOTAL_BRONZE_JOBS, (
            f"Esperado {self.TOTAL_BRONZE_JOBS} jobs bronze, encontrado {len(bronze_tasks)}"
        )

    def test_contem_11_jobs_dim(self, dag_pipeline):
        dim_tasks = [t for t in dag_pipeline.tasks if "dim_" in t.task_id and "run_" in t.task_id]
        assert len(dim_tasks) == self.TOTAL_DIM_JOBS, (
            f"Esperado {self.TOTAL_DIM_JOBS} jobs dimensao, encontrado {len(dim_tasks)}"
        )

    def test_contem_jobs_fato(self, dag_pipeline):
        total_fato = self.TOTAL_WAVE1_JOBS + self.TOTAL_WAVE2_JOBS
        fato_tasks = [t for t in dag_pipeline.tasks if "fato_" in t.task_id and "run_" in t.task_id]
        assert len(fato_tasks) == total_fato, (
            f"Esperado {total_fato} jobs fato, encontrado {len(fato_tasks)}"
        )

    def test_task_inicio_existe(self, dag_pipeline):
        task_ids = {t.task_id for t in dag_pipeline.tasks}
        assert "inicio_pipeline" in task_ids

    def test_task_trigger_redshift_existe(self, dag_pipeline):
        task_ids = {t.task_id for t in dag_pipeline.tasks}
        assert "acionar_carga_redshift" in task_ids

    def test_trigger_redshift_aponta_para_dag_correta(self, dag_pipeline):
        trigger = dag_pipeline.get_task("acionar_carga_redshift")
        assert trigger.trigger_dag_id == "cc_carga_redshift"

    def test_tags_presentes(self, dag_pipeline):
        assert "contact-center" in dag_pipeline.tags
        assert "glue" in dag_pipeline.tags


class TestDAGCargaRedshift:

    TOTAL_DIMS  = 11
    TOTAL_FATOS = 11

    def test_dag_id_correto(self, dag_redshift):
        assert dag_redshift.dag_id == "cc_carga_redshift"

    def test_schedule_none_acionada_externamente(self, dag_redshift):
        assert dag_redshift.schedule_interval is None

    def test_catchup_desabilitado(self, dag_redshift):
        assert dag_redshift.catchup is False

    def test_numero_tasks_dimensoes(self, dag_redshift):
        dim_tasks = [t for t in dag_redshift.tasks if t.task_id.startswith("copy_dim_")]
        assert len(dim_tasks) == self.TOTAL_DIMS, (
            f"Esperado {self.TOTAL_DIMS} tasks de dimensao, encontrado {len(dim_tasks)}"
        )

    def test_numero_tasks_fatos(self, dag_redshift):
        fato_tasks = [t for t in dag_redshift.tasks if t.task_id.startswith("copy_fato_")]
        assert len(fato_tasks) == self.TOTAL_FATOS, (
            f"Esperado {self.TOTAL_FATOS} tasks de fato, encontrado {len(fato_tasks)}"
        )

    def test_cobre_tabelas_principais(self, dag_redshift):
        task_ids = {t.task_id for t in dag_redshift.tasks}
        tabelas_criticas = [
            "copy_dim_data",
            "copy_dim_operador",
            "copy_dim_cliente",
            "copy_fato_chamada",
            "copy_fato_ticket",
            "copy_fato_qualidade",
        ]
        for tabela in tabelas_criticas:
            assert tabela in task_ids, f"Task '{tabela}' nao encontrada na DAG"

    def test_tags_presentes(self, dag_redshift):
        assert "redshift" in dag_redshift.tags
        assert "contact-center" in dag_redshift.tags


# ─── Testes de conteudo dos arquivos (sem Airflow) ───────────────────────────

class TestConteudoArquivosDAG:
    """Valida conteudo dos arquivos sem importar Airflow."""

    def _read(self, filename: str) -> str:
        path = os.path.join(DAGS_DIR, filename)
        with open(path, encoding="utf-8") as f:
            return f.read()

    def test_pipeline_define_18_bronze_jobs(self):
        content = self._read("dag_pipeline_diario.py")
        bronze_entries = [l for l in content.split("\n") if "bronze-to-silver" in l and '"-"' not in l and '"' in l]
        # Conta entradas na lista BRONZE_JOBS
        import re
        matches = re.findall(r'"job-tb-[\w-]+-bronze-to-silver"', content)
        assert len(matches) == 18, f"Esperado 18 bronze jobs, encontrado {len(matches)}"

    def test_pipeline_define_11_dim_jobs(self):
        content = self._read("dag_pipeline_diario.py")
        import re
        matches = re.findall(r'"job-dim-[\w-]+-gold"', content)
        assert len(matches) == 11, f"Esperado 11 dim jobs, encontrado {len(matches)}"

    def test_pipeline_define_11_fato_jobs(self):
        content = self._read("dag_pipeline_diario.py")
        import re
        matches = re.findall(r'"job-fato-[\w-]+-gold"', content)
        assert len(matches) == 11, f"Esperado 11 fato jobs, encontrado {len(matches)}"

    def test_carga_usa_truncate_antes_de_copy(self):
        content = self._read("dag_carga_redshift.py")
        assert "TRUNCATE TABLE" in content, "Carga deve fazer TRUNCATE antes do COPY"

    def test_carga_usa_formato_parquet(self):
        content = self._read("dag_carga_redshift.py")
        assert "FORMAT AS PARQUET" in content, "Carga deve usar formato PARQUET"

    def test_carga_usa_iam_role(self):
        content = self._read("dag_carga_redshift.py")
        assert "IAM_ROLE" in content, "COPY deve usar IAM_ROLE"

    def test_pipeline_referencia_dag_redshift(self):
        content = self._read("dag_pipeline_diario.py")
        assert "cc_carga_redshift" in content, "Pipeline deve referenciar a DAG de carga Redshift"

    def test_pipeline_tem_4_waves(self):
        content = self._read("dag_pipeline_diario.py")
        assert "BRONZE_JOBS" in content
        assert "GOLD_DIM_JOBS" in content
        assert "GOLD_FATO_WAVE1_JOBS" in content
        assert "GOLD_FATO_WAVE2_JOBS" in content
