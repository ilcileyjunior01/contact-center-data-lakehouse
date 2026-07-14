"""
conftest.py
===========
Fixtures compartilhadas para os testes do Contact Center Data Lakehouse.

Cria uma SparkSession local reutilizada em toda a suite de testes.
Não requer awsglue — usa PySpark puro para testar a lógica de transformação.
"""

import os
import sys
import pytest
from pyspark.sql import SparkSession

# Garante que o PySpark workers usem o mesmo Python do ambiente atual.
# Necessário no Windows onde 'python' pode apontar para o Microsoft Store stub.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    """
    SparkSession local para testes unitários.

    - scope="session": criada uma única vez por execução de pytest
    - master="local[2]": 2 threads locais (rápido, sem cluster)
    - Configurações mínimas para testes unitários
    """
    session = (
        SparkSession.builder
        .appName("cc-lakehouse-unit-tests")
        .master("local[1]")   # thread única — evita fork de workers no Windows
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.driver.memory", "512m")
        .config("spark.python.worker.reuse", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
