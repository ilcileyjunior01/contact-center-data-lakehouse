"""
test_transformations.py
=======================
Testa a lógica de transformação dos jobs PySpark usando pandas.

Usa pandas (não PySpark) para testar as regras de negócio em isolamento.
PySpark em si já é validado pelo Spark/Glue CI na AWS — aqui testamos
se as REGRAS estão corretas: thresholds, flags, mascaramentos, faixas.

Cobertura:
  - fl_duracao_valida   (duração > 0 e dt_fim > dt_inicio)
  - fl_chamada_completa (dt_inicio e dt_fim preenchidos)
  - nr_duracao_minutos  (conversão de segundos para minutos)
  - hash_registro       (MD5 detecta mudanças de conteúdo)
  - quarentena          (registros inválidos separados dos válidos)
  - mascaramento PII    (CPF, e-mail, telefone — conformidade LGPD)
  - fl_sla_cumprido     (tickets dentro do SLA de 480 min)
  - ds_faixa_nota       (faixa de qualidade por nota numérica)
  - fl_aprovado         (nota >= 7)
"""

import hashlib
from datetime import datetime, timedelta

import pandas as pd
import pytest


# ─── Helpers de transformação (espelham a lógica dos jobs PySpark) ────────────

def calc_fl_duracao_valida(nr_duracao_segundos, dt_inicio, dt_fim) -> int:
    """fl_duracao_valida = 1 quando duração > 0 E dt_fim > dt_inicio."""
    if nr_duracao_segundos > 0 and dt_fim is not None and dt_inicio is not None:
        return 1 if dt_fim > dt_inicio else 0
    return 0


def calc_fl_chamada_completa(dt_inicio, dt_fim) -> int:
    """fl_chamada_completa = 1 quando ambas as datas estão preenchidas."""
    return 1 if dt_inicio is not None and dt_fim is not None else 0


def calc_nr_duracao_minutos(nr_duracao_segundos: int) -> float:
    """nr_duracao_minutos = round(nr_duracao_segundos / 60.0, 2)."""
    return round(nr_duracao_segundos / 60.0, 2)


def calc_hash_registro(*campos) -> str:
    """hash_registro = MD5(concat_ws('|', campos)) — detecta mudanças."""
    partes = [str(c) if c is not None else "" for c in campos]
    concat = "|".join(partes)
    return hashlib.md5(concat.encode()).hexdigest()


def calc_motivo_quarentena(id_chamada, id_cliente, dt_inicio, nr_duracao_segundos):
    """Retorna o motivo de quarentena ou None se o registro for válido."""
    if id_chamada is None:
        return "id_chamada_nulo"
    if id_cliente is None:
        return "id_cliente_nulo"
    if dt_inicio is None:
        return "dt_inicio_nulo"
    if nr_duracao_segundos < 0:
        return "duracao_negativa"
    return None


def mascarar_cpf(cpf: str) -> str:
    """3 primeiros + ***** + 2 últimos dígitos."""
    return f"{cpf[:3]}*****{cpf[-2:]}"


def mascarar_email(email: str) -> str:
    """Mantém apenas o domínio: ***@dominio.com."""
    dominio = email.split("@", 1)[1]
    return f"***@{dominio}"


def mascarar_telefone(telefone: str) -> str:
    """6 asteriscos + 4 últimos dígitos."""
    return f"******{telefone[-4:]}"


def calc_fl_sla_cumprido(nr_tempo_resolucao_min: float, sla_min: int = 480) -> int:
    """fl_sla_cumprido = 1 quando tempo de resolução <= SLA (480 min = 8h)."""
    return 1 if nr_tempo_resolucao_min <= sla_min else 0


def calc_ds_faixa_nota(nr_nota: float) -> str:
    """Classifica a nota em 5 faixas de qualidade."""
    if nr_nota >= 9:
        return "EXCELENTE"
    elif nr_nota >= 7:
        return "BOM"
    elif nr_nota >= 5:
        return "REGULAR"
    elif nr_nota >= 3:
        return "RUIM"
    else:
        return "CRITICO"


def calc_fl_aprovado(nr_nota: float) -> int:
    """fl_aprovado = 1 quando nota >= 7."""
    return 1 if nr_nota >= 7 else 0


# ─── fl_duracao_valida ────────────────────────────────────────────────────────

class TestDuracaoValida:

    def test_valida_quando_duracao_positiva_e_datas_corretas(self):
        now = datetime(2025, 3, 15, 10, 0, 0)
        assert calc_fl_duracao_valida(300, now, now + timedelta(minutes=5)) == 1

    def test_invalida_quando_duracao_zero(self):
        now = datetime(2025, 3, 15, 10, 0, 0)
        assert calc_fl_duracao_valida(0, now, now + timedelta(minutes=5)) == 0

    def test_invalida_quando_dt_fim_menor_que_dt_inicio(self):
        now = datetime(2025, 3, 15, 10, 0, 0)
        assert calc_fl_duracao_valida(300, now, now - timedelta(minutes=1)) == 0

    def test_invalida_quando_dt_fim_nulo(self):
        now = datetime(2025, 3, 15, 10, 0, 0)
        assert calc_fl_duracao_valida(300, now, None) == 0

    def test_invalida_quando_duracao_negativa(self):
        now = datetime(2025, 3, 15, 10, 0, 0)
        assert calc_fl_duracao_valida(-10, now, now + timedelta(minutes=5)) == 0


# ─── fl_chamada_completa ──────────────────────────────────────────────────────

class TestChamadaCompleta:

    def test_completa_quando_ambas_datas_preenchidas(self):
        now = datetime(2025, 3, 15, 10, 0, 0)
        assert calc_fl_chamada_completa(now, now + timedelta(minutes=3)) == 1

    def test_incompleta_quando_dt_fim_nulo(self):
        now = datetime(2025, 3, 15, 10, 0, 0)
        assert calc_fl_chamada_completa(now, None) == 0

    def test_incompleta_quando_dt_inicio_nulo(self):
        now = datetime(2025, 3, 15, 10, 0, 0)
        assert calc_fl_chamada_completa(None, now) == 0

    def test_incompleta_quando_ambas_nulas(self):
        assert calc_fl_chamada_completa(None, None) == 0


# ─── nr_duracao_minutos ───────────────────────────────────────────────────────

class TestDuracaoMinutos:

    @pytest.mark.parametrize("segundos,esperado", [
        (60,  1.0),
        (90,  1.5),
        (300, 5.0),
        (0,   0.0),
        (61,  1.02),
        (3600, 60.0),
    ])
    def test_conversao_segundos_para_minutos(self, segundos, esperado):
        result = calc_nr_duracao_minutos(segundos)
        assert abs(result - esperado) < 0.01, f"Esperado {esperado}, obtido {result}"


# ─── hash_registro ───────────────────────────────────────────────────────────

class TestHashRegistro:

    def test_hash_identico_para_mesmo_conteudo(self):
        h1 = calc_hash_registro(1, 100, 10, 300, "ATENDIDA")
        h2 = calc_hash_registro(1, 100, 10, 300, "ATENDIDA")
        assert h1 == h2

    def test_hash_diferente_quando_status_muda(self):
        h_orig = calc_hash_registro(1, "ATENDIDA")
        h_upd  = calc_hash_registro(1, "TRANSFERIDA")
        assert h_orig != h_upd, "Hash igual após mudança de status — MERGE não detectaria"

    def test_hash_diferente_quando_duracao_muda(self):
        h1 = calc_hash_registro(1, 100, 300)
        h2 = calc_hash_registro(1, 100, 310)
        assert h1 != h2

    def test_hash_e_string_hexadecimal_32_chars(self):
        h = calc_hash_registro(1, "ATENDIDA", 300)
        assert len(h) == 32
        assert all(c in "0123456789abcdef" for c in h)

    def test_campo_nulo_nao_quebra_hash(self):
        """None deve ser tratado como string vazia no concat_ws."""
        h1 = calc_hash_registro(1, None, "ATENDIDA")
        h2 = calc_hash_registro(1, None, "ATENDIDA")
        assert h1 == h2  # determinístico mesmo com None


# ─── Quarentena ───────────────────────────────────────────────────────────────

class TestQuarentena:

    def test_registro_valido_nao_vai_para_quarentena(self):
        motivo = calc_motivo_quarentena(1, 100, datetime(2025, 3, 15), 300)
        assert motivo is None

    @pytest.mark.parametrize("id_chamada,id_cliente,dt_inicio,duracao,motivo_esperado", [
        (None, 100,  datetime(2025, 3, 15),  300, "id_chamada_nulo"),
        (1,    None, datetime(2025, 3, 15),  300, "id_cliente_nulo"),
        (1,    100,  None,                   300, "dt_inicio_nulo"),
        (1,    100,  datetime(2025, 3, 15), -10,  "duracao_negativa"),
    ])
    def test_registros_invalidos_tem_motivo_correto(
        self, id_chamada, id_cliente, dt_inicio, duracao, motivo_esperado
    ):
        motivo = calc_motivo_quarentena(id_chamada, id_cliente, dt_inicio, duracao)
        assert motivo == motivo_esperado, (
            f"Esperado '{motivo_esperado}', obtido '{motivo}'"
        )

    def test_prioridade_de_validacao_id_chamada_primeiro(self):
        """id_chamada nulo tem prioridade sobre outros campos nulos."""
        motivo = calc_motivo_quarentena(None, None, None, -10)
        assert motivo == "id_chamada_nulo"


# ─── Mascaramento PII ─────────────────────────────────────────────────────────

class TestMascaramentoPII:

    def test_mascaramento_cpf_formato_correto(self):
        result = mascarar_cpf("12345678901")
        assert result == "123*****01"
        assert len(result) == 10
        assert "*****" in result

    def test_mascaramento_cpf_nao_expoe_digitos_do_meio(self):
        result = mascarar_cpf("12345678901")
        assert "456789" not in result
        assert "4567" not in result

    def test_mascaramento_email_mantem_dominio(self):
        result = mascarar_email("operador@empresa.com.br")
        assert result == "***@empresa.com.br"
        assert result.startswith("***@")
        assert "operador" not in result

    def test_mascaramento_email_dominio_simples(self):
        result = mascarar_email("joao@gmail.com")
        assert result == "***@gmail.com"

    def test_mascaramento_telefone_formato_correto(self):
        result = mascarar_telefone("11987654321")
        assert result == "******4321"
        assert result.startswith("******")
        assert len(result) == 10

    def test_mascaramento_telefone_nao_expoe_parte_intermediaria(self):
        result = mascarar_telefone("11987654321")
        assert "9876" not in result
        assert "87654" not in result

    @pytest.mark.parametrize("cpf,esperado", [
        ("12345678901", "123*****01"),
        ("98765432100", "987*****00"),
        ("00011122233", "000*****33"),
    ])
    def test_mascaramento_cpf_varios_exemplos(self, cpf, esperado):
        assert mascarar_cpf(cpf) == esperado


# ─── SLA de tickets ───────────────────────────────────────────────────────────

class TestSLATicket:

    @pytest.mark.parametrize("tempo_min,esperado", [
        (240,  1),   # 4h — dentro do SLA
        (480,  1),   # 8h — exatamente no limite (inclusivo)
        (481,  0),   # 8h 1min — fora do SLA
        (1440, 0),   # 24h — muito fora do SLA
        (0,    1),   # resolvido imediatamente
        (479,  1),   # 1 min antes do limite
    ])
    def test_fl_sla_cumprido(self, tempo_min, esperado):
        result = calc_fl_sla_cumprido(float(tempo_min))
        assert result == esperado, (
            f"tempo={tempo_min}min → esperado {esperado}, obtido {result}"
        )

    def test_sla_limite_e_inclusivo(self):
        """SLA de 480 min deve ser cumprido quando tempo == 480."""
        assert calc_fl_sla_cumprido(480.0) == 1
        assert calc_fl_sla_cumprido(480.1) == 0


# ─── Faixa de nota ───────────────────────────────────────────────────────────

class TestFaixaNota:

    @pytest.mark.parametrize("nota,faixa_esperada", [
        (10.0, "EXCELENTE"),
        (9.0,  "EXCELENTE"),
        (8.9,  "BOM"),
        (8.0,  "BOM"),
        (7.0,  "BOM"),
        (6.9,  "REGULAR"),
        (6.0,  "REGULAR"),
        (5.0,  "REGULAR"),
        (4.9,  "RUIM"),
        (4.0,  "RUIM"),
        (3.0,  "RUIM"),
        (2.9,  "CRITICO"),
        (2.0,  "CRITICO"),
        (1.0,  "CRITICO"),
    ])
    def test_classificacao_faixa_nota(self, nota, faixa_esperada):
        result = calc_ds_faixa_nota(nota)
        assert result == faixa_esperada, (
            f"nota={nota} → esperado '{faixa_esperada}', obtido '{result}'"
        )

    @pytest.mark.parametrize("nota,esperado", [
        (10.0, 1), (7.0, 1), (7.1, 1),
        (6.9,  0), (1.0, 0), (0.0, 0),
    ])
    def test_fl_aprovado(self, nota, esperado):
        result = calc_fl_aprovado(nota)
        assert result == esperado, (
            f"nota={nota} → esperado fl_aprovado={esperado}, obtido={result}"
        )

    def test_fronteira_excelente_bom(self):
        assert calc_ds_faixa_nota(9.0) == "EXCELENTE"
        assert calc_ds_faixa_nota(8.99) == "BOM"

    def test_fronteira_bom_regular(self):
        assert calc_ds_faixa_nota(7.0) == "BOM"
        assert calc_ds_faixa_nota(6.99) == "REGULAR"

    def test_fronteira_regular_ruim(self):
        assert calc_ds_faixa_nota(5.0) == "REGULAR"
        assert calc_ds_faixa_nota(4.99) == "RUIM"

    def test_fronteira_ruim_critico(self):
        assert calc_ds_faixa_nota(3.0) == "RUIM"
        assert calc_ds_faixa_nota(2.99) == "CRITICO"
