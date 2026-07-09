"""
generate_data.py
================
Gerador de dados sintéticos para o Contact Center Data Lakehouse.

Gera dados para as 18 tabelas do domínio de Contact Center com consistência
relacional (chaves estrangeiras válidas entre tabelas), simulação de CDC
(campos _timestamp e Op) e persistência em CSV.

Uso
---
    python generate_data.py
    python generate_data.py --rows 1000
    python generate_data.py --rows 2000 --output-dir /tmp/output

Dependências
------------
    pip install faker pandas numpy

Autor: Ilciley Junior
Data : 2026-07-09
"""

from __future__ import annotations

import argparse
import os
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from faker import Faker

# ---------------------------------------------------------------------------
# Configuração global
# ---------------------------------------------------------------------------

faker = Faker("pt_BR")
Faker.seed(42)
random.seed(42)
np.random.seed(42)

# Constantes de domínio
ESTADOS_BR = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO",
    "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR",
    "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
]

CARGOS = ["Agente", "Supervisor", "Coordenador"]
SKILLS = ["Vendas", "Suporte", "Cobrança", "Retenção", "SAC"]
TIPOS_FILA = ["ENTRADA", "SAIDA", "CHAT", "WHATSAPP"]
STATUS_CHAMADA = ["ATENDIDA", "ABANDONADA", "TRANSFERIDA", "ENGAJADA"]
TIPOS_CHAMADA = ["ENTRADA", "SAIDA"]
CATEGORIAS_TICKET = ["Reclamação", "Dúvida", "Solicitação", "Elogio"]
PRIORIDADES_TICKET = ["BAIXA", "MEDIA", "ALTA", "URGENTE"]
STATUS_TICKET = ["ABERTO", "EM_ANDAMENTO", "RESOLVIDO", "FECHADO"]
TIPOS_INTERACAO = ["COMENTARIO", "ESCALONAMENTO", "RESOLUCAO"]
STATUS_CHAT = ["ENCERRADO", "ABANDONADO", "TRANSFERIDO"]
STATUS_ATENDIMENTO_WA = ["RESOLVIDO", "PENDENTE", "TRANSFERIDO"]
STATUS_DISCAGEM = ["PENDENTE", "CONTACTADO", "SEM_RESPOSTA", "NUMERO_INVALIDO"]
OBJETIVOS_CAMPANHA = ["Vendas", "Cobrança", "Retenção", "Pesquisa"]
STATUS_PRESENCA = ["PRESENTE", "AUSENTE", "FALTA"]
OPCOES_URA = [
    "1 - Atendimento Financeiro",
    "2 - Suporte Técnico",
    "3 - Cancelamento",
    "4 - Falar com Operador",
    "5 - Reclamações",
    "9 - Menu Anterior",
    "0 - Encerrar",
]

# Operações CDC
CDC_OPS = ["I", "I", "I", "I", "U", "D"]  # Pesos: ~67% INSERT, ~17% UPDATE, ~17% DELETE


# ---------------------------------------------------------------------------
# Funções utilitárias
# ---------------------------------------------------------------------------

def agora_aleatorio(inicio: datetime, fim: datetime) -> datetime:
    """Retorna um datetime aleatório entre inicio e fim."""
    delta = fim - inicio
    segundos = random.randint(0, int(delta.total_seconds()))
    return inicio + timedelta(seconds=segundos)


def gerar_timestamp_cdc(base: datetime | None = None) -> str:
    """
    Gera um timestamp ISO 8601 para o campo _timestamp do CDC.
    Se base for fornecido, adiciona um pequeno jitter em segundos.
    """
    if base is None:
        base = agora_aleatorio(datetime(2023, 1, 1), datetime(2025, 12, 31))
    jitter = timedelta(seconds=random.randint(0, 3600))
    return (base + jitter).strftime("%Y-%m-%dT%H:%M:%S")


def gerar_op() -> str:
    """Retorna uma operação CDC aleatória ponderada."""
    return random.choice(CDC_OPS)


def gerar_cpf() -> str:
    """Gera um CPF formatado (apenas dígitos, sem validação de dígitos verificadores)."""
    return f"{random.randint(100, 999)}.{random.randint(100, 999)}.{random.randint(100, 999)}-{random.randint(10, 99)}"


def gerar_telefone() -> str:
    """Gera número de telefone brasileiro no formato (XX) 9XXXX-XXXX."""
    ddd = random.choice([
        "11", "12", "13", "14", "15", "16", "17", "18", "19",
        "21", "22", "24", "27", "28", "31", "32", "33", "34",
        "35", "37", "38", "41", "42", "43", "44", "45", "46",
        "47", "48", "49", "51", "53", "54", "55", "61", "62",
        "63", "64", "65", "66", "67", "68", "69", "71", "73",
        "74", "75", "77", "79", "81", "82", "83", "84", "85",
        "86", "87", "88", "89", "91", "92", "93", "94", "95",
        "96", "97", "98", "99",
    ])
    return f"({ddd}) 9{random.randint(1000, 9999)}-{random.randint(1000, 9999)}"


def gerar_cep() -> str:
    """Gera CEP no formato XXXXX-XXX."""
    return f"{random.randint(10000, 99999)}-{random.randint(100, 999)}"


def salvar_csv(df: pd.DataFrame, nome_tabela: str, output_dir: Path) -> None:
    """Salva um DataFrame como arquivo CSV no diretório de saída."""
    caminho = output_dir / f"{nome_tabela}.csv"
    df.to_csv(caminho, index=False, encoding="utf-8-sig")
    print(f"  [OK] {nome_tabela:45s} -> {len(df):>6,} registros -> {caminho.name}")


# ---------------------------------------------------------------------------
# Geradores de tabelas
# ---------------------------------------------------------------------------

def gerar_tb_cliente(n: int) -> pd.DataFrame:
    """
    Gera dados para tb_cliente.

    Campos: id_cliente, nm_cliente, nr_documento, ds_email,
            nr_telefone, dt_cadastro, st_cliente, _timestamp, Op
    """
    registros = []
    for i in range(1, n + 1):
        dt_cadastro = agora_aleatorio(datetime(2015, 1, 1), datetime(2025, 6, 30))
        registros.append({
            "id_cliente":    i,
            "nm_cliente":    faker.name(),
            "nr_documento":  gerar_cpf(),
            "ds_email":      faker.email(),
            "nr_telefone":   gerar_telefone(),
            "dt_cadastro":   dt_cadastro.strftime("%Y-%m-%d"),
            "st_cliente":    random.choice(["A", "A", "A", "I"]),  # 75% ativos
            "_timestamp":    gerar_timestamp_cdc(dt_cadastro),
            "Op":            gerar_op(),
        })
    return pd.DataFrame(registros)


def gerar_tb_endereco_cliente(ids_cliente: list[int]) -> pd.DataFrame:
    """
    Gera dados para tb_endereco_cliente.

    Cada cliente recebe entre 1 e 2 endereços.
    """
    registros = []
    id_endereco = 1
    for id_cliente in ids_cliente:
        qtd = random.choices([1, 2], weights=[0.75, 0.25])[0]
        for _ in range(qtd):
            registros.append({
                "id_endereco":  id_endereco,
                "id_cliente":   id_cliente,
                "ds_logradouro": faker.street_address(),
                "ds_bairro":    faker.bairro(),
                "ds_cidade":    faker.city(),
                "ds_estado":    random.choice(ESTADOS_BR),
                "nr_cep":       gerar_cep(),
                "_timestamp":   gerar_timestamp_cdc(),
                "Op":           gerar_op(),
            })
            id_endereco += 1
    return pd.DataFrame(registros)


def gerar_tb_operador(n: int) -> pd.DataFrame:
    """
    Gera dados para tb_operador.

    Distribuição de cargos: 70% Agente, 20% Supervisor, 10% Coordenador.
    """
    registros = []
    pesos_cargo = ["Agente"] * 70 + ["Supervisor"] * 20 + ["Coordenador"] * 10
    for i in range(1, n + 1):
        cargo = random.choice(pesos_cargo)
        dt_admissao = agora_aleatorio(datetime(2010, 1, 1), datetime(2024, 12, 31))
        registros.append({
            "id_operador":  i,
            "nm_operador":  faker.name(),
            "ds_cargo":     cargo,
            "ds_email":     faker.company_email(),
            "dt_admissao":  dt_admissao.strftime("%Y-%m-%d"),
            "st_operador":  random.choice(["A", "A", "A", "I"]),
            "fl_supervisor": 1 if cargo in ("Supervisor", "Coordenador") else 0,
            "_timestamp":   gerar_timestamp_cdc(dt_admissao),
            "Op":           gerar_op(),
        })
    return pd.DataFrame(registros)


def gerar_tb_skill_operador(ids_operador: list[int]) -> pd.DataFrame:
    """
    Gera dados para tb_skill_operador.

    Cada operador recebe entre 1 e 3 skills distintas.
    """
    registros = []
    id_skill = 1
    for id_operador in ids_operador:
        skills_escolhidas = random.sample(SKILLS, k=random.randint(1, 3))
        for skill in skills_escolhidas:
            dt_cert = agora_aleatorio(datetime(2018, 1, 1), datetime(2025, 6, 30))
            registros.append({
                "id_skill":        id_skill,
                "id_operador":     id_operador,
                "ds_skill":        skill,
                "nr_nivel":        random.randint(1, 5),
                "dt_certificacao": dt_cert.strftime("%Y-%m-%d"),
                "_timestamp":      gerar_timestamp_cdc(dt_cert),
                "Op":              gerar_op(),
            })
            id_skill += 1
    return pd.DataFrame(registros)


def gerar_tb_fila_atendimento(n: int = 20) -> pd.DataFrame:
    """
    Gera dados para tb_fila_atendimento.

    Gera um conjunto fixo de filas representativas.
    """
    nomes_filas = [
        "Fila Vendas SP", "Fila Suporte Técnico", "Fila Cobrança",
        "Fila Retenção", "Fila SAC Geral", "Fila Chat Vendas",
        "Fila WhatsApp Suporte", "Fila VIP", "Fila Reclamações",
        "Fila Cancelamento", "Fila Renegociação", "Fila Pesquisa",
        "Fila Saída Cobrança", "Fila Saída Vendas", "Fila Chat SAC",
        "Fila WhatsApp Vendas", "Fila Elogios", "Fila Transferência",
        "Fila Suporte N2", "Fila Emergencial",
    ]
    registros = []
    for i in range(1, min(n, len(nomes_filas)) + 1):
        registros.append({
            "id_fila":          i,
            "nm_fila":          nomes_filas[i - 1],
            "ds_tipo_fila":     random.choice(TIPOS_FILA),
            "nr_capacidade_max": random.choice([10, 15, 20, 25, 30, 50]),
            "fl_ativa":         random.choice([1, 1, 1, 0]),  # 75% ativas
            "_timestamp":       gerar_timestamp_cdc(),
            "Op":               gerar_op(),
        })
    return pd.DataFrame(registros)


def gerar_tb_chamada(
    n: int,
    ids_cliente: list[int],
    ids_operador: list[int],
    ids_fila: list[int],
) -> pd.DataFrame:
    """
    Gera dados para tb_chamada.

    Chamadas possuem FK válidas para cliente, operador e fila.
    A duração é calculada a partir de dt_inicio e dt_fim.
    """
    registros = []
    for i in range(1, n + 1):
        dt_inicio = agora_aleatorio(datetime(2023, 1, 1), datetime(2025, 12, 31))
        # Duração entre 30 segundos e 45 minutos
        duracao_seg = random.randint(30, 2700)
        dt_fim = dt_inicio + timedelta(seconds=duracao_seg)
        status = random.choice(STATUS_CHAMADA)
        # Chamadas abandonadas têm duração menor
        if status == "ABANDONADA":
            duracao_seg = random.randint(5, 120)
            dt_fim = dt_inicio + timedelta(seconds=duracao_seg)

        registros.append({
            "id_chamada":          i,
            "id_cliente":          random.choice(ids_cliente),
            "id_operador":         random.choice(ids_operador),
            "id_fila":             random.choice(ids_fila),
            "nr_telefone_origem":  gerar_telefone(),
            "nr_telefone_destino": gerar_telefone(),
            "dt_inicio":           dt_inicio.strftime("%Y-%m-%d %H:%M:%S"),
            "dt_fim":              dt_fim.strftime("%Y-%m-%d %H:%M:%S"),
            "nr_duracao_segundos": duracao_seg,
            "st_chamada":          status,
            "tp_chamada":          random.choice(TIPOS_CHAMADA),
            "_timestamp":          gerar_timestamp_cdc(dt_inicio),
            "Op":                  gerar_op(),
        })
    return pd.DataFrame(registros)


def gerar_tb_gravacao_chamada(ids_chamada: list[int]) -> pd.DataFrame:
    """
    Gera dados para tb_gravacao_chamada.

    Nem toda chamada possui gravação (aprox. 80% possuem).
    """
    registros = []
    ids_com_gravacao = random.sample(ids_chamada, k=int(len(ids_chamada) * 0.80))
    for i, id_chamada in enumerate(ids_com_gravacao, start=1):
        dt_gravacao = agora_aleatorio(datetime(2023, 1, 1), datetime(2025, 12, 31))
        registros.append({
            "id_gravacao":    i,
            "id_chamada":     id_chamada,
            "ds_url_gravacao": (
                f"s3://contact-center-recordings/audio/"
                f"{dt_gravacao.strftime('%Y/%m/%d')}/rec_{id_chamada:08d}.mp3"
            ),
            "nr_tamanho_mb":  round(random.uniform(0.5, 25.0), 2),
            "fl_processada":  random.choice([0, 1, 1, 1]),  # 75% processadas
            "dt_gravacao":    dt_gravacao.strftime("%Y-%m-%d %H:%M:%S"),
            "_timestamp":     gerar_timestamp_cdc(dt_gravacao),
            "Op":             gerar_op(),
        })
    return pd.DataFrame(registros)


def gerar_tb_ticket(
    n: int,
    ids_cliente: list[int],
    ids_operador: list[int],
) -> pd.DataFrame:
    """
    Gera dados para tb_ticket.

    Tickets resolvidos/fechados possuem dt_resolucao e nr_tempo_resolucao_horas.
    """
    registros = []
    for i in range(1, n + 1):
        dt_abertura = agora_aleatorio(datetime(2023, 1, 1), datetime(2025, 12, 31))
        status = random.choice(STATUS_TICKET)
        dt_resolucao = None
        nr_tempo_horas = None
        if status in ("RESOLVIDO", "FECHADO"):
            horas = random.randint(1, 720)  # até 30 dias
            dt_resolucao = (dt_abertura + timedelta(hours=horas)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            nr_tempo_horas = horas

        registros.append({
            "id_ticket":                 i,
            "id_cliente":                random.choice(ids_cliente),
            "id_operador":               random.choice(ids_operador),
            "ds_titulo":                 faker.sentence(nb_words=6),
            "ds_categoria":              random.choice(CATEGORIAS_TICKET),
            "ds_prioridade":             random.choice(PRIORIDADES_TICKET),
            "st_ticket":                 status,
            "dt_abertura":               dt_abertura.strftime("%Y-%m-%d %H:%M:%S"),
            "dt_resolucao":              dt_resolucao,
            "nr_tempo_resolucao_horas":  nr_tempo_horas,
            "_timestamp":                gerar_timestamp_cdc(dt_abertura),
            "Op":                        gerar_op(),
        })
    return pd.DataFrame(registros)


def gerar_tb_interacao_ticket(
    ids_ticket: list[int],
    ids_operador: list[int],
) -> pd.DataFrame:
    """
    Gera dados para tb_interacao_ticket.

    Cada ticket recebe entre 1 e 5 interações.
    """
    registros = []
    id_interacao = 1
    for id_ticket in ids_ticket:
        qtd = random.randint(1, 5)
        for _ in range(qtd):
            dt_interacao = agora_aleatorio(datetime(2023, 1, 1), datetime(2025, 12, 31))
            registros.append({
                "id_interacao":      id_interacao,
                "id_ticket":         id_ticket,
                "id_operador":       random.choice(ids_operador),
                "ds_tipo_interacao": random.choice(TIPOS_INTERACAO),
                "ds_descricao":      faker.paragraph(nb_sentences=2),
                "dt_interacao":      dt_interacao.strftime("%Y-%m-%d %H:%M:%S"),
                "_timestamp":        gerar_timestamp_cdc(dt_interacao),
                "Op":                gerar_op(),
            })
            id_interacao += 1
    return pd.DataFrame(registros)


def gerar_tb_avaliacao_qualidade(
    ids_chamada: list[int],
    ids_operador: list[int],
    ids_supervisor: list[int],
) -> pd.DataFrame:
    """
    Gera dados para tb_avaliacao_qualidade.

    Apenas ~60% das chamadas possuem avaliação de qualidade.
    O avaliador (id_supervisor) é sempre um operador com fl_supervisor=1.
    """
    registros = []
    ids_avaliados = random.sample(ids_chamada, k=int(len(ids_chamada) * 0.60))
    for i, id_chamada in enumerate(ids_avaliados, start=1):
        dt_avaliacao = agora_aleatorio(datetime(2023, 6, 1), datetime(2025, 12, 31))
        registros.append({
            "id_avaliacao":       i,
            "id_chamada":         id_chamada,
            "id_operador":        random.choice(ids_operador),
            "id_supervisor":      random.choice(ids_supervisor),
            "nr_nota_geral":      random.randint(1, 10),
            "nr_nota_comunicacao": random.randint(1, 10),
            "nr_nota_resolucao":  random.randint(1, 10),
            "ds_feedback":        faker.sentence(nb_words=12),
            "dt_avaliacao":       dt_avaliacao.strftime("%Y-%m-%d %H:%M:%S"),
            "_timestamp":         gerar_timestamp_cdc(dt_avaliacao),
            "Op":                 gerar_op(),
        })
    return pd.DataFrame(registros)


def gerar_tb_jornada_operador(
    ids_operador: list[int],
    n_dias: int = 30,
) -> pd.DataFrame:
    """
    Gera dados para tb_jornada_operador.

    Gera registros para os últimos n_dias para cada operador.
    """
    registros = []
    id_jornada = 1
    hoje = datetime(2025, 12, 31)

    for id_operador in ids_operador:
        for d in range(n_dias):
            dt_jornada = hoje - timedelta(days=d)
            # Fim de semana: maior chance de ausência
            if dt_jornada.weekday() >= 5:
                if random.random() < 0.70:
                    continue  # não registra jornada em 70% dos fins de semana

            status = random.choices(
                STATUS_PRESENCA, weights=[0.85, 0.10, 0.05]
            )[0]
            hora_entrada = dt_jornada.replace(
                hour=random.choice([7, 8, 9]),
                minute=random.randint(0, 59),
                second=0,
            )
            horas_trabalhadas = 0.0
            hora_saida = None
            chamadas = 0
            tickets = 0

            if status == "PRESENTE":
                horas_trabalhadas = round(random.uniform(7.5, 9.0), 2)
                hora_saida = hora_entrada + timedelta(hours=horas_trabalhadas)
                chamadas = random.randint(20, 120)
                tickets = random.randint(0, 15)

            registros.append({
                "id_jornada":             id_jornada,
                "id_operador":            id_operador,
                "dt_jornada":             dt_jornada.strftime("%Y-%m-%d"),
                "dt_entrada":             hora_entrada.strftime("%Y-%m-%d %H:%M:%S"),
                "dt_saida":               hora_saida.strftime("%Y-%m-%d %H:%M:%S") if hora_saida else None,
                "nr_horas_trabalhadas":   horas_trabalhadas,
                "nr_chamadas_atendidas":  chamadas,
                "nr_tickets_resolvidos":  tickets,
                "st_presenca":            status,
                "_timestamp":             gerar_timestamp_cdc(hora_entrada),
                "Op":                     gerar_op(),
            })
            id_jornada += 1
    return pd.DataFrame(registros)


def gerar_tb_chat(
    n: int,
    ids_cliente: list[int],
    ids_operador: list[int],
    ids_fila: list[int],
) -> pd.DataFrame:
    """
    Gera dados para tb_chat.

    Chats têm duração em minutos e nota de satisfação de 1 a 5.
    """
    registros = []
    for i in range(1, n + 1):
        dt_inicio = agora_aleatorio(datetime(2023, 1, 1), datetime(2025, 12, 31))
        duracao_min = random.randint(1, 60)
        dt_fim = dt_inicio + timedelta(minutes=duracao_min)
        status = random.choice(STATUS_CHAT)
        satisfacao = None
        if status == "ENCERRADO":
            satisfacao = random.randint(1, 5)

        registros.append({
            "id_chat":           i,
            "id_cliente":        random.choice(ids_cliente),
            "id_operador":       random.choice(ids_operador),
            "id_fila":           random.choice(ids_fila),
            "dt_inicio":         dt_inicio.strftime("%Y-%m-%d %H:%M:%S"),
            "dt_fim":            dt_fim.strftime("%Y-%m-%d %H:%M:%S"),
            "nr_duracao_minutos": duracao_min,
            "st_chat":           status,
            "nr_satisfacao":     satisfacao,
            "_timestamp":        gerar_timestamp_cdc(dt_inicio),
            "Op":                gerar_op(),
        })
    return pd.DataFrame(registros)


def gerar_tb_mensagem_chat(ids_chat: list[int]) -> pd.DataFrame:
    """
    Gera dados para tb_mensagem_chat.

    Cada chat possui entre 3 e 20 mensagens alternadas entre CLIENTE e OPERADOR.
    """
    registros = []
    id_mensagem = 1
    for id_chat in ids_chat:
        qtd_msgs = random.randint(3, 20)
        dt_base = agora_aleatorio(datetime(2023, 1, 1), datetime(2025, 12, 30))
        for j in range(qtd_msgs):
            remetente = "CLIENTE" if j % 2 == 0 else "OPERADOR"
            dt_msg = dt_base + timedelta(seconds=j * random.randint(15, 120))
            registros.append({
                "id_mensagem": id_mensagem,
                "id_chat":     id_chat,
                "ds_remetente": remetente,
                "ds_conteudo": faker.sentence(nb_words=random.randint(4, 20)),
                "dt_mensagem": dt_msg.strftime("%Y-%m-%d %H:%M:%S"),
                "_timestamp":  gerar_timestamp_cdc(dt_msg),
                "Op":          gerar_op(),
            })
            id_mensagem += 1
    return pd.DataFrame(registros)


def gerar_tb_whatsapp_atendimento(
    n: int,
    ids_cliente: list[int],
    ids_operador: list[int],
) -> pd.DataFrame:
    """
    Gera dados para tb_whatsapp_atendimento.

    Atendimentos via WhatsApp com satisfação apenas quando resolvido.
    """
    registros = []
    for i in range(1, n + 1):
        dt_inicio = agora_aleatorio(datetime(2023, 1, 1), datetime(2025, 12, 31))
        duracao_min = random.randint(5, 120)
        dt_fim = dt_inicio + timedelta(minutes=duracao_min)
        status = random.choice(STATUS_ATENDIMENTO_WA)
        satisfacao = random.randint(1, 5) if status == "RESOLVIDO" else None

        registros.append({
            "id_whatsapp":         i,
            "id_cliente":          random.choice(ids_cliente),
            "id_operador":         random.choice(ids_operador),
            "nr_telefone_cliente": gerar_telefone(),
            "dt_inicio":           dt_inicio.strftime("%Y-%m-%d %H:%M:%S"),
            "dt_fim":              dt_fim.strftime("%Y-%m-%d %H:%M:%S"),
            "st_atendimento":      status,
            "nr_satisfacao":       satisfacao,
            "_timestamp":          gerar_timestamp_cdc(dt_inicio),
            "Op":                  gerar_op(),
        })
    return pd.DataFrame(registros)


def gerar_tb_ura_navegacao(
    n: int,
    ids_cliente: list[int],
) -> pd.DataFrame:
    """
    Gera dados para tb_ura_navegacao.

    Simula navegação de clientes pela URA (Unidade de Resposta Audível).
    """
    registros = []
    for i in range(1, n + 1):
        dt_navegacao = agora_aleatorio(datetime(2023, 1, 1), datetime(2025, 12, 31))
        fl_transferiu = random.choice([0, 0, 1, 1, 1])  # 60% transferem para humano
        registros.append({
            "id_ura":              i,
            "id_cliente":          random.choice(ids_cliente),
            "ds_opcao_selecionada": random.choice(OPCOES_URA),
            "nr_tentativas":       random.randint(1, 5),
            "fl_transferiu_humano": fl_transferiu,
            "dt_navegacao":        dt_navegacao.strftime("%Y-%m-%d %H:%M:%S"),
            "nr_duracao_segundos": random.randint(15, 300),
            "_timestamp":          gerar_timestamp_cdc(dt_navegacao),
            "Op":                  gerar_op(),
        })
    return pd.DataFrame(registros)


def gerar_tb_campanha(n: int = 30) -> pd.DataFrame:
    """
    Gera dados para tb_campanha.

    Campanhas de discagem ativa com metas e conversões.
    """
    registros = []
    for i in range(1, n + 1):
        dt_inicio = agora_aleatorio(datetime(2022, 1, 1), datetime(2025, 6, 30))
        dt_fim = dt_inicio + timedelta(days=random.randint(7, 90))
        meta = random.randint(500, 10000)
        realizados = random.randint(0, meta)
        conversoes = random.randint(0, realizados // 4 if realizados > 0 else 0)
        fl_ativa = 1 if dt_fim > datetime(2025, 1, 1) else 0

        objetivo = random.choice(OBJETIVOS_CAMPANHA)
        registros.append({
            "id_campanha":           i,
            "nm_campanha":           f"Campanha {objetivo} {dt_inicio.strftime('%Y')} #{i:03d}",
            "ds_objetivo":           objetivo,
            "dt_inicio":             dt_inicio.strftime("%Y-%m-%d"),
            "dt_fim":                dt_fim.strftime("%Y-%m-%d"),
            "nr_meta_contatos":      meta,
            "nr_contatos_realizados": realizados,
            "nr_conversoes":         conversoes,
            "fl_ativa":              fl_ativa,
            "_timestamp":            gerar_timestamp_cdc(dt_inicio),
            "Op":                    gerar_op(),
        })
    return pd.DataFrame(registros)


def gerar_tb_discagem(
    n: int,
    ids_campanha: list[int],
    ids_cliente: list[int],
    ids_operador: list[int],
) -> pd.DataFrame:
    """
    Gera dados para tb_discagem.

    Registros de tentativas de discagem ativa vinculadas a campanhas.
    """
    registros = []
    for i in range(1, n + 1):
        fl_contato = random.choice([0, 0, 1, 1, 1])  # 60% de contato realizado
        fl_convertido = 0
        if fl_contato:
            fl_convertido = random.choice([0, 0, 0, 1])  # 25% de conversão

        dt_ultima = agora_aleatorio(datetime(2023, 1, 1), datetime(2025, 12, 31))
        registros.append({
            "id_discagem":           i,
            "id_campanha":           random.choice(ids_campanha),
            "id_cliente":            random.choice(ids_cliente),
            "id_operador":           random.choice(ids_operador),
            "nr_tentativas":         random.randint(1, 6),
            "fl_contato_realizado":  fl_contato,
            "fl_convertido":         fl_convertido,
            "dt_ultima_tentativa":   dt_ultima.strftime("%Y-%m-%d %H:%M:%S"),
            "st_discagem":           random.choice(STATUS_DISCAGEM),
            "_timestamp":            gerar_timestamp_cdc(dt_ultima),
            "Op":                    gerar_op(),
        })
    return pd.DataFrame(registros)


def gerar_tb_metricas_operacionais(
    n: int,
    ids_operador: list[int],
    ids_fila: list[int],
) -> pd.DataFrame:
    """
    Gera dados para tb_metricas_operacionais.

    Métricas diárias por operador e fila. TMA = Tempo Médio de Atendimento,
    TME = Tempo Médio de Espera, NPS = Net Promoter Score.
    """
    registros = []
    for i in range(1, n + 1):
        dt_ref = agora_aleatorio(datetime(2023, 1, 1), datetime(2025, 12, 31))
        chamadas_atendidas = random.randint(10, 150)
        chamadas_abandonadas = random.randint(0, chamadas_atendidas // 5)
        total = chamadas_atendidas + chamadas_abandonadas
        taxa_abandono = round(chamadas_abandonadas / total * 100, 2) if total > 0 else 0.0

        registros.append({
            "id_metrica":              i,
            "id_operador":             random.choice(ids_operador),
            "id_fila":                 random.choice(ids_fila),
            "dt_referencia":           dt_ref.strftime("%Y-%m-%d"),
            "nr_chamadas_atendidas":   chamadas_atendidas,
            "nr_chamadas_abandonadas": chamadas_abandonadas,
            "nr_tma_segundos":         random.randint(60, 900),   # 1 a 15 min
            "nr_tme_segundos":         random.randint(5, 300),    # 5 seg a 5 min
            "nr_taxa_abandono":        taxa_abandono,
            "nr_nps":                  round(random.uniform(-100, 100), 1),
            "_timestamp":              gerar_timestamp_cdc(dt_ref),
            "Op":                      gerar_op(),
        })
    return pd.DataFrame(registros)


# ---------------------------------------------------------------------------
# Orquestrador principal
# ---------------------------------------------------------------------------

def gerar_todos_os_dados(n_rows: int, output_dir: Path) -> dict[str, int]:
    """
    Gera todos os dados sintéticos para as 18 tabelas e salva como CSV.

    Parameters
    ----------
    n_rows : int
        Número base de registros para as tabelas principais.
    output_dir : Path
        Diretório de saída para os arquivos CSV.

    Returns
    -------
    dict[str, int]
        Dicionário com o nome da tabela e a quantidade de registros gerados.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    contagem: dict[str, int] = {}

    print(f"\n{'=' * 65}")
    print(f"  Gerando dados sintéticos - Contact Center Data Lakehouse")
    print(f"  Registros base (n_rows): {n_rows:,}")
    print(f"  Diretório de saída     : {output_dir}")
    print(f"{'=' * 65}\n")

    # ------------------------------------------------------------------
    # 1. Tabelas de cadastro (independentes ou semi-independentes)
    # ------------------------------------------------------------------
    print("[1/6] Tabelas de Cadastro")

    # tb_cliente
    n_clientes = n_rows
    df_cliente = gerar_tb_cliente(n_clientes)
    salvar_csv(df_cliente, "tb_cliente", output_dir)
    contagem["tb_cliente"] = len(df_cliente)
    ids_cliente = df_cliente["id_cliente"].tolist()

    # tb_endereco_cliente (depende de tb_cliente)
    df_endereco = gerar_tb_endereco_cliente(ids_cliente)
    salvar_csv(df_endereco, "tb_endereco_cliente", output_dir)
    contagem["tb_endereco_cliente"] = len(df_endereco)

    # tb_operador
    n_operadores = max(50, n_rows // 20)  # 1 operador para cada 20 clientes
    df_operador = gerar_tb_operador(n_operadores)
    salvar_csv(df_operador, "tb_operador", output_dir)
    contagem["tb_operador"] = len(df_operador)
    ids_operador = df_operador["id_operador"].tolist()
    ids_supervisor = df_operador[df_operador["fl_supervisor"] == 1]["id_operador"].tolist()
    # Garante pelo menos um supervisor
    if not ids_supervisor:
        ids_supervisor = [ids_operador[0]]

    # tb_skill_operador (depende de tb_operador)
    df_skill = gerar_tb_skill_operador(ids_operador)
    salvar_csv(df_skill, "tb_skill_operador", output_dir)
    contagem["tb_skill_operador"] = len(df_skill)

    # tb_fila_atendimento
    df_fila = gerar_tb_fila_atendimento(n=20)
    salvar_csv(df_fila, "tb_fila_atendimento", output_dir)
    contagem["tb_fila_atendimento"] = len(df_fila)
    ids_fila = df_fila["id_fila"].tolist()

    # ------------------------------------------------------------------
    # 2. Tabelas de operação (dependem de cadastro)
    # ------------------------------------------------------------------
    print("\n[2/6] Tabelas de Operação")

    # tb_chamada
    n_chamadas = n_rows
    df_chamada = gerar_tb_chamada(n_chamadas, ids_cliente, ids_operador, ids_fila)
    salvar_csv(df_chamada, "tb_chamada", output_dir)
    contagem["tb_chamada"] = len(df_chamada)
    ids_chamada = df_chamada["id_chamada"].tolist()

    # tb_gravacao_chamada (depende de tb_chamada)
    df_gravacao = gerar_tb_gravacao_chamada(ids_chamada)
    salvar_csv(df_gravacao, "tb_gravacao_chamada", output_dir)
    contagem["tb_gravacao_chamada"] = len(df_gravacao)

    # tb_chat
    n_chat = n_rows // 2
    df_chat = gerar_tb_chat(n_chat, ids_cliente, ids_operador, ids_fila)
    salvar_csv(df_chat, "tb_chat", output_dir)
    contagem["tb_chat"] = len(df_chat)
    ids_chat = df_chat["id_chat"].tolist()

    # tb_mensagem_chat (depende de tb_chat)
    # Limita o sample para não explodir a memória
    ids_chat_amostra = random.sample(ids_chat, k=min(500, len(ids_chat)))
    df_mensagem = gerar_tb_mensagem_chat(ids_chat_amostra)
    salvar_csv(df_mensagem, "tb_mensagem_chat", output_dir)
    contagem["tb_mensagem_chat"] = len(df_mensagem)

    # tb_whatsapp_atendimento
    n_whatsapp = n_rows // 3
    df_whatsapp = gerar_tb_whatsapp_atendimento(n_whatsapp, ids_cliente, ids_operador)
    salvar_csv(df_whatsapp, "tb_whatsapp_atendimento", output_dir)
    contagem["tb_whatsapp_atendimento"] = len(df_whatsapp)

    # tb_ura_navegacao
    n_ura = n_rows
    df_ura = gerar_tb_ura_navegacao(n_ura, ids_cliente)
    salvar_csv(df_ura, "tb_ura_navegacao", output_dir)
    contagem["tb_ura_navegacao"] = len(df_ura)

    # ------------------------------------------------------------------
    # 3. Tabelas de suporte / tickets
    # ------------------------------------------------------------------
    print("\n[3/6] Tabelas de Suporte / Tickets")

    # tb_ticket
    n_ticket = n_rows // 2
    df_ticket = gerar_tb_ticket(n_ticket, ids_cliente, ids_operador)
    salvar_csv(df_ticket, "tb_ticket", output_dir)
    contagem["tb_ticket"] = len(df_ticket)
    ids_ticket = df_ticket["id_ticket"].tolist()

    # tb_interacao_ticket (depende de tb_ticket)
    ids_ticket_amostra = random.sample(ids_ticket, k=min(1000, len(ids_ticket)))
    df_interacao = gerar_tb_interacao_ticket(ids_ticket_amostra, ids_operador)
    salvar_csv(df_interacao, "tb_interacao_ticket", output_dir)
    contagem["tb_interacao_ticket"] = len(df_interacao)

    # ------------------------------------------------------------------
    # 4. Tabelas de qualidade
    # ------------------------------------------------------------------
    print("\n[4/6] Tabelas de Qualidade")

    # tb_avaliacao_qualidade (depende de tb_chamada e tb_operador)
    df_avaliacao = gerar_tb_avaliacao_qualidade(ids_chamada, ids_operador, ids_supervisor)
    salvar_csv(df_avaliacao, "tb_avaliacao_qualidade", output_dir)
    contagem["tb_avaliacao_qualidade"] = len(df_avaliacao)

    # tb_jornada_operador
    # Para não gerar volume excessivo, amostra no máximo 100 operadores
    ids_op_jornada = random.sample(ids_operador, k=min(100, len(ids_operador)))
    df_jornada = gerar_tb_jornada_operador(ids_op_jornada, n_dias=90)
    salvar_csv(df_jornada, "tb_jornada_operador", output_dir)
    contagem["tb_jornada_operador"] = len(df_jornada)

    # ------------------------------------------------------------------
    # 5. Tabelas de marketing / campanhas
    # ------------------------------------------------------------------
    print("\n[5/6] Tabelas de Marketing / Campanhas")

    # tb_campanha
    df_campanha = gerar_tb_campanha(n=30)
    salvar_csv(df_campanha, "tb_campanha", output_dir)
    contagem["tb_campanha"] = len(df_campanha)
    ids_campanha = df_campanha["id_campanha"].tolist()

    # tb_discagem
    n_discagem = n_rows
    df_discagem = gerar_tb_discagem(n_discagem, ids_campanha, ids_cliente, ids_operador)
    salvar_csv(df_discagem, "tb_discagem", output_dir)
    contagem["tb_discagem"] = len(df_discagem)

    # ------------------------------------------------------------------
    # 6. Tabelas de métricas operacionais
    # ------------------------------------------------------------------
    print("\n[6/6] Tabelas de Métricas Operacionais")

    n_metricas = n_rows
    df_metricas = gerar_tb_metricas_operacionais(n_metricas, ids_operador, ids_fila)
    salvar_csv(df_metricas, "tb_metricas_operacionais", output_dir)
    contagem["tb_metricas_operacionais"] = len(df_metricas)

    return contagem


# ---------------------------------------------------------------------------
# Resumo final
# ---------------------------------------------------------------------------

def imprimir_resumo(contagem: dict[str, int]) -> None:
    """Imprime um resumo formatado com a quantidade de registros por tabela."""
    total = sum(contagem.values())
    print(f"\n{'=' * 65}")
    print(f"  RESUMO — Registros gerados por tabela")
    print(f"{'=' * 65}")
    for tabela, qtd in contagem.items():
        print(f"  {tabela:45s} {qtd:>8,} registros")
    print(f"{'-' * 65}")
    print(f"  {'TOTAL':45s} {total:>8,} registros")
    print(f"{'=' * 65}\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Processa os argumentos de linha de comando."""
    parser = argparse.ArgumentParser(
        description=(
            "Gerador de dados sintéticos para o Contact Center Data Lakehouse. "
            "Gera CSVs com dados relacionalmente consistentes para as 18 tabelas."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python generate_data.py
  python generate_data.py --rows 1000
  python generate_data.py --rows 5000 --output-dir /tmp/contact_center_data
        """,
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=5000,
        metavar="N",
        help=(
            "Número base de registros para as tabelas principais "
            "(padrão: 5000). Tabelas dependentes escalam proporcionalmente."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(
            Path(__file__).parent / "output"
        ),
        metavar="DIR",
        help=(
            "Diretório de saída para os arquivos CSV "
            "(padrão: data/synthetic/output/)."
        ),
    )
    return parser.parse_args()


def main() -> None:
    """Ponto de entrada principal do gerador de dados sintéticos."""
    args = parse_args()
    output_dir = Path(args.output_dir)

    try:
        contagem = gerar_todos_os_dados(n_rows=args.rows, output_dir=output_dir)
        imprimir_resumo(contagem)
    except KeyboardInterrupt:
        print("\n[INTERROMPIDO] Geração cancelada pelo usuário.", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"\n[ERRO] Falha durante a geração: {exc}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
