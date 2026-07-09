"""
lambda_function.py
==================
AWS Lambda function que inicia Glue Crawlers automaticamente em resposta
a eventos do EventBridge disparados por notificações de PutObject no S3.

Fluxo:
    1. EventBridge captura evento S3 PutObject na camada Bronze.
    2. Lambda extrai bucket e prefix do evento.
    3. Mapeia o prefix para o nome do Glue Crawler correspondente.
    4. Verifica se o Crawler não está em execução (evita sobreposição).
    5. Inicia o Crawler e retorna o status da operação.

Variáveis de ambiente esperadas:
    LOG_LEVEL        Nível de log (DEBUG, INFO, WARNING, ERROR). Padrão: INFO.
    REGION           Região AWS. Padrão: sa-east-1.

IAM Permissions necessárias para a Lambda:
    - glue:GetCrawler
    - glue:StartCrawler
    - logs:CreateLogGroup
    - logs:CreateLogStream
    - logs:PutLogEvents

Evento de entrada (EventBridge — S3 PutObject):
    {
        "source": "aws.s3",
        "detail-type": "Object Created",
        "detail": {
            "bucket": { "name": "contact-center-bronze" },
            "object": { "key": "bronze/operacao/chamada/year=2025/month=01/file.parquet" }
        }
    }

Também suporta invocação direta para testes:
    {
        "object_key": "bronze/operacao/chamada/2025/01/01/arquivo.parquet",
        "bucket_name": "contact-center-bronze"
    }

Autor: Ilciley Junior
Data : 2026-07-09
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Configuração de logging estruturado
# ---------------------------------------------------------------------------

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# Configura o logger raiz (compatível com o ambiente Lambda, que já cria
# um handler de StreamHandler por padrão)
logger = logging.getLogger()
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))


# ---------------------------------------------------------------------------
# Mapeamento de prefixes S3 → Glue Crawlers
# ---------------------------------------------------------------------------

# Cada chave representa um prefix S3 no formato "camada/dominio/tabela/".
# O mapeamento cobre os 18 domínios de dados do Contact Center.
#
# Convenção de nomes dos crawlers:
#   crawler-bronze-tb-<nome-da-tabela>
#
PREFIX_TO_CRAWLER: dict[str, str] = {
    # ---- Domínio: Operação ------------------------------------------------
    # Registros de interações operacionais: chamadas, tickets, chat, etc.
    "bronze/operacao/chamada/":      "crawler-bronze-tb-chamada",
    "bronze/operacao/ticket/":       "crawler-bronze-tb-ticket",
    "bronze/operacao/chat/":         "crawler-bronze-tb-chat",
    "bronze/operacao/whatsapp/":     "crawler-bronze-tb-whatsapp",
    "bronze/operacao/ura/":          "crawler-bronze-tb-ura-navegacao",
    "bronze/operacao/discagem/":     "crawler-bronze-tb-discagem",
    "bronze/operacao/metricas/":     "crawler-bronze-tb-metricas-operacionais",
    "bronze/operacao/gravacao/":     "crawler-bronze-tb-gravacao-chamada",

    # ---- Domínio: Cadastro -----------------------------------------------
    # Dados mestres: clientes, endereços, operadores, skills e filas
    "bronze/cadastro/cliente/":      "crawler-bronze-tb-cliente",
    "bronze/cadastro/endereco/":     "crawler-bronze-tb-endereco-cliente",
    "bronze/cadastro/operador/":     "crawler-bronze-tb-operador",
    "bronze/cadastro/skill/":        "crawler-bronze-tb-skill-operador",
    "bronze/cadastro/fila/":         "crawler-bronze-tb-fila-atendimento",

    # ---- Domínio: Qualidade ----------------------------------------------
    # Monitoramento de qualidade: avaliações, jornadas e interações de ticket
    "bronze/qualidade/avaliacao/":        "crawler-bronze-tb-avaliacao-qualidade",
    "bronze/qualidade/jornada/":          "crawler-bronze-tb-jornada-operador",
    "bronze/qualidade/ticket-interacao/": "crawler-bronze-tb-interacao-ticket",

    # ---- Domínio: Marketing ----------------------------------------------
    # Campanhas de discagem ativa e outbound
    "bronze/marketing/campanha/":    "crawler-bronze-tb-campanha",

    # ---- Domínio: Suporte ------------------------------------------------
    # Mensagens de chat (volume elevado, crawler separado)
    "bronze/suporte/mensagem-chat/": "crawler-bronze-tb-mensagem-chat",
}

# Estados do Glue Crawler que indicam execução em andamento.
# Iniciar um crawler nesses estados gera CrawlerRunningException.
CRAWLER_RUNNING_STATES: frozenset[str] = frozenset({"RUNNING", "STOPPING"})

# Região AWS (configurável via variável de ambiente)
REGION = os.environ.get("REGION", "sa-east-1")


# ---------------------------------------------------------------------------
# Cliente boto3
# Inicializado fora do handler para reuso entre invocações (execution context).
# ---------------------------------------------------------------------------

glue_client = boto3.client("glue", region_name=REGION)


# ---------------------------------------------------------------------------
# Funções auxiliares
# ---------------------------------------------------------------------------

def extrair_s3_info(event: dict[str, Any]) -> tuple[str, str]:
    """
    Extrai o bucket e a chave (key) S3 de um evento do EventBridge.

    Suporta dois formatos:
    1. Evento nativo S3 via EventBridge (source: aws.s3 / detail-type: Object Created).
    2. Invocação direta com os campos "object_key" e opcionalmente "bucket_name".

    Parameters
    ----------
    event : dict
        Evento recebido pelo handler Lambda.

    Returns
    -------
    tuple[str, str]
        Uma tupla (bucket_name, object_key).

    Raises
    ------
    ValueError
        Se o formato do evento não for reconhecido ou os campos obrigatórios
        estiverem ausentes.
    """
    source = event.get("source", "")

    # --- Formato 1: Evento EventBridge nativo do S3 ---
    if source == "aws.s3":
        detail = event.get("detail", {})
        bucket_name: str = detail.get("bucket", {}).get("name", "")
        object_key: str = detail.get("object", {}).get("key", "")

        if not bucket_name:
            raise ValueError(
                "Campo 'detail.bucket.name' ausente no evento EventBridge S3. "
                f"detail recebido: {json.dumps(detail, default=str)}"
            )
        if not object_key:
            raise ValueError(
                "Campo 'detail.object.key' ausente no evento EventBridge S3. "
                f"detail recebido: {json.dumps(detail, default=str)}"
            )
        return bucket_name, object_key

    # --- Formato 2: Invocação direta (testes / Step Functions) ---
    if "object_key" in event:
        object_key = event["object_key"]
        bucket_name = event.get("bucket_name", "contact-center-bronze")
        if not object_key:
            raise ValueError("Campo 'object_key' está vazio na invocação direta.")
        return bucket_name, object_key

    raise ValueError(
        f"Formato de evento não reconhecido. source='{source}'. "
        "Esperado: evento EventBridge S3 (source='aws.s3') ou "
        "invocação direta com campo 'object_key'."
    )


def resolver_prefix(object_key: str) -> str:
    """
    Resolve o prefix S3 a partir da chave completa do objeto.

    A chave pode ter o formato:
        bronze/operacao/chamada/year=2025/month=01/day=15/file.parquet

    O prefix é formado pelos primeiros três segmentos do caminho seguidos
    de barra final:
        bronze/operacao/chamada/

    Parameters
    ----------
    object_key : str
        Chave completa do objeto S3.

    Returns
    -------
    str
        Prefix no formato "segmento1/segmento2/segmento3/".

    Raises
    ------
    ValueError
        Se a chave não contiver ao menos três segmentos de path ou
        se o primeiro segmento não for "bronze".
    """
    # Remove barras no início e no fim para normalizar
    partes = object_key.strip("/").split("/")

    if len(partes) < 3:
        raise ValueError(
            f"Chave S3 '{object_key}' possui apenas {len(partes)} segmento(s). "
            "Esperado ao menos 3 (ex: bronze/operacao/chamada/...)."
        )

    # Valida que o objeto pertence à camada bronze
    if partes[0].lower() != "bronze":
        raise ValueError(
            f"Chave S3 '{object_key}' não pertence à camada bronze. "
            f"Primeiro segmento encontrado: '{partes[0]}'."
        )

    # Reconstrói o prefix com os 3 primeiros segmentos + barra final
    prefix = "/".join(partes[:3]) + "/"
    return prefix


def obter_nome_crawler(prefix: str) -> str:
    """
    Retorna o nome do Glue Crawler associado ao prefix S3.

    Parameters
    ----------
    prefix : str
        Prefix S3 no formato "camada/dominio/tabela/".

    Returns
    -------
    str
        Nome do Glue Crawler correspondente.

    Raises
    ------
    KeyError
        Se o prefix não estiver mapeado em PREFIX_TO_CRAWLER.
    """
    crawler_name = PREFIX_TO_CRAWLER.get(prefix)
    if crawler_name is None:
        raise KeyError(
            f"Prefix '{prefix}' não encontrado no mapeamento de crawlers. "
            f"Prefixes registrados: {sorted(PREFIX_TO_CRAWLER.keys())}"
        )
    return crawler_name


def obter_estado_crawler(crawler_name: str) -> str:
    """
    Consulta o estado atual do Glue Crawler via API.

    States possíveis do Glue Crawler:
        READY     — Pronto para iniciar.
        RUNNING   — Em execução.
        STOPPING  — Em processo de parada.

    Parameters
    ----------
    crawler_name : str
        Nome do Glue Crawler.

    Returns
    -------
    str
        Estado atual do crawler ("READY", "RUNNING", "STOPPING" ou "NOT_FOUND").

    Raises
    ------
    ClientError
        Se houver erro de comunicação com a API AWS (exceto EntityNotFound).
    """
    try:
        response = glue_client.get_crawler(Name=crawler_name)
        crawler_info = response.get("Crawler", {})
        state: str = crawler_info.get("State", "UNKNOWN")
        last_crawl = crawler_info.get("LastCrawl", {})

        logger.info(
            json.dumps({
                "message": "Estado do crawler consultado",
                "crawler_name": crawler_name,
                "state": state,
                "last_crawl_status": last_crawl.get("Status"),
                "last_crawl_start_time": str(last_crawl.get("StartTime")),
            })
        )
        return state

    except glue_client.exceptions.EntityNotFoundException:
        logger.warning(
            json.dumps({
                "message": "Crawler não encontrado no Glue",
                "crawler_name": crawler_name,
            })
        )
        return "NOT_FOUND"

    except ClientError as exc:
        logger.error(
            json.dumps({
                "message": "Erro ao consultar estado do crawler",
                "crawler_name": crawler_name,
                "error_code": exc.response["Error"]["Code"],
                "error_message": exc.response["Error"]["Message"],
            })
        )
        raise


def iniciar_crawler(crawler_name: str) -> dict[str, Any]:
    """
    Inicia o Glue Crawler via API, verificando previamente seu estado.

    Evita sobreposição de execuções ao verificar se o crawler já
    está nos estados RUNNING ou STOPPING antes de chamá-lo.

    Parameters
    ----------
    crawler_name : str
        Nome do Glue Crawler a ser iniciado.

    Returns
    -------
    dict
        Dicionário descrevendo o resultado da operação:
        - status (str): "STARTED" | "SKIPPED" | "ERROR"
        - crawler_name (str): nome do crawler.
        - state_before (str): estado do crawler antes da ação.
        - message (str): mensagem descritiva do resultado.

    Raises
    ------
    ClientError
        Se houver erro inesperado na API da AWS (diferente de
        CrawlerRunningException e EntityNotFoundException).
    """
    estado_atual = obter_estado_crawler(crawler_name)

    # Crawler não existe: erro de configuração
    if estado_atual == "NOT_FOUND":
        msg = (
            f"Crawler '{crawler_name}' não encontrado no Glue. "
            "Verifique se o crawler foi provisionado pelo Terraform/CDK."
        )
        logger.error(json.dumps({"message": msg, "crawler_name": crawler_name}))
        return {
            "status": "ERROR",
            "crawler_name": crawler_name,
            "state_before": estado_atual,
            "message": msg,
        }

    # Crawler já em execução: pula sem disparar novo run
    if estado_atual in CRAWLER_RUNNING_STATES:
        msg = (
            f"Crawler '{crawler_name}' já está em execução "
            f"(estado: {estado_atual}). Nenhuma ação tomada."
        )
        logger.warning(json.dumps({"message": msg, "crawler_name": crawler_name, "state": estado_atual}))
        return {
            "status": "SKIPPED",
            "crawler_name": crawler_name,
            "state_before": estado_atual,
            "message": msg,
        }

    # Inicia o crawler
    logger.info(
        json.dumps({
            "message": "Iniciando Glue Crawler",
            "crawler_name": crawler_name,
            "state_before": estado_atual,
        })
    )

    try:
        glue_client.start_crawler(Name=crawler_name)

    except glue_client.exceptions.CrawlerRunningException:
        # Condição de corrida: outro processo iniciou entre get e start
        msg = (
            f"Condição de corrida: crawler '{crawler_name}' foi iniciado "
            "por outro processo entre a verificação e o disparo."
        )
        logger.warning(json.dumps({"message": msg, "crawler_name": crawler_name}))
        return {
            "status": "SKIPPED",
            "crawler_name": crawler_name,
            "state_before": estado_atual,
            "message": msg,
        }

    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        error_msg = exc.response["Error"]["Message"]
        logger.error(
            json.dumps({
                "message": "Erro ao iniciar Glue Crawler",
                "crawler_name": crawler_name,
                "error_code": error_code,
                "error_message": error_msg,
            })
        )
        raise

    msg = f"Crawler '{crawler_name}' iniciado com sucesso (era: {estado_atual})."
    logger.info(json.dumps({"message": msg, "crawler_name": crawler_name}))

    return {
        "status": "STARTED",
        "crawler_name": crawler_name,
        "state_before": estado_atual,
        "message": msg,
    }


def construir_resposta(status_code: int, body: dict[str, Any]) -> dict[str, Any]:
    """
    Constrói a resposta padronizada da Lambda (formato API Gateway / EventBridge).

    Parameters
    ----------
    status_code : int
        Código HTTP de status (200, 400, 500).
    body : dict
        Corpo da resposta a ser serializado como JSON.

    Returns
    -------
    dict
        Resposta no formato { statusCode, headers, body }.
    """
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, default=str, ensure_ascii=False),
    }


# ---------------------------------------------------------------------------
# Handler principal
# ---------------------------------------------------------------------------

def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Ponto de entrada da AWS Lambda.

    Recebe um evento do EventBridge gerado por notificação S3 PutObject,
    identifica o Glue Crawler responsável pela tabela afetada e o inicia
    caso não esteja em execução.

    Parameters
    ----------
    event : dict
        Evento do EventBridge. Formatos suportados:

        1. Evento S3 via EventBridge (preferido em produção):
           {
               "source": "aws.s3",
               "detail-type": "Object Created",
               "detail": {
                   "bucket": {"name": "contact-center-bronze"},
                   "object": {"key": "bronze/operacao/chamada/year=2025/01/data.parquet"}
               }
           }

        2. Invocação direta (para testes e Step Functions):
           {
               "object_key": "bronze/operacao/chamada/2025/01/01/data.parquet",
               "bucket_name": "contact-center-bronze"
           }

    context : LambdaContext
        Contexto de execução da Lambda (function_name, request_id, etc.).

    Returns
    -------
    dict
        Resposta com statusCode e body JSON:
        - 200: Operação concluída (crawler iniciado, em execução ou não mapeado).
        - 400: Evento mal formatado, chave S3 inválida ou prefix não mapeado.
        - 500: Erro inesperado na API AWS.
    """
    # Log inicial — registra contexto da invocação
    logger.info(
        json.dumps({
            "message": "Lambda invocada",
            "function_name": getattr(context, "function_name", "local"),
            "request_id": getattr(context, "aws_request_id", "local"),
            "event_source": event.get("source", "desconhecido"),
            "detail_type": event.get("detail-type", "desconhecido"),
        })
    )
    logger.debug(json.dumps({"message": "Evento completo", "event": event}, default=str))

    # ------------------------------------------------------------------
    # Etapa 1: Extrair informações S3 do evento
    # ------------------------------------------------------------------
    try:
        bucket_name, object_key = extrair_s3_info(event)
    except ValueError as exc:
        logger.error(json.dumps({"message": "Evento inválido", "error": str(exc)}))
        return construir_resposta(
            status_code=400,
            body={
                "status": "error",
                "error_type": "invalid_event",
                "message": str(exc),
            },
        )

    logger.info(
        json.dumps({
            "message": "Informações S3 extraídas",
            "bucket": bucket_name,
            "object_key": object_key,
        })
    )

    # ------------------------------------------------------------------
    # Etapa 2: Resolver o prefix a partir da chave do objeto
    # ------------------------------------------------------------------
    try:
        prefix = resolver_prefix(object_key)
    except ValueError as exc:
        logger.error(
            json.dumps({
                "message": "Chave S3 com formato inválido",
                "object_key": object_key,
                "error": str(exc),
            })
        )
        return construir_resposta(
            status_code=400,
            body={
                "status": "error",
                "error_type": "invalid_s3_key",
                "bucket": bucket_name,
                "object_key": object_key,
                "message": str(exc),
            },
        )

    logger.info(json.dumps({"message": "Prefix S3 resolvido", "prefix": prefix}))

    # ------------------------------------------------------------------
    # Etapa 3: Mapear o prefix para o nome do Glue Crawler
    # ------------------------------------------------------------------
    try:
        crawler_name = obter_nome_crawler(prefix)
    except KeyError as exc:
        logger.warning(
            json.dumps({
                "message": "Prefix S3 sem crawler mapeado — evento ignorado",
                "prefix": prefix,
                "object_key": object_key,
            })
        )
        return construir_resposta(
            status_code=400,
            body={
                "status": "not_mapped",
                "bucket": bucket_name,
                "object_key": object_key,
                "prefix": prefix,
                "message": str(exc),
            },
        )

    logger.info(
        json.dumps({
            "message": "Crawler identificado para o prefix",
            "prefix": prefix,
            "crawler_name": crawler_name,
        })
    )

    # ------------------------------------------------------------------
    # Etapa 4: Iniciar o Glue Crawler (se elegível)
    # ------------------------------------------------------------------
    try:
        resultado = iniciar_crawler(crawler_name)
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        error_msg = exc.response["Error"]["Message"]
        logger.error(
            json.dumps({
                "message": "Erro inesperado ao interagir com o Glue",
                "crawler_name": crawler_name,
                "error_code": error_code,
                "error_message": error_msg,
            })
        )
        return construir_resposta(
            status_code=500,
            body={
                "status": "error",
                "error_type": "aws_api_error",
                "crawler_name": crawler_name,
                "aws_error_code": error_code,
                "message": error_msg,
            },
        )
    except Exception as exc:  # noqa: BLE001 — captura residual para evitar perda de evento
        logger.exception(
            json.dumps({
                "message": "Erro não tratado na execução da Lambda",
                "crawler_name": crawler_name,
                "error": str(exc),
            })
        )
        return construir_resposta(
            status_code=500,
            body={
                "status": "error",
                "error_type": "unexpected_error",
                "crawler_name": crawler_name,
                "message": str(exc),
            },
        )

    # ------------------------------------------------------------------
    # Etapa 5: Retornar resposta de sucesso
    # ------------------------------------------------------------------
    logger.info(json.dumps({"message": "Execução concluída", "resultado": resultado}))

    return construir_resposta(
        status_code=200,
        body={
            "status": "success",
            "bucket": bucket_name,
            "object_key": object_key,
            "prefix": prefix,
            **resultado,
        },
    )


# ---------------------------------------------------------------------------
# Bloco de teste local
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    """
    Execução local para validação do fluxo sem deploy na AWS.

    Simula eventos para todos os 18 prefixes mapeados para garantir
    que o mapeamento está correto.

    Uso:
        python lambda_function.py

    Requer credenciais AWS configuradas e crawlers existentes na conta.
    Para testar apenas o roteamento (sem chamada real ao Glue), o
    mock do glue_client pode ser substituído por um MagicMock.
    """
    import sys

    print("=" * 65)
    print("  Teste local — fn_start_glue_crawler")
    print("=" * 65)

    # Contexto mock
    class MockContext:
        function_name = "fn_start_glue_crawler"
        aws_request_id = "local-test-00000001"

    # Testa o roteamento de cada prefix registrado (sem chamar o Glue de fato)
    print("\n[1] Validando mapeamento de todos os prefixes:\n")
    erros = 0
    for prefix, crawler_esperado in PREFIX_TO_CRAWLER.items():
        try:
            crawler_obtido = obter_nome_crawler(prefix)
            status = "OK" if crawler_obtido == crawler_esperado else "FALHOU"
            if status != "OK":
                erros += 1
        except KeyError:
            status = "FALHOU"
            erros += 1
        print(f"  [{status}] {prefix:45s} → {crawler_esperado}")

    print(f"\n  Resultado: {len(PREFIX_TO_CRAWLER)} prefixes testados, {erros} erro(s).\n")

    # Testa o handler com um evento EventBridge completo
    print("[2] Simulando evento EventBridge S3 PutObject:\n")
    evento_teste: dict[str, Any] = {
        "version": "0",
        "id": "test-event-id-001",
        "source": "aws.s3",
        "detail-type": "Object Created",
        "region": "sa-east-1",
        "time": "2025-07-09T12:00:00Z",
        "detail": {
            "bucket": {
                "name": "contact-center-data-lakehouse-bronze"
            },
            "object": {
                "key": (
                    "bronze/operacao/chamada/"
                    "year=2025/month=07/day=09/part-0001.parquet"
                ),
                "size": 2048576,
                "etag": "d41d8cd98f00b204e9800998ecf8427e",
            },
            "reason": "PutObject",
        },
    }

    print(f"  Evento:\n  {json.dumps(evento_teste, indent=4, ensure_ascii=False)}\n")

    resposta = lambda_handler(evento_teste, MockContext())
    body = json.loads(resposta["body"])

    print(f"  Status Code : {resposta['statusCode']}")
    print(f"  Resposta    : {json.dumps(body, indent=4, ensure_ascii=False)}\n")

    sys.exit(0 if erros == 0 else 1)
