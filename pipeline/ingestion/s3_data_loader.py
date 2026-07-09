"""
s3_data_loader.py
=================
Simula a ingestão CDC do pipeline DMS → Kinesis → Firehose → S3 Bronze,
escrevendo arquivos Parquet particionados diretamente no S3.

Em produção, esse caminho é feito automaticamente pelo AWS DMS capturando
o WAL do PostgreSQL e entregando via Kinesis Firehose com dynamic partitioning.

Para o ambiente de demonstração do portfólio, este script substitui toda
a camada de ingestão com custo zero.

Uso:
    python s3_data_loader.py --bucket act-cc-dev-lakehouse --tabela all
    python s3_data_loader.py --bucket act-cc-dev-lakehouse --tabela tb_chamada
    python s3_data_loader.py --bucket act-cc-dev-lakehouse --tabela all --dry-run
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ─── Mapeamento: tabela → path S3 Bronze ──────────────────────────────────────
TABLE_S3_PREFIX = {
    "tb_cliente":                "bronze/cadastro/cliente",
    "tb_endereco_cliente":       "bronze/cadastro/endereco",
    "tb_operador":               "bronze/cadastro/operador",
    "tb_skill_operador":         "bronze/cadastro/skill",
    "tb_fila_atendimento":       "bronze/cadastro/fila",
    "tb_chamada":                "bronze/operacao/chamada",
    "tb_gravacao_chamada":       "bronze/operacao/gravacao",
    "tb_ticket":                 "bronze/operacao/ticket",
    "tb_interacao_ticket":       "bronze/qualidade/ticket-interacao",
    "tb_avaliacao_qualidade":    "bronze/qualidade/avaliacao",
    "tb_jornada_operador":       "bronze/qualidade/jornada",
    "tb_chat":                   "bronze/operacao/chat",
    "tb_mensagem_chat":          "bronze/suporte/mensagem-chat",
    "tb_whatsapp_atendimento":   "bronze/operacao/whatsapp",
    "tb_ura_navegacao":          "bronze/operacao/ura",
    "tb_campanha":               "bronze/marketing/campanha",
    "tb_discagem":               "bronze/operacao/discagem",
    "tb_metricas_operacionais":  "bronze/operacao/metricas",
}

# ─── Coluna de partição temporal por tabela ──────────────────────────────────
TABLE_DATE_COLUMN = {
    "tb_cliente":               "dt_cadastro",
    "tb_endereco_cliente":      "_timestamp",
    "tb_operador":              "dt_admissao",
    "tb_skill_operador":        "dt_certificacao",
    "tb_fila_atendimento":      "_timestamp",
    "tb_chamada":               "dt_inicio",
    "tb_gravacao_chamada":      "dt_gravacao",
    "tb_ticket":                "dt_abertura",
    "tb_interacao_ticket":      "dt_interacao",
    "tb_avaliacao_qualidade":   "dt_avaliacao",
    "tb_jornada_operador":      "dt_jornada",
    "tb_chat":                  "dt_inicio",
    "tb_mensagem_chat":         "dt_mensagem",
    "tb_whatsapp_atendimento":  "dt_inicio",
    "tb_ura_navegacao":         "dt_navegacao",
    "tb_campanha":              "dt_inicio",
    "tb_discagem":              "dt_ultima_tentativa",
    "tb_metricas_operacionais": "dt_referencia",
}


def get_synthetic_dir() -> Path:
    """Localiza o diretório de dados sintéticos gerados pelo generate_data.py."""
    base = Path(__file__).resolve().parents[2]
    candidates = [
        base / "data" / "synthetic" / "output",
        base / "data" / "synthetic",
    ]
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError(
        "Diretório de dados sintéticos não encontrado. "
        "Execute primeiro: python data/synthetic/generate_data.py"
    )


def load_csv(tabela: str, data_dir: Path) -> pd.DataFrame:
    """Carrega o CSV de uma tabela a partir do diretório de dados sintéticos."""
    path = data_dir / f"{tabela}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")
    df = pd.read_csv(path, dtype=str)
    print(f"  Carregados {len(df):,} registros de {tabela}")
    return df


def add_partition_columns(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """Adiciona colunas de partição ano/mes/dia a partir de uma coluna de data."""
    if date_col not in df.columns:
        # Fallback: usa timestamp atual
        now = datetime.utcnow()
        df["ano"] = str(now.year)
        df["mes"] = f"{now.month:02d}"
        df["dia"] = f"{now.day:02d}"
        return df

    try:
        dt = pd.to_datetime(df[date_col], errors="coerce")
        df["ano"] = dt.dt.year.fillna(datetime.utcnow().year).astype(int).astype(str)
        df["mes"] = dt.dt.month.fillna(datetime.utcnow().month).astype(int).apply(
            lambda m: f"{int(m):02d}"
        )
        df["dia"] = dt.dt.day.fillna(datetime.utcnow().day).astype(int).apply(
            lambda d: f"{int(d):02d}"
        )
    except Exception as e:
        print(f"    Aviso ao particionar por {date_col}: {e}. Usando data atual.")
        now = datetime.utcnow()
        df["ano"] = str(now.year)
        df["mes"] = f"{now.month:02d}"
        df["dia"] = f"{now.day:02d}"

    return df


def upload_parquet_to_s3(
    df: pd.DataFrame,
    bucket: str,
    prefix: str,
    tabela: str,
    s3_client,
    dry_run: bool = False,
) -> int:
    """
    Faz upload do DataFrame como Parquet para o S3, particionado por ano/mes/dia.
    Retorna o número de arquivos enviados.
    """
    partitions = df.groupby(["ano", "mes", "dia"])
    files_uploaded = 0

    for (ano, mes, dia), partition_df in partitions:
        s3_key = (
            f"{prefix}/ano={ano}/mes={mes}/dia={dia}/"
            f"{tabela}_{ano}{mes}{dia}_{datetime.utcnow().strftime('%H%M%S')}.parquet"
        )

        # Remover colunas de partição do conteúdo (já estão no path)
        data_df = partition_df.drop(columns=["ano", "mes", "dia"], errors="ignore")

        # Converter para Parquet em memória
        table = pa.Table.from_pandas(data_df, preserve_index=False)
        buf = pa.BufferOutputStream()
        pq.write_table(
            table,
            buf,
            compression="snappy",
            use_deprecated_int96_timestamps=False,
        )
        parquet_bytes = buf.getvalue().to_pybytes()

        if dry_run:
            print(f"    [DRY-RUN] s3://{bucket}/{s3_key} ({len(partition_df):,} linhas)")
        else:
            s3_client.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=parquet_bytes,
                ContentType="application/octet-stream",
            )
            print(
                f"    Enviado: s3://{bucket}/{s3_key} "
                f"({len(partition_df):,} linhas, {len(parquet_bytes)/1024:.1f} KB)"
            )

        files_uploaded += 1

    return files_uploaded


def process_table(
    tabela: str,
    bucket: str,
    data_dir: Path,
    s3_client,
    dry_run: bool,
) -> dict:
    """Processa uma tabela: carrega CSV → adiciona partições → faz upload no S3."""
    result = {"tabela": tabela, "linhas": 0, "arquivos": 0, "status": "OK", "erro": ""}

    try:
        df = load_csv(tabela, data_dir)
        result["linhas"] = len(df)

        date_col = TABLE_DATE_COLUMN.get(tabela, "_timestamp")
        df = add_partition_columns(df, date_col)

        prefix = TABLE_S3_PREFIX[tabela]
        n_files = upload_parquet_to_s3(df, bucket, prefix, tabela, s3_client, dry_run)
        result["arquivos"] = n_files

    except FileNotFoundError as e:
        result["status"] = "SKIP"
        result["erro"] = str(e)
        print(f"    SKIP — {e}")
    except Exception as e:
        result["status"] = "ERRO"
        result["erro"] = str(e)
        print(f"    ERRO — {e}")

    return result


def print_summary(results: list[dict], dry_run: bool) -> None:
    """Imprime resumo da execução."""
    mode = "DRY-RUN" if dry_run else "REAL"
    print(f"\n{'='*65}")
    print(f"  RESUMO DA INGESTÃO [{mode}]")
    print(f"{'='*65}")
    print(f"  {'TABELA':<35} {'LINHAS':>8} {'ARQUIVOS':>9} {'STATUS'}")
    print(f"  {'-'*35} {'-'*8} {'-'*9} {'-'*6}")

    total_linhas = 0
    total_arquivos = 0
    for r in results:
        print(
            f"  {r['tabela']:<35} {r['linhas']:>8,} {r['arquivos']:>9} {r['status']}"
        )
        if r["status"] == "OK":
            total_linhas += r["linhas"]
            total_arquivos += r["arquivos"]

    print(f"  {'-'*35} {'-'*8} {'-'*9} {'-'*6}")
    print(f"  {'TOTAL':<35} {total_linhas:>8,} {total_arquivos:>9}")
    print(f"{'='*65}\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Carrega dados sintéticos CSV → S3 Bronze (Parquet + Snappy)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--bucket",
        default="act-cc-dev-lakehouse",
        help="Nome do bucket S3 (padrão: act-cc-dev-lakehouse)",
    )
    parser.add_argument(
        "--tabela",
        default="all",
        choices=["all"] + list(TABLE_S3_PREFIX.keys()),
        help="Tabela a carregar, ou 'all' para todas (padrão: all)",
    )
    parser.add_argument(
        "--region",
        default="sa-east-1",
        help="Região AWS (padrão: sa-east-1)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simula o upload sem enviar dados para o S3",
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Caminho para o diretório de CSVs sintéticos (opcional)",
    )
    args = parser.parse_args()

    # Localizar dados sintéticos
    if args.data_dir:
        data_dir = Path(args.data_dir)
    else:
        try:
            data_dir = get_synthetic_dir()
        except FileNotFoundError as e:
            print(f"\nERRO: {e}\n")
            sys.exit(1)

    print(f"\nDiretório de dados: {data_dir}")
    print(f"Bucket S3: s3://{args.bucket}")
    print(f"Modo: {'DRY-RUN' if args.dry_run else 'UPLOAD REAL'}\n")

    # Cliente S3
    s3_client = boto3.client("s3", region_name=args.region)

    # Selecionar tabelas
    tabelas = list(TABLE_S3_PREFIX.keys()) if args.tabela == "all" else [args.tabela]

    results = []
    for tabela in tabelas:
        print(f"\n[{tabelas.index(tabela)+1}/{len(tabelas)}] Processando: {tabela}")
        result = process_table(tabela, args.bucket, data_dir, s3_client, args.dry_run)
        results.append(result)

    print_summary(results, args.dry_run)

    # Exit code 1 se alguma tabela teve erro
    erros = [r for r in results if r["status"] == "ERRO"]
    if erros:
        print(f"Atenção: {len(erros)} tabela(s) com erro.")
        sys.exit(1)


if __name__ == "__main__":
    main()
