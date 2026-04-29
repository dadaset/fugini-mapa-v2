# ============================================================
# src/ingestion/loader.py
# Carrega planilhas do comercial, normaliza colunas consultando
# o mapeamento no PostgreSQL, e filtra clientes disponíveis.
#
# Quando chegar nova planilha com nomes diferentes:
#   1. Insira as novas linhas em colunas_mapeamento no banco
#   2. Rode o pipeline normalmente — sem tocar no código
# ============================================================

import logging
import psycopg2.extras
import pandas as pd
from src.database.connection import get_connection

logger = logging.getLogger(__name__)

# Colunas canônicas que o pipeline precisa
COLUNAS_NECESSARIAS = [
    "cod_cliente",
    "observacao",
    "nome_cliente",
    "limite_disp",
    "lat_totvs",
    "lng_totvs",
    "endereco",
    "bairro",
    "cep",
    "cod_ibge",
    "uf",
]


def carregar_mapeamento(fonte: str) -> dict[str, str]:
    """
    Busca no banco o mapeamento col_original -> col_canonical para a fonte.
    Retorna dicionário: {'OBSERVAÇÃO GR': 'observacao', 'cod-cliente': 'cod_cliente', ...}
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT col_original, col_canonical FROM colunas_mapeamento WHERE fonte = %s",
                (fonte,),
            )
            rows = cur.fetchall()
            if not rows:
                raise ValueError(
                    f"Nenhum mapeamento encontrado para fonte '{fonte}'. "
                    f"Insira as entradas em colunas_mapeamento."
                )
            return {r["col_original"]: r["col_canonical"] for r in rows}
    finally:
        conn.close()


def carregar_municipios_banco() -> pd.DataFrame:
    """Carrega tabela de municípios do banco como DataFrame."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT cod_ibge, nome_municipio, uf FROM municipios")
            rows = cur.fetchall()
            return pd.DataFrame(rows)
    finally:
        conn.close()


def carregar_planilha(path: str, fonte: str) -> pd.DataFrame:
    """
    Carrega uma planilha Excel, normaliza os nomes das colunas
    conforme mapeamento no banco, e filtra clientes disponíveis.

    Parâmetros
    ----------
    path   : caminho para o arquivo .xlsx
    fonte  : 'saneamento' ou 'diversos' (deve existir em colunas_mapeamento)

    Retorna
    -------
    DataFrame com colunas canônicas e apenas clientes disponíveis.
    """
    logger.info(f"Carregando planilha: {path} (fonte: {fonte})")
    df = pd.read_excel(path)
    logger.info(f"  {len(df):,} linhas carregadas.")

    # Busca mapeamento no banco
    mapeamento = carregar_mapeamento(fonte)

    # Renomeia apenas as colunas que existem no mapeamento
    colunas_renomear     = {k: v for k, v in mapeamento.items() if k in df.columns}
    colunas_nao_mapeadas = [c for c in df.columns if c not in mapeamento]
    df = df.rename(columns=colunas_renomear)

    if colunas_nao_mapeadas:
        logger.debug(f"  Colunas não mapeadas (ignoradas): {colunas_nao_mapeadas}")

    if "observacao" not in df.columns:
        raise ValueError(
            f"Coluna 'observacao' não encontrada após mapeamento para fonte '{fonte}'. "
            f"Verifique o mapeamento em colunas_mapeamento."
        )

    # Filtra clientes disponíveis — case-insensitive, ignora acentos
    total_antes = len(df)
    df["observacao"] = df["observacao"].astype(str).str.strip()
    df = df[
        df["observacao"]
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("ascii")
        .str.lower() == "disponivel"
    ]
    logger.info(f"  Disponíveis: {len(df):,} de {total_antes:,}")

    # Mantém só as colunas canônicas que existirem
    colunas_presentes = [c for c in COLUNAS_NECESSARIAS if c in df.columns]
    df = df[colunas_presentes].copy()

    # Tipos
    for col in ["lat_totvs", "lng_totvs", "limite_disp"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "cod_ibge" in df.columns:
        df["cod_ibge"] = pd.to_numeric(df["cod_ibge"], errors="coerce")

    df["cod_cliente"] = df["cod_cliente"].astype(str).str.strip()
    df["fonte"] = fonte

    logger.info(f"  Colunas finais: {df.columns.tolist()}")
    return df


def carregar_todas(fontes: list[tuple[str, str]]) -> pd.DataFrame:
    """
    Carrega e concatena múltiplas planilhas e faz join com
    a tabela de municípios do banco para trazer nome_municipio.

    Parâmetros
    ----------
    fontes : lista de tuplas (path, fonte)

    Retorna
    -------
    DataFrame unificado com nome_municipio resolvido.
    """
    dfs = []
    for path, fonte in fontes:
        try:
            df = carregar_planilha(path, fonte)
            dfs.append(df)
            logger.info(f"✅ {fonte}: {len(df):,} clientes disponíveis")
        except Exception as e:
            logger.error(f"❌ Erro ao carregar {fonte} ({path}): {e}")
            raise

    df_total = pd.concat(dfs, ignore_index=True)
    df_total = df_total.drop_duplicates(subset="cod_cliente", keep="first")

    # Join com municípios do banco
    if "cod_ibge" in df_total.columns:
        logger.info("Resolvendo nome_municipio via banco...")
        df_mun = carregar_municipios_banco()
        df_mun["cod_ibge"] = pd.to_numeric(df_mun["cod_ibge"], errors="coerce")
        df_total = df_total.merge(
            df_mun[["cod_ibge", "nome_municipio"]],
            on="cod_ibge",
            how="left",
        )
        resolvidos = df_total["nome_municipio"].notna().sum()
        logger.info(f"  nome_municipio resolvido: {resolvidos:,}/{len(df_total):,}")
    else:
        df_total["nome_municipio"] = None

    logger.info(f"\nTotal unificado: {len(df_total):,} clientes")
    return df_total
