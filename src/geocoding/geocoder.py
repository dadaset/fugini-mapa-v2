# ============================================================
# src/geocoding/geocoder.py
# Geocodifica clientes via Google Maps API.
# Checkpoint salvo no PostgreSQL — nunca reprocesa o que já foi.
# ============================================================

import logging
import time
import requests
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from config.settings import (
    GOOGLE_API_KEY,
    GEOCODING_BATCH_SIZE,
    GEOCODING_MAX_WORKERS,
    GEOCODING_SLEEP_BETWEEN_BATCHES,
    GEO_LAT_MIN, GEO_LAT_MAX,
    GEO_LNG_MIN, GEO_LNG_MAX,
)
from src.database.connection import get_connection

logger = logging.getLogger(__name__)


# ============================================================
# VALIDAÇÃO DE COORDENADA
# ============================================================

def coordenada_valida(lat, lng) -> bool:
    """Verifica se lat/lng está dentro do bounding box do Brasil."""
    try:
        lat, lng = float(lat), float(lng)
        return (
            pd.notna(lat) and pd.notna(lng)
            and lat != 0 and lng != 0
            and GEO_LAT_MIN <= lat <= GEO_LAT_MAX
            and GEO_LNG_MIN <= lng <= GEO_LNG_MAX
        )
    except (TypeError, ValueError):
        return False


# ============================================================
# MONTAGEM DE ENDEREÇO
# ============================================================

def montar_endereco(row: dict) -> tuple[str | None, int | None]:
    """
    Monta string de endereço para geocodificação.
    Retorna (endereco, nivel) onde nivel indica a qualidade:
      1 = endereço completo + CEP
      2 = só CEP
      3 = endereço sem CEP
      4 = só cidade
    """
    endereco  = row.get("endereco")
    bairro    = row.get("bairro")
    cep       = row.get("cep")
    cidade    = row.get("nome_municipio")
    uf        = row.get("uf")

    cep_limpo = None
    if pd.notna(cep) and str(cep).strip():
        c = str(cep).replace(".", "").replace("-", "").strip()
        if len(c) == 8:
            cep_limpo = f"{c[:5]}-{c[5:]}"

    def tem(val):
        return pd.notna(val) and str(val).strip() != ""

    if tem(endereco) and cep_limpo:
        partes = [str(endereco).strip()]
        if tem(bairro): partes.append(str(bairro).strip())
        if tem(cidade): partes.append(str(cidade).strip())
        if tem(uf):     partes.append(str(uf).strip())
        partes += [cep_limpo, "Brasil"]
        return ", ".join(partes), 1

    elif cep_limpo:
        return f"{cep_limpo}, Brasil", 2

    elif tem(endereco):
        partes = [str(endereco).strip()]
        if tem(bairro): partes.append(str(bairro).strip())
        if tem(cidade): partes.append(str(cidade).strip())
        if tem(uf):     partes.append(str(uf).strip())
        partes.append("Brasil")
        return ", ".join(partes), 3

    elif tem(cidade):
        return f"{cidade}, {uf}, Brasil", 4

    return None, None


# ============================================================
# CHAMADA À API DO GOOGLE
# ============================================================

def geocodificar_google(endereco: str) -> tuple:
    """
    Chama a API do Google Maps e retorna (lat, lng, tipo, status).
    """
    if not endereco or not endereco.strip():
        return None, None, "endereco_vazio", "SKIP"

    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address":  endereco,
        "key":      GOOGLE_API_KEY,
        "region":   "br",
        "language": "pt-BR",
    }

    try:
        resp = requests.get(url, params=params, timeout=5)
        data = resp.json()

        if data["status"] == "OK":
            result   = data["results"][0]
            location = result["geometry"]["location"]
            tipo     = result["geometry"]["location_type"]
            return location["lat"], location["lng"], tipo, "OK"

        elif data["status"] == "ZERO_RESULTS":
            return None, None, "nao_encontrado", "ZERO_RESULTS"

        elif data["status"] == "OVER_QUERY_LIMIT":
            time.sleep(1)
            return None, None, "limite_excedido", "OVER_QUERY_LIMIT"

        else:
            return None, None, "erro", data["status"]

    except Exception as e:
        return None, None, "exception", str(e)


# ============================================================
# CHECKPOINT — POSTGRESQL
# ============================================================

def carregar_checkpoint() -> set[str]:
    """Retorna set de cod_cliente já processados."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT cod_cliente FROM geocodificacao_checkpoint")
            return {r[0] for r in cur.fetchall()}
    finally:
        conn.close()


def salvar_checkpoint(resultados: list[dict]):
    """Salva lista de resultados no checkpoint. Ignora duplicatas."""
    if not resultados:
        return
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO geocodificacao_checkpoint
                        (cod_cliente, lat_google, lng_google,
                         tipo_localizacao, status, valido, nivel)
                    VALUES
                        (%(cod_cliente)s, %(lat_google)s, %(lng_google)s,
                         %(tipo_localizacao)s, %(status)s, %(valido)s, %(nivel)s)
                    ON CONFLICT (cod_cliente) DO NOTHING
                    """,
                    resultados,
                )
    finally:
        conn.close()


def carregar_resultados_checkpoint() -> pd.DataFrame:
    """Retorna todos os resultados do checkpoint como DataFrame."""
    conn = get_connection()
    try:
        return pd.read_sql(
            "SELECT cod_cliente, lat_google, lng_google, valido FROM geocodificacao_checkpoint",
            conn,
        )
    finally:
        conn.close()


# ============================================================
# PROCESSAMENTO DE UM CLIENTE
# ============================================================

def processar_cliente(row: dict) -> dict:
    """Geocodifica um cliente e retorna o resultado."""
    endereco_montado, nivel = montar_endereco(row)

    if endereco_montado:
        lat, lng, tipo, status = geocodificar_google(endereco_montado)
        valido = (
            status == "OK"
            and lat is not None
            and coordenada_valida(lat, lng)
        )
    else:
        lat, lng, tipo, status, valido, nivel = None, None, "sem_dados", "SKIP", False, None

    return {
        "cod_cliente":      str(row["cod_cliente"]),
        "lat_google":       lat,
        "lng_google":       lng,
        "tipo_localizacao": tipo,
        "status":           status,
        "valido":           valido,
        "nivel":            nivel,
    }


# ============================================================
# PIPELINE DE GEOCODIFICAÇÃO
# ============================================================

def geocodificar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Recebe DataFrame com clientes (saída do ingestion),
    adiciona colunas lat_final e lng_final.

    Clientes com coordenada TOTVS válida → usa direto.
    Restantes → consulta checkpoint → chama API se necessário.

    Retorna DataFrame com lat_final, lng_final, geo_valida_final.
    """
    df = df.copy()

    # --- Valida coordenadas TOTVS ---
    df["geo_valida_totvs"] = df.apply(
        lambda r: coordenada_valida(r.get("lat_totvs"), r.get("lng_totvs")),
        axis=1,
    )

    n_totvs   = df["geo_valida_totvs"].sum()
    n_sem_geo = (~df["geo_valida_totvs"]).sum()
    logger.info(f"Coordenada TOTVS válida: {n_totvs:,} | Precisam geocodificar: {n_sem_geo:,}")

    # --- Checkpoint ---
    ja_processados = carregar_checkpoint()
    df_sem_geo     = df[~df["geo_valida_totvs"]].copy()
    df_a_processar = df_sem_geo[
        ~df_sem_geo["cod_cliente"].astype(str).isin(ja_processados)
    ]

    logger.info(f"Já no checkpoint: {len(ja_processados):,} | "
                f"Novos para API: {len(df_a_processar):,}")

    # --- Geocodifica novos em batches paralelos ---
    if len(df_a_processar) > 0:
        _geocodificar_em_batches(df_a_processar)

    # --- Merge com checkpoint ---
    df_checkpoint = carregar_resultados_checkpoint()
    df = df.merge(
        df_checkpoint[["cod_cliente", "lat_google", "lng_google", "valido"]],
        on="cod_cliente",
        how="left",
    )

    # --- Coordenada final ---
    df["lat_final"] = np.where(
        df["geo_valida_totvs"],
        pd.to_numeric(df["lat_totvs"], errors="coerce"),
        pd.to_numeric(df["lat_google"], errors="coerce"),
    )
    df["lng_final"] = np.where(
        df["geo_valida_totvs"],
        pd.to_numeric(df["lng_totvs"], errors="coerce"),
        pd.to_numeric(df["lng_google"], errors="coerce"),
    )
    df["geo_valida_final"] = df["geo_valida_totvs"] | (df["valido"] == True)

    cobertura = df["geo_valida_final"].sum()
    logger.info(
        f"Cobertura final: {cobertura:,}/{len(df):,} "
        f"({cobertura/len(df)*100:.1f}%)"
    )

    return df


def _geocodificar_em_batches(df_a_processar: pd.DataFrame):
    """Processa clientes em batches paralelos e salva no checkpoint."""
    total         = len(df_a_processar)
    total_batches = (total + GEOCODING_BATCH_SIZE - 1) // GEOCODING_BATCH_SIZE
    inicio        = time.time()
    novos         = 0

    for i in range(total_batches):
        batch = df_a_processar.iloc[i * GEOCODING_BATCH_SIZE:(i + 1) * GEOCODING_BATCH_SIZE]
        logger.info(f"Batch {i+1}/{total_batches} ({len(batch)} clientes)")

        rows = batch.to_dict("records")
        resultados = []

        with ThreadPoolExecutor(max_workers=GEOCODING_MAX_WORKERS) as executor:
            futures = {executor.submit(processar_cliente, row): row for row in rows}
            for future in as_completed(futures):
                try:
                    resultados.append(future.result())
                except Exception as e:
                    logger.warning(f"Erro em cliente: {e}")

        salvar_checkpoint(resultados)
        novos += len(resultados)

        decorrido = time.time() - inicio
        velocidade = novos / decorrido if decorrido > 0 else 1
        restantes  = total - novos
        eta        = restantes / velocidade if velocidade > 0 else 0

        logger.info(f"  Progresso: {novos}/{total} | ETA: {eta/60:.1f}min")

        if i < total_batches - 1:
            time.sleep(GEOCODING_SLEEP_BETWEEN_BATCHES)
