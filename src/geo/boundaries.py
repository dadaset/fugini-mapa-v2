# ============================================================
# src/geo/boundaries.py
# Baixa GeoJSON dos municípios de SP, monta polígono da Grande SP
# e classifica clientes como dentro ou fora da região.
# ============================================================

import logging
import requests
import pandas as pd
from shapely.geometry import Point, shape
from shapely.ops import unary_union

from config.settings import MUNICIPIOS_GRANDE_SP, GEOJSON_SP_URL

logger = logging.getLogger(__name__)

# Cache em memória — evita baixar o GeoJSON múltiplas vezes na mesma execução
_geojson_cache: dict | None = None
_poligono_cache = None


def carregar_geojson_sp() -> dict:
    """Baixa e cacheia o GeoJSON dos municípios de SP."""
    global _geojson_cache
    if _geojson_cache is not None:
        return _geojson_cache

    logger.info("Baixando GeoJSON dos municípios de SP...")
    resp = requests.get(GEOJSON_SP_URL, timeout=30)
    resp.raise_for_status()
    _geojson_cache = resp.json()
    logger.info(f"GeoJSON carregado: {len(_geojson_cache['features'])} municípios.")
    return _geojson_cache


def geojson_grande_sp() -> dict:
    """Retorna GeoJSON filtrado apenas com os municípios da Grande SP."""
    geojson_sp = carregar_geojson_sp()
    features = [
        f for f in geojson_sp["features"]
        if f["properties"]["name"] in MUNICIPIOS_GRANDE_SP
    ]
    encontrados = [f["properties"]["name"] for f in features]
    nao_encontrados = set(MUNICIPIOS_GRANDE_SP) - set(encontrados)
    if nao_encontrados:
        logger.warning(f"Municípios não encontrados no GeoJSON: {nao_encontrados}")
    logger.info(f"Municípios da Grande SP no GeoJSON: {len(features)}")
    return {"type": "FeatureCollection", "features": features}


def poligono_grande_sp():
    """Retorna polígono Shapely unificado da Grande SP (com cache)."""
    global _poligono_cache
    if _poligono_cache is not None:
        return _poligono_cache

    geojson_sp = carregar_geojson_sp()
    poligonos = [
        shape(f["geometry"])
        for f in geojson_sp["features"]
        if f["properties"]["name"] in MUNICIPIOS_GRANDE_SP
    ]
    _poligono_cache = unary_union(poligonos)
    return _poligono_cache


def classificar_clientes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona coluna 'dentro_grande_sp' ao DataFrame.
    Requer colunas lat_final e lng_final.
    """
    df = df.copy()
    poligono = poligono_grande_sp()

    logger.info("Classificando clientes dentro/fora da Grande SP...")
    df["dentro_grande_sp"] = df.apply(
        lambda row: (
            poligono.contains(Point(row["lng_final"], row["lat_final"]))
            if pd.notna(row["lat_final"]) and pd.notna(row["lng_final"])
            else False
        ),
        axis=1,
    )

    dentro = df["dentro_grande_sp"].sum()
    fora   = (~df["dentro_grande_sp"]).sum()
    logger.info(f"Dentro da Grande SP: {dentro:,} | Fora: {fora:,}")
    return df
