# ============================================================
# src/clustering/kmeans.py
# Aplica K-Means nos clientes da Grande SP e calcula métricas.
# ============================================================

import logging
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import haversine_distances

from config.settings import N_CLUSTERS, KMEANS_SEED, KMEANS_NINIT, CORES_AREAS

logger = logging.getLogger(__name__)


def aplicar_kmeans(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica K-Means nos clientes dentro da Grande SP.
    Ordena clusters de oeste para leste (por longitude do centroide).
    Adiciona coluna 'area_nome' ao DataFrame completo.

    Parâmetros
    ----------
    df : DataFrame com colunas lat_final, lng_final, dentro_grande_sp

    Retorna
    -------
    DataFrame com coluna area_nome preenchida para clientes da Grande SP.
    """
    df = df.copy()
    df["area_nome"] = None

    df_grande_sp = df[df["dentro_grande_sp"]].copy()

    if len(df_grande_sp) < N_CLUSTERS:
        raise ValueError(
            f"Clientes dentro da Grande SP ({len(df_grande_sp)}) "
            f"insuficientes para {N_CLUSTERS} clusters."
        )

    coords = df_grande_sp[["lat_final", "lng_final"]].values

    kmeans = KMeans(
        n_clusters=N_CLUSTERS,
        random_state=KMEANS_SEED,
        n_init=KMEANS_NINIT,
    )
    df_grande_sp["cluster"] = kmeans.fit_predict(coords)

    # Ordena de oeste para leste pela longitude do centroide
    centroids  = kmeans.cluster_centers_
    ordem      = np.argsort(centroids[:, 1])
    areas      = list(CORES_AREAS.keys())
    mapa_area  = {int(cluster_id): areas[i] for i, cluster_id in enumerate(ordem)}

    df_grande_sp["area_nome"] = df_grande_sp["cluster"].map(mapa_area)
    df.loc[df_grande_sp.index, "area_nome"] = df_grande_sp["area_nome"]

    logger.info("Distribuição por área:")
    for area in areas:
        n = (df_grande_sp["area_nome"] == area).sum()
        logger.info(f"  {area}: {n:,} clientes")

    return df, kmeans, mapa_area


def calcular_metricas(df: pd.DataFrame, kmeans: KMeans, mapa_area: dict) -> pd.DataFrame:
    """
    Calcula métricas por área: clientes, média de crédito, CV e distância máxima ao centroide.

    Retorna DataFrame com as métricas.
    """
    df_grande_sp = df[df["dentro_grande_sp"] & df["area_nome"].notna()].copy()

    resumo = df_grande_sp.groupby("area_nome")["limite_disp"].agg(
        clientes="count",
        media="mean",
        std="std",
    )
    resumo["cv_pct"] = (resumo["std"] / resumo["media"] * 100).round(1)

    dist_max = {}
    for cluster_id, area_nome in mapa_area.items():
        clientes_area = df_grande_sp[df_grande_sp["area_nome"] == area_nome]
        if len(clientes_area) == 0:
            dist_max[area_nome] = None
            continue
        centroide     = kmeans.cluster_centers_[cluster_id]
        coords_rad    = np.radians(clientes_area[["lat_final", "lng_final"]].values)
        centroide_rad = np.radians(centroide).reshape(1, -1)
        distancias_km = haversine_distances(coords_rad, centroide_rad) * 6371
        dist_max[area_nome] = round(float(distancias_km.max()), 1)

    resumo["dist_max_km"] = resumo.index.map(dist_max)
    resumo = resumo.drop(columns="std")
    resumo.columns = ["Clientes", "Média Crédito (R$)", "CV (%)", "Dist. Máx. (km)"]

    logger.info("\nMétricas por área:")
    logger.info(f"\n{resumo.sort_index().to_string()}")

    return resumo.sort_index()
