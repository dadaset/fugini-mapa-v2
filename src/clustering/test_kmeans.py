# ============================================================
# src/clustering/test_kmeans.py
# Roda com: python -m src.clustering.test_kmeans
# ============================================================

import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

from src.ingestion.loader import carregar_todas
from src.geocoding.geocoder import geocodificar
from src.geo.boundaries import classificar_clientes
from src.clustering.kmeans import aplicar_kmeans, calcular_metricas

FONTES = [
    (r"data/input/clientes_saneamento_regional_sp_cap_classificacao_comercial.xlsx", "saneamento"),
    (r"data/input/clientes_carteiras_diversos_sp_cap.xlsx", "diversos"),
]

if __name__ == "__main__":
    df = carregar_todas(FONTES)
    df = geocodificar(df)
    df = classificar_clientes(df)
    df, kmeans, mapa_area = aplicar_kmeans(df)
    metricas = calcular_metricas(df, kmeans, mapa_area)

    print(f"\n{'='*50}")
    print("MÉTRICAS POR ÁREA")
    print(f"{'='*50}")
    print(metricas.to_string())
    print(f"\nDistribuição:")
    print(df["area_nome"].value_counts().sort_index().to_string())
