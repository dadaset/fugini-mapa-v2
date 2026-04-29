# ============================================================
# src/mapping/test_builder.py
# Gera os HTMLs SEM criptografia para validar visualmente.
# Roda com: python -m src.mapping.test_builder
# ============================================================

import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

from src.ingestion.loader import carregar_todas
from src.geocoding.geocoder import geocodificar
from src.geo.boundaries import classificar_clientes, geojson_grande_sp
from src.clustering.kmeans import aplicar_kmeans
from src.mapping.builder import exportar_mapas

FONTES = [
    (r"data/input/clientes_saneamento_regional_sp_cap_classificacao_comercial.xlsx", "saneamento"),
    (r"data/input/clientes_carteiras_diversos_sp_cap.xlsx", "diversos"),
]

if __name__ == "__main__":
    df = carregar_todas(FONTES)
    df = geocodificar(df)
    df = classificar_clientes(df)
    df, kmeans, mapa_area = aplicar_kmeans(df)
    geojson = geojson_grande_sp()

    # criptografar=False para testar sem precisar do npm/pagecrypt
    arquivos = exportar_mapas(df, geojson, criptografar=False)

    print(f"\n{'='*50}")
    print("HTMLs gerados em data/output/:")
    for nome, path in arquivos.items():
        print(f"  {nome}: {path}")
    print("\nAbra o master.html no navegador para validar.")
