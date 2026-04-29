# ============================================================
# src/geo/test_boundaries.py
# Roda com: python -m src.geo.test_boundaries
# ============================================================

import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

from src.ingestion.loader import carregar_todas
from src.geocoding.geocoder import geocodificar
from src.geo.boundaries import classificar_clientes

FONTES = [
    (r"data/input/clientes_saneamento_regional_sp_cap_classificacao_comercial.xlsx", "saneamento"),
    (r"data/input/clientes_carteiras_diversos_sp_cap.xlsx", "diversos"),
]

if __name__ == "__main__":
    df = carregar_todas(FONTES)
    df = geocodificar(df)
    df = classificar_clientes(df)

    print(f"\n{'='*50}")
    print(f"RESULTADO CLASSIFICAÇÃO")
    print(f"{'='*50}")
    print(f"Dentro da Grande SP: {df['dentro_grande_sp'].sum():,}")
    print(f"Fora da Grande SP:   {(~df['dentro_grande_sp']).sum():,}")
