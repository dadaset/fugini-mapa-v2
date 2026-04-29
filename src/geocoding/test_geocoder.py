# ============================================================
# src/geocoding/test_geocoder.py
# Testa geocodificação com os clientes carregados.
# Roda com: python -m src.geocoding.test_geocoder
# ============================================================

import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

from src.ingestion.loader import carregar_todas
from src.geocoding.geocoder import geocodificar

FONTES = [
    (r"data/input/clientes_saneamento_regional_sp_cap_classificacao_comercial.xlsx", "saneamento"),
    (r"data/input/clientes_carteiras_diversos_sp_cap.xlsx", "diversos"),
]

if __name__ == "__main__":
    print("Carregando clientes...")
    df = carregar_todas(FONTES)

    print(f"\nGeocodificando {len(df):,} clientes...")
    df = geocodificar(df)

    print(f"\n{'='*50}")
    print(f"RESULTADO GEOCODIFICAÇÃO")
    print(f"{'='*50}")
    print(f"Total:              {len(df):,}")
    print(f"Com geo válida:     {df['geo_valida_final'].sum():,}")
    print(f"Sem geo:            {(~df['geo_valida_final']).sum():,}")
    print(f"\nPrimeiras linhas com coordenada:")
    print(df[df["geo_valida_final"]][["cod_cliente", "nome_cliente", "lat_final", "lng_final"]].head(5).to_string())
