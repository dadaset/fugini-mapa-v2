# ============================================================
# src/ingestion/test_loader.py
# Testa o carregamento e normalização das planilhas.
# Roda com: python -m src.ingestion.test_loader
# ============================================================

import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

from src.ingestion.loader import carregar_todas

FONTES = [
    (r"data/input/clientes_saneamento_regional_sp_cap_classificacao_comercial.xlsx", "saneamento"),
    (r"data/input/clientes_carteiras_diversos_sp_cap.xlsx", "diversos"),
]

if __name__ == "__main__":
    df = carregar_todas(FONTES)

    print(f"\n{'='*50}")
    print(f"RESULTADO FINAL")
    print(f"{'='*50}")
    print(f"Total de clientes disponíveis: {len(df):,}")
    print(f"\nPor fonte:")
    print(df["fonte"].value_counts().to_string())
    print(f"\nColunas: {df.columns.tolist()}")
    print(f"\nPrimeiras linhas:")
    print(df.head(3).to_string())
