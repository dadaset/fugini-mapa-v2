# ============================================================
# src/database/load_municipios.py
# Carrega totvs_municipio.csv direto do servidor para o banco.
# Idempotente — pode rodar várias vezes sem duplicar dados.
# Roda com: python -m src.database.load_municipios
# ============================================================

import logging
import pandas as pd
from src.database.connection import get_connection

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)

PATH_MUNICIPIO = r"\\192.168.0.226\pdi\in\full\totvs_municipio.csv"


def carregar_municipios():
    logger.info(f"Lendo: {PATH_MUNICIPIO}")
    df = pd.read_csv(PATH_MUNICIPIO, sep=";", encoding="latin1")
    df.columns = df.columns.str.strip()

    df = df.rename(columns={
        "cod-ibge":       "cod_ibge",
        "nome-municipio": "nome_municipio",
        "uf":             "uf",
        "populacao":      "populacao",
    })

    df["cod_ibge"]   = pd.to_numeric(df["cod_ibge"],   errors="coerce")
    df["populacao"]  = pd.to_numeric(df["populacao"],  errors="coerce")
    df = df.dropna(subset=["cod_ibge"])
    df["cod_ibge"] = df["cod_ibge"].astype(int)

    logger.info(f"{len(df):,} municípios carregados do CSV.")

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO municipios (cod_ibge, nome_municipio, uf, populacao)
                    VALUES (%(cod_ibge)s, %(nome_municipio)s, %(uf)s, %(populacao)s)
                    ON CONFLICT (cod_ibge) DO UPDATE SET
                        nome_municipio = EXCLUDED.nome_municipio,
                        uf             = EXCLUDED.uf,
                        populacao      = EXCLUDED.populacao
                    """,
                    df.to_dict("records"),
                )
        logger.info(f"✅ {len(df):,} municípios inseridos/atualizados no banco.")
        print(f"✅ {len(df):,} municípios carregados com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao carregar municípios: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    carregar_municipios()
