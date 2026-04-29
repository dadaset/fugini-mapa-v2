# ============================================================
# src/database/setup.py
# Cria as tabelas do projeto no banco mapa_clientes.
# Idempotente — pode rodar várias vezes sem problema.
# ============================================================

import logging
from src.database.connection import get_connection

logger = logging.getLogger(__name__)


DDL = """

CREATE TABLE IF NOT EXISTS municipios (
    cod_ibge        INTEGER PRIMARY KEY,
    nome_municipio  TEXT NOT NULL,
    uf              TEXT NOT NULL,
    populacao       INTEGER
);

CREATE TABLE IF NOT EXISTS colunas_mapeamento (
    id            SERIAL PRIMARY KEY,
    fonte         TEXT NOT NULL,
    col_original  TEXT NOT NULL,
    col_canonical TEXT NOT NULL,
    criado_em     TIMESTAMP DEFAULT NOW(),
    UNIQUE (fonte, col_original)
);

CREATE TABLE IF NOT EXISTS geocodificacao_checkpoint (
    cod_cliente      TEXT PRIMARY KEY,
    lat_google       DOUBLE PRECISION,
    lng_google       DOUBLE PRECISION,
    tipo_localizacao TEXT,
    status           TEXT,
    valido           BOOLEAN,
    nivel            INTEGER,
    processado_em    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS clientes (
    cod_cliente      TEXT PRIMARY KEY,
    nome_cliente     TEXT,
    nome_municipio   TEXT,
    uf               TEXT,
    lat_final        DOUBLE PRECISION,
    lng_final        DOUBLE PRECISION,
    limite_disp      DOUBLE PRECISION,
    fonte            TEXT,
    dentro_grande_sp BOOLEAN,
    area_nome        TEXT,
    atualizado_em    TIMESTAMP DEFAULT NOW()
);

"""


def criar_tabelas():
    """Cria todas as tabelas do projeto. Seguro para rodar múltiplas vezes."""
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(DDL)
        logger.info("Tabelas criadas/verificadas com sucesso.")
        print("✅ Tabelas criadas/verificadas com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao criar tabelas: {e}")
        raise
    finally:
        conn.close()


def popular_mapeamento_inicial():
    """Insere mapeamento padrão de colunas. Seguro para rodar múltiplas vezes."""
    mapeamentos = [
        ("saneamento", "OBSERVAÇÃO",    "observacao"),
        ("saneamento", "cod-cliente",   "cod_cliente"),
        ("saneamento", "nome-cliente",  "nome_cliente"),
        ("saneamento", "limite-disp",   "limite_disp"),
        ("saneamento", "lat-cliente",   "lat_totvs"),
        ("saneamento", "long-cliente",  "lng_totvs"),
        ("saneamento", "endereco",      "endereco"),
        ("saneamento", "bairro",        "bairro"),
        ("saneamento", "cep",           "cep"),
        ("saneamento", "cod-ibge",      "cod_ibge"),
        ("saneamento", "uf",            "uf"),

        ("diversos",   "OBSERVAÇÃO GR", "observacao"),
        ("diversos",   "cod-cliente",   "cod_cliente"),
        ("diversos",   "nome-cliente",  "nome_cliente"),
        ("diversos",   "limite-disp",   "limite_disp"),
        ("diversos",   "lat-cliente",   "lat_totvs"),
        ("diversos",   "long-cliente",  "lng_totvs"),
        ("diversos",   "endereco",      "endereco"),
        ("diversos",   "bairro",        "bairro"),
        ("diversos",   "cep",           "cep"),
        ("diversos",   "cod-ibge",      "cod_ibge"),
        ("diversos",   "uf",            "uf"),
    ]

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO colunas_mapeamento (fonte, col_original, col_canonical)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (fonte, col_original) DO NOTHING
                    """,
                    mapeamentos,
                )
        logger.info(f"Mapeamento inserido ({len(mapeamentos)} entradas).")
        print(f"✅ Mapeamento inserido ({len(mapeamentos)} entradas).")
    except Exception as e:
        logger.error(f"Erro ao popular mapeamento: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    criar_tabelas()
    popular_mapeamento_inicial()
