# ============================================================
# src/database/setup.py
# Cria todas as tabelas do projeto no banco mapa_clientes.
# Idempotente — pode rodar várias vezes sem problema.
# ============================================================

import logging
from src.database.connection import get_connection

logger = logging.getLogger(__name__)


DDL = """

-- ----------------------------------------------------------
-- MUNICÍPIOS
-- Tabela de referência cod_ibge -> nome_municipio.
-- Carregada uma vez via src/database/load_municipios.py,
-- lendo direto do servidor \\192.168.0.226\pdi\in\full\totvs_municipio.csv
-- Usada no ingestion para resolver nome_municipio de cada cliente.
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS municipios (
    cod_ibge        INTEGER PRIMARY KEY,
    nome_municipio  TEXT NOT NULL,
    uf              TEXT NOT NULL,
    populacao       INTEGER
);

-- ----------------------------------------------------------
-- MAPEAMENTO DE COLUNAS POR FONTE
-- Resolve o problema de planilhas do comercial chegarem com
-- nomes de colunas diferentes a cada envio.
--
-- Cada linha mapeia:
--   col_original  → nome como veio na planilha (ex: 'OBSERVAÇÃO GR')
--   col_canonical → nome padrão interno       (ex: 'observacao')
--
-- Quando chegar planilha nova com colunas diferentes:
--   INSERT INTO colunas_mapeamento (fonte, col_original, col_canonical)
--   VALUES ('nova_fonte', 'COLUNA NOVA', 'nome_canonico');
-- Sem tocar no código.
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS colunas_mapeamento (
    id            SERIAL PRIMARY KEY,
    fonte         TEXT NOT NULL,
    col_original  TEXT NOT NULL,
    col_canonical TEXT NOT NULL,
    criado_em     TIMESTAMP DEFAULT NOW(),
    UNIQUE (fonte, col_original)
);

-- ----------------------------------------------------------
-- CHECKPOINT DE GEOCODIFICAÇÃO
-- Evita chamar a API do Google Maps repetidamente para o
-- mesmo cliente. Cada cliente processado fica aqui.
--
-- Fluxo:
--   1. Pipeline verifica se cod_cliente já existe aqui
--   2. Se sim → usa resultado salvo, não chama API
--   3. Se não → chama API, salva resultado aqui
--
-- Substitui o CSV de checkpoint que existia no projeto original.
-- ----------------------------------------------------------
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

-- ----------------------------------------------------------
-- CLIENTES PROCESSADOS
-- Resultado final do pipeline após geocodificação e clustering.
-- Recriada a cada execução (TRUNCATE + INSERT) para refletir
-- sempre o estado mais recente das planilhas.
-- ----------------------------------------------------------
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

-- ----------------------------------------------------------
-- CORREÇÕES DE COORDENADA
-- Resolve clientes que aparecem "fora da Grande SP" por erro
-- de coordenada cadastrada no backoffice.
--
-- Lógica de decisão automática (src/geocoding/corrector.py):
--   - Se nome_municipio está em MUNICIPIOS_GRANDE_SP
--     → coordenada está errada → chama API do Google pelo
--       endereço do cliente → salva coordenada corrigida
--       com status = 'corrigido'
--   - Se nome_municipio NÃO está em MUNICIPIOS_GRANDE_SP
--     → cliente genuinamente fora → salva com
--       status = 'confirmado_fora', sem chamar API
--
-- Nas próximas execuções do pipeline, clientes com registro
-- aqui não são reprocessados — usa o resultado já salvo.
-- Isso evita chamadas desnecessárias à API do Google e
-- mantém um histórico auditável de todas as correções feitas.
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS correcoes_coordenada (
    cod_cliente    TEXT PRIMARY KEY,
    status         TEXT NOT NULL,        -- 'corrigido' | 'confirmado_fora'
    lat_corrigida  DOUBLE PRECISION,     -- preenchido só quando status = 'corrigido'
    lng_corrigida  DOUBLE PRECISION,     -- preenchido só quando status = 'corrigido'
    endereco_usado TEXT,                 -- endereço enviado para a API do Google
    revisado_em    TIMESTAMP DEFAULT NOW()
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
    """
    Insere mapeamento padrão de colunas para as fontes conhecidas.
    Seguro para rodar múltiplas vezes — ignora duplicatas.
    """
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