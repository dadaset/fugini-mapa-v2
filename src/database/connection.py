# ============================================================
# src/database/connection.py
# Gerencia conexão com o PostgreSQL.
# ============================================================

import psycopg2
import psycopg2.extras
import logging
from config.settings import PG_HOST, PG_PORT, PG_DBNAME, PG_USER, PG_PASSWORD

logger = logging.getLogger(__name__)


def get_connection():
    """Retorna uma conexão aberta com o banco mapa_clientes."""
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DBNAME,
            user=PG_USER,
            password=PG_PASSWORD,
        )
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Erro ao conectar no PostgreSQL: {e}")
        raise


def get_cursor(conn):
    """Retorna um cursor com retorno em dicionário (RealDictCursor)."""
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
