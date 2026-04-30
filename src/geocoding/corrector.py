# ============================================================
# src/geocoding/corrector.py
# Corrige automaticamente coordenadas de clientes que aparecem
# "fora da Grande SP" por erro de cadastro no backoffice.
#
# LÓGICA DE DECISÃO (automática, sem intervenção manual):
#
#   Para cada cliente fora da Grande SP:
#
#   1. Já tem registro em correcoes_coordenada?
#      → SIM: usa o resultado salvo. Não reprocessa.
#      → NÃO: vai para o passo 2.
#
#   2. O nome_municipio do cliente está em MUNICIPIOS_GRANDE_SP?
#      → SIM: a coordenada está errada (o cliente deveria estar
#             dentro). Chama a API do Google pelo endereço do
#             cliente para obter coordenada correta.
#             Valida se a coordenada retornada está DENTRO do
#             polígono real da Grande SP (via Shapely) — não
#             apenas dentro do bounding box do Brasil, o que
#             evita aceitar geocodificações erradas da API.
#             Se válida: salva com status='corrigido'.
#             Se inválida: salva com status='confirmado_fora'.
#      → NÃO: o cliente está genuinamente fora da Grande SP.
#             Salva em correcoes_coordenada com status='confirmado_fora'.
#             Não chama a API.
#
# RESULTADO:
#   O DataFrame retornado tem lat_final/lng_final corrigidos
#   para os clientes com status='corrigido', e dentro_grande_sp
#   atualizado para True nesses casos.
#
# REUTILIZAÇÃO:
#   Nas próximas execuções do pipeline, clientes já registrados
#   em correcoes_coordenada são ignorados — só novos clientes
#   fora da Grande SP são processados.
# ============================================================

import logging
import pandas as pd
from shapely.geometry import Point

from config.settings import MUNICIPIOS_GRANDE_SP
from src.database.connection import get_connection
from src.geocoding.geocoder import geocodificar_google, montar_endereco
from src.geo.boundaries import poligono_grande_sp

logger = logging.getLogger(__name__)

# Set para lookup O(1)
_MUNICIPIOS_GRANDE_SP = set(MUNICIPIOS_GRANDE_SP)


# ============================================================
# BANCO — LEITURA E ESCRITA
# ============================================================

def carregar_correcoes() -> dict[str, dict]:
    """
    Retorna dicionário com todos os clientes já registrados
    em correcoes_coordenada.
    Formato: {cod_cliente: {status, lat, lng}}
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT cod_cliente, status, lat_corrigida, lng_corrigida
                FROM correcoes_coordenada
            """)
            return {
                r[0]: {"status": r[1], "lat": r[2], "lng": r[3]}
                for r in cur.fetchall()
            }
    finally:
        conn.close()


def salvar_correcao(cod_cliente: str, status: str,
                    lat: float | None = None, lng: float | None = None,
                    endereco_usado: str | None = None):
    """
    Salva ou atualiza o resultado de correção de um cliente.
    status: 'corrigido' | 'confirmado_fora'
    """
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO correcoes_coordenada
                        (cod_cliente, status, lat_corrigida, lng_corrigida, endereco_usado)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (cod_cliente) DO UPDATE SET
                        status         = EXCLUDED.status,
                        lat_corrigida  = EXCLUDED.lat_corrigida,
                        lng_corrigida  = EXCLUDED.lng_corrigida,
                        endereco_usado = EXCLUDED.endereco_usado,
                        revisado_em    = NOW()
                """, (cod_cliente, status, lat, lng, endereco_usado))
    finally:
        conn.close()


# ============================================================
# VALIDAÇÃO GEOGRÁFICA
# ============================================================

def _dentro_grande_sp(lat: float, lng: float) -> bool:
    """
    Verifica se a coordenada está dentro do polígono real
    da Grande SP usando Shapely.

    Mais restrito que o bounding box do Brasil — evita aceitar
    coordenadas que a API retorna erradas mas que passam no
    filtro simples de latitude/longitude.
    """
    try:
        return poligono_grande_sp().contains(Point(lng, lat))
    except Exception:
        return False


# ============================================================
# CORREÇÃO
# ============================================================

def corrigir_fora_grande_sp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processa clientes fora da Grande SP e corrige coordenadas
    quando o município indica que deveriam estar dentro.

    Parâmetros
    ----------
    df : DataFrame com colunas lat_final, lng_final,
         dentro_grande_sp, nome_municipio e campos de endereço.

    Retorna
    -------
    DataFrame com lat_final, lng_final e dentro_grande_sp
    atualizados para clientes corrigidos.
    """
    df = df.copy()

    df_fora = df[~df["dentro_grande_sp"]].copy()

    if df_fora.empty:
        logger.info("Nenhum cliente fora da Grande SP. Nada a corrigir.")
        return df

    logger.info(f"Clientes fora da Grande SP: {len(df_fora):,}")

    # Carrega correções já feitas para não reprocessar
    ja_corrigidos  = carregar_correcoes()
    novos          = df_fora[~df_fora["cod_cliente"].isin(ja_corrigidos)].copy()
    ja_processados = df_fora[df_fora["cod_cliente"].isin(ja_corrigidos)].copy()

    logger.info(f"  Já registrados: {len(ja_processados):,} | Novos para processar: {len(novos):,}")

    n_corrigidos       = 0
    n_confirmados_fora = 0

    for _, row in novos.iterrows():
        cod       = str(row["cod_cliente"])
        municipio = row.get("nome_municipio", "")

        if municipio in _MUNICIPIOS_GRANDE_SP:
            # Município é da Grande SP → coordenada errada → tenta corrigir via API
            endereco, _ = montar_endereco(row.to_dict())
            lat, lng, _, status_api = geocodificar_google(endereco) if endereco else (None, None, None, "SKIP")

            # Valida se a coordenada retornada está DENTRO do polígono da Grande SP
            # Isso evita aceitar geocodificações erradas da API que passariam
            # no bounding box simples mas apontam para outra região do Brasil
            coordenada_ok = (
                status_api == "OK"
                and lat is not None
                and lng is not None
                and _dentro_grande_sp(lat, lng)
            )

            if coordenada_ok:
                salvar_correcao(cod, "corrigido", lat, lng, endereco)
                logger.info(f"  ✅ Corrigido: {cod} ({municipio}) → ({lat:.6f}, {lng:.6f})")
                n_corrigidos += 1
            else:
                # API não retornou coordenada válida dentro da Grande SP
                salvar_correcao(cod, "confirmado_fora", endereco_usado=endereco)
                logger.warning(
                    f"  ⚠️ API retornou coordenada fora da Grande SP para "
                    f"{cod} ({municipio}) — marcado como confirmado_fora"
                )
                n_confirmados_fora += 1
        else:
            # Município genuinamente fora da Grande SP
            salvar_correcao(cod, "confirmado_fora")
            logger.info(f"  📍 Confirmado fora: {cod} ({municipio})")
            n_confirmados_fora += 1

    logger.info(
        f"\nCorreção concluída: {n_corrigidos} corrigidos | "
        f"{n_confirmados_fora} confirmados fora | "
        f"{len(ja_processados)} já registrados"
    )

    # Aplica correções no DataFrame — recarrega do banco para incluir recém-salvos
    todas_correcoes = carregar_correcoes()

    for idx, row in df[~df["dentro_grande_sp"]].iterrows():
        cod      = str(row["cod_cliente"])
        correcao = todas_correcoes.get(cod)

        if correcao and correcao["status"] == "corrigido":
            df.at[idx, "lat_final"]        = correcao["lat"]
            df.at[idx, "lng_final"]        = correcao["lng"]
            df.at[idx, "dentro_grande_sp"] = True

    total_corrigidos = sum(
        1 for c in todas_correcoes.values() if c["status"] == "corrigido"
    )
    logger.info(f"Coordenadas corrigidas aplicadas no DataFrame: {total_corrigidos}")

    return df