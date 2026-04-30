# ============================================================
# pipeline.py
# Orquestra todas as etapas do projeto em ordem.
#
# Uso:
#   python pipeline.py              → completo com criptografia e publicação
#   python pipeline.py --no-crypt  → sem criptografia (teste local)
#   python pipeline.py --no-publish → sem publicar no GitHub
# ============================================================

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

from src.ingestion.loader        import carregar_todas
from src.geocoding.geocoder      import geocodificar
from src.geocoding.corrector     import corrigir_fora_grande_sp
from src.geo.boundaries          import classificar_clientes, geojson_grande_sp
from src.clustering.kmeans       import aplicar_kmeans, calcular_metricas
from src.mapping.builder         import exportar_mapas
from src.publishing.github_pages import publicar

# ------------------------------------------------------------
# FONTES DE DADOS
# Adicione novas planilhas aqui quando o comercial enviar.
# Se vierem com colunas diferentes, insira o mapeamento em
# colunas_mapeamento no banco — sem alterar o código.
# ------------------------------------------------------------
FONTES = [
    (r"data/input/clientes_saneamento_regional_sp_cap_classificacao_comercial.xlsx", "saneamento"),
    (r"data/input/clientes_carteiras_diversos_sp_cap.xlsx",                          "diversos"),
]


def run(criptografar: bool = True, publicar_github: bool = True):
    logger.info("=" * 60)
    logger.info("PIPELINE INICIADO")
    logger.info("=" * 60)

    # 1. Ingestion
    logger.info("\n[1/7] Carregando planilhas...")
    df = carregar_todas(FONTES)

    # 2. Geocodificação
    logger.info("\n[2/7] Geocodificando clientes...")
    df = geocodificar(df)

    # 3. Classificação Grande SP
    logger.info("\n[3/7] Classificando dentro/fora da Grande SP...")
    df = classificar_clientes(df)
    geojson = geojson_grande_sp()

    # 4. Correção de coordenadas
    # Clientes fora da Grande SP cujo município indica que deveriam
    # estar dentro têm coordenada corrigida via API do Google.
    # Clientes genuinamente fora são confirmados e não reprocessados.
    logger.info("\n[4/7] Corrigindo coordenadas fora da Grande SP...")
    df = corrigir_fora_grande_sp(df)

    # 5. Clustering
    logger.info("\n[5/7] Aplicando K-Means...")
    df, kmeans, mapa_area = aplicar_kmeans(df)
    metricas = calcular_metricas(df, kmeans, mapa_area)
    logger.info(f"\n{metricas.to_string()}")

    # 6. Geração dos mapas
    logger.info(f"\n[6/7] Gerando HTMLs (criptografar={criptografar})...")
    arquivos = exportar_mapas(df, geojson, criptografar=criptografar)

    # 7. Publicação
    if publicar_github:
        logger.info("\n[7/7] Publicando no GitHub Pages...")
        url = publicar()
        logger.info(f"🌐 {url}")
    else:
        logger.info("\n[7/7] Publicação pulada (--no-publish).")
        url = None

    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE CONCLUÍDO")
    logger.info("=" * 60)

    return df, arquivos, url


if __name__ == "__main__":
    criptografar    = "--no-crypt"    not in sys.argv
    publicar_github = "--no-publish"  not in sys.argv
    run(criptografar=criptografar, publicar_github=publicar_github)