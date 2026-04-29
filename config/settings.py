# ============================================================
# config/settings.py
# Ponto único de configuração do projeto.
# Todas as outras partes do código importam daqui.
# ============================================================

from pathlib import Path
from dotenv import load_dotenv
import os

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# ------------------------------------------------------------
# GOOGLE MAPS
# ------------------------------------------------------------
GOOGLE_API_KEY: str = os.environ["GOOGLE_API_KEY"]

# ------------------------------------------------------------
# POSTGRESQL
# ------------------------------------------------------------
PG_HOST:     str = os.getenv("PG_HOST", "localhost")
PG_PORT:     int = int(os.getenv("PG_PORT", 5432))
PG_DBNAME:   str = os.environ["PG_DBNAME"]
PG_USER:     str = os.environ["PG_USER"]
PG_PASSWORD: str = os.environ["PG_PASSWORD"]

# ------------------------------------------------------------
# GITHUB
# ------------------------------------------------------------
GITHUB_TOKEN: str = os.environ["GITHUB_TOKEN"]
GITHUB_REPO:  str = os.environ["GITHUB_REPO"]

# ------------------------------------------------------------
# CLUSTERING
# ------------------------------------------------------------
N_CLUSTERS:   int = 4
KMEANS_SEED:  int = 42
KMEANS_NINIT: int = 10

# ------------------------------------------------------------
# GEOCODIFICAÇÃO
# ------------------------------------------------------------
GEOCODING_BATCH_SIZE:            int   = 500
GEOCODING_MAX_WORKERS:           int   = 10
GEOCODING_SLEEP_BETWEEN_BATCHES: float = 2.0

GEO_LAT_MIN: float = -35.0
GEO_LAT_MAX: float =   6.0
GEO_LNG_MIN: float = -75.0
GEO_LNG_MAX: float = -30.0

# ------------------------------------------------------------
# MUNICÍPIOS DA GRANDE SP
# ------------------------------------------------------------
MUNICIPIOS_GRANDE_SP: list[str] = [
    "São Paulo",
    "Caieiras", "Cajamar", "Francisco Morato", "Franco da Rocha", "Mairiporã",
    "Arujá", "Biritiba-Mirim", "Ferraz de Vasconcelos", "Guararema", "Guarulhos",
    "Itaquaquecetuba", "Mogi das Cruzes", "Poá", "Salesópolis", "Santa Isabel", "Suzano",
    "Diadema", "Mauá", "Ribeirão Pires", "Rio Grande da Serra",
    "Santo André", "São Bernardo do Campo", "São Caetano do Sul",
    "Cotia", "Embu das Artes", "Embu", "Embu-Guaçu", "Itapecerica da Serra",
    "Juquitiba", "São Lourenço da Serra", "Taboão da Serra", "Vargem Grande Paulista",
    "Barueri", "Carapicuíba", "Itapevi", "Jandira",
    "Osasco", "Pirapora do Bom Jesus", "Santana de Parnaíba",
]

GEOJSON_SP_URL: str = (
    "https://raw.githubusercontent.com/tbrugz/geodata-br/"
    "master/geojson/geojs-35-mun.json"
)

# ------------------------------------------------------------
# CORES POR ÁREA
# ------------------------------------------------------------
CORES_AREAS: dict[str, dict[str, str]] = {
    "Área 1": {"marker": "#e74c3c", "fill": "#f1948a"},
    "Área 2": {"marker": "#2980b9", "fill": "#7fb3d3"},
    "Área 3": {"marker": "#27ae60", "fill": "#82e0aa"},
    "Área 4": {"marker": "#8e44ad", "fill": "#c39bd3"},
}

COR_FORA_GRANDE_SP: dict[str, str] = {"marker": "#7f8c8d", "fill": "#bdc3c7"}

# ------------------------------------------------------------
# USUÁRIOS DO MAPA
# ------------------------------------------------------------
USUARIOS_MAPA: dict[str, dict[str, str]] = {
    "vendedor1": {"senha": "fugini@area1", "arquivo": "area1.html"},
    "vendedor2": {"senha": "fugini@area2", "arquivo": "area2.html"},
    "vendedor3": {"senha": "fugini@area3", "arquivo": "area3.html"},
    "vendedor4": {"senha": "fugini@area4", "arquivo": "area4.html"},
    "master":    {"senha": "fugini@master", "arquivo": "master.html"},
}
