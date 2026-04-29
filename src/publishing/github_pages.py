# ============================================================
# src/publishing/github_pages.py
# Publica os HTMLs gerados no GitHub Pages.
# Faz commit automático com timestamp no branch gh-pages.
# ============================================================

import logging
import base64
from datetime import datetime
from pathlib import Path

from github import Github, GithubException
from config.settings import GITHUB_TOKEN, GITHUB_REPO

logger = logging.getLogger(__name__)

ARQUIVOS_PUBLICAR = [
    "index.html",
    "master.html",
    "area1.html",
    "area2.html",
    "area3.html",
    "area4.html",
]

OUTPUT_DIR = Path("data/output")
BRANCH     = "gh-pages"


def publicar():
    """
    Publica os HTMLs em data/output/ no branch gh-pages do repositório.
    Cria o branch se não existir.
    Atualiza arquivos existentes ou cria novos.
    """
    logger.info(f"Conectando ao GitHub: {GITHUB_REPO}")
    g    = Github(GITHUB_TOKEN)
    repo = g.get_repo(GITHUB_REPO)

    # Garante que o branch gh-pages existe
    _garantir_branch(repo)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    erros     = []

    for nome_arquivo in ARQUIVOS_PUBLICAR:
        path_local = OUTPUT_DIR / nome_arquivo
        if not path_local.exists():
            logger.warning(f"  Arquivo não encontrado, pulando: {path_local}")
            continue

        conteudo = path_local.read_bytes()
        logger.info(f"  Publicando {nome_arquivo} ({len(conteudo)/1024:.1f} KB)...")

        try:
            # Verifica se arquivo já existe no repo
            try:
                arquivo_remoto = repo.get_contents(nome_arquivo, ref=BRANCH)
                sha = arquivo_remoto.sha
                repo.update_file(
                    path=nome_arquivo,
                    message=f"chore: atualiza {nome_arquivo} [{timestamp}]",
                    content=conteudo,
                    sha=sha,
                    branch=BRANCH,
                )
                logger.info(f"  ✅ {nome_arquivo} atualizado")
            except GithubException as e:
                if e.status == 404:
                    repo.create_file(
                        path=nome_arquivo,
                        message=f"chore: cria {nome_arquivo} [{timestamp}]",
                        content=conteudo,
                        branch=BRANCH,
                    )
                    logger.info(f"  ✅ {nome_arquivo} criado")
                else:
                    raise

        except Exception as e:
            logger.error(f"  ❌ Erro ao publicar {nome_arquivo}: {e}")
            erros.append(nome_arquivo)

    if erros:
        raise RuntimeError(f"Falha ao publicar: {erros}")

    url = f"https://{repo.owner.login}.github.io/{repo.name}/"
    logger.info(f"\n🌐 Publicado em: {url}")
    return url


def _garantir_branch(repo):
    """Cria o branch gh-pages se não existir."""
    try:
        repo.get_branch(BRANCH)
        logger.info(f"Branch '{BRANCH}' encontrado.")
    except GithubException as e:
        if e.status == 404:
            logger.info(f"Branch '{BRANCH}' não existe. Criando...")
            sb = repo.get_branch(repo.default_branch)
            repo.create_git_ref(
                ref=f"refs/heads/{BRANCH}",
                sha=sb.commit.sha,
            )
            logger.info(f"Branch '{BRANCH}' criado.")
        else:
            raise
