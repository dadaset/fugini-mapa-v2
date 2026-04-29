# ============================================================
# src/mapping/builder.py
# Monta mapas Folium e exporta HTMLs.
# Criptografia delegada para src/mapping/crypto.py.
# ============================================================

import logging
import json
from pathlib import Path

import folium
import folium.plugins
import pandas as pd

from config.settings import CORES_AREAS, COR_FORA_GRANDE_SP, USUARIOS_MAPA
from src.mapping.crypto import criptografar_html

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("data/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# MAPA FOLIUM
# ============================================================

def montar_mapa(
    df_mapa: pd.DataFrame,
    geojson_grande_sp: dict,
    areas_visiveis: list[str] | None = None,
) -> folium.Map:
    """
    Monta mapa Folium com marcadores por área.

    areas_visiveis=None  → todas as áreas + fora + heatmap (master)
    areas_visiveis=[...] → só as áreas listadas
    """
    mapa = folium.Map(
        location=[-23.55, -46.63],
        zoom_start=10,
        tiles="CartoDB positron",
    )

    folium.GeoJson(
        geojson_grande_sp,
        name="Limites dos municípios",
        style_function=lambda x: {
            "fillColor":  "#f4a522",
            "color":      "#d4821a",
            "weight":      2,
            "fillOpacity": 0.15,
        },
        tooltip=folium.GeoJsonTooltip(fields=["name"], aliases=["Município:"]),
        show=True,
    ).add_to(mapa)

    cores_iter = (
        CORES_AREAS if areas_visiveis is None
        else {k: v for k, v in CORES_AREAS.items() if k in areas_visiveis}
    )

    for area_nome, cores in cores_iter.items():
        fg = folium.FeatureGroup(name=area_nome, show=True)
        clientes_area = df_mapa[df_mapa["area_nome"] == area_nome]

        for _, row in clientes_area.iterrows():
            nome    = row.get("nome_cliente",   "N/D")
            cod     = row.get("cod_cliente",    "N/D")
            cidade  = row.get("nome_municipio", "N/D")
            credito = row.get("limite_disp", 0) or 0

            popup_html = f"""
            <div style="font-family:Arial;font-size:12px;min-width:180px">
                <b>{nome}</b><br>
                <span style="color:#666">Cód: {cod}</span><br>
                <span style="color:#666">{cidade}</span><br>
                <span style="color:#666">Crédito disp.: R$ {credito:,.2f}</span><br>
                <span style="color:{cores['marker']}"><b>{area_nome}</b></span>
            </div>
            """
            folium.CircleMarker(
                location=[row["lat_final"], row["lng_final"]],
                radius=6,
                color=cores["marker"],
                fill=True,
                fill_color=cores["fill"],
                fill_opacity=0.85,
                popup=folium.Popup(popup_html, max_width=250),
                tooltip=folium.Tooltip(f"{nome} ({area_nome})"),
            ).add_to(fg)

        fg.add_to(mapa)

    # Fora da Grande SP — só no master
    if areas_visiveis is None:
        fg_fora = folium.FeatureGroup(name="Fora da Grande SP", show=True)
        for _, row in df_mapa[df_mapa["area_nome"].isna()].iterrows():
            nome   = row.get("nome_cliente",   "N/D")
            cod    = row.get("cod_cliente",    "N/D")
            cidade = row.get("nome_municipio", "N/D")
            popup_html = f"""
            <div style="font-family:Arial;font-size:12px;min-width:180px">
                <b>{nome}</b><br>
                <span style="color:#666">Cód: {cod}</span><br>
                <span style="color:#666">{cidade}</span><br>
                <span style="color:#7f8c8d"><b>Fora da Grande SP</b></span>
            </div>
            """
            folium.CircleMarker(
                location=[row["lat_final"], row["lng_final"]],
                radius=5,
                color=COR_FORA_GRANDE_SP["marker"],
                fill=True,
                fill_color=COR_FORA_GRANDE_SP["fill"],
                fill_opacity=0.7,
                popup=folium.Popup(popup_html, max_width=250),
                tooltip=folium.Tooltip(f"{nome} (Fora da Grande SP)"),
            ).add_to(fg_fora)
        fg_fora.add_to(mapa)

    # Heatmap — só no master
    if areas_visiveis is None:
        fg_heat = folium.FeatureGroup(name="Heatmap Crédito Disponível", show=False)
        heat_data = [
            [row["lat_final"], row["lng_final"], row["limite_disp"]]
            for _, row in df_mapa[
                df_mapa["limite_disp"].notna() & (df_mapa["limite_disp"] > 0)
            ].iterrows()
        ]
        folium.plugins.HeatMap(
            heat_data, min_opacity=0.3, radius=20, blur=15, max_zoom=13
        ).add_to(fg_heat)
        fg_heat.add_to(mapa)

    folium.LayerControl(collapsed=False).add_to(mapa)
    return mapa


# ============================================================
# INDEX.HTML
# ============================================================

def gerar_index_html() -> str:
    """Gera o HTML da página de login que redireciona para o arquivo correto."""
    # Só expõe o mapeamento usuário → arquivo, SEM senhas
    usuarios_js = json.dumps(
        {u: d["arquivo"] for u, d in USUARIOS_MAPA.items()},
        ensure_ascii=False,
        indent=2,
    )
    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Fugini — Mapa de Clientes</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: 'Segoe UI', sans-serif;
      background: #f0f2f5;
      display: flex; align-items: center; justify-content: center;
      min-height: 100vh;
    }}
    .card {{
      background: white; border-radius: 12px; padding: 40px;
      width: 100%; max-width: 380px;
      box-shadow: 0 4px 24px rgba(0,0,0,0.10);
    }}
    .logo {{ text-align: center; margin-bottom: 28px; }}
    .logo h1 {{ font-size: 22px; color: #1a1a2e; font-weight: 700; }}
    .logo p  {{ font-size: 13px; color: #888; margin-top: 4px; }}
    label {{ display: block; font-size: 13px; font-weight: 600; color: #444; margin-bottom: 6px; }}
    input {{
      width: 100%; padding: 10px 14px;
      border: 1.5px solid #ddd; border-radius: 8px;
      font-size: 14px; margin-bottom: 16px;
      outline: none; transition: border 0.2s;
    }}
    input:focus {{ border-color: #e74c3c; }}
    button {{
      width: 100%; padding: 12px;
      background: #e74c3c; color: white;
      border: none; border-radius: 8px;
      font-size: 15px; font-weight: 600;
      cursor: pointer; transition: background 0.2s;
    }}
    button:hover {{ background: #c0392b; }}
    .erro {{
      display: none; margin-top: 14px; padding: 10px 14px;
      background: #fdecea; border-radius: 8px;
      color: #c0392b; font-size: 13px; text-align: center;
    }}
  </style>
</head>
<body>
  <div class="card">
    <div class="logo">
      <h1>Fugini Alimentos</h1>
      <p>Mapa de Clientes Disponíveis</p>
    </div>
    <label for="usuario">Usuário</label>
    <input type="text" id="usuario" placeholder="seu usuário" autocomplete="username">
    <label for="senha">Senha</label>
    <input type="password" id="senha" placeholder="sua senha" autocomplete="current-password">
    <button onclick="entrar()">Entrar</button>
    <div class="erro" id="erro">Usuário ou senha incorretos.</div>
  </div>
  <script>
    // Apenas mapeamento usuário → arquivo. Nenhuma senha aqui.
    const ARQUIVOS = {usuarios_js};

    function b64ToBytes(b64) {{
      const bin = atob(b64);
      const arr = new Uint8Array(bin.length);
      for (let i = 0; i < bin.length; i++) arr[i] = bin.charCodeAt(i);
      return arr;
    }}

    async function descriptografarEExibir(arquivo, senha) {{
      const resp   = await fetch(arquivo);
      const texto  = await resp.text();
      const parser = new DOMParser();
      const doc    = parser.parseFromString(texto, 'text/html');

      let conteudoB64, saltB64, ivB64;
      doc.querySelectorAll('script').forEach(s => {{
        const m1 = s.textContent.match(/CONTEUDO_B64\s*=\s*"([^"]+)"/);
        const m2 = s.textContent.match(/SALT_B64\s*=\s*"([^"]+)"/);
        const m3 = s.textContent.match(/IV_B64\s*=\s*"([^"]+)"/);
        if (m1) conteudoB64 = m1[1];
        if (m2) saltB64     = m2[1];
        if (m3) ivB64       = m3[1];
      }});

      const enc    = new TextEncoder();
      const keyMat = await crypto.subtle.importKey(
        "raw", enc.encode(senha), "PBKDF2", false, ["deriveKey"]
      );
      const chave = await crypto.subtle.deriveKey(
        {{ name: "PBKDF2", salt: b64ToBytes(saltB64), iterations: 100000, hash: "SHA-256" }},
        keyMat,
        {{ name: "AES-CBC", length: 256 }},
        false, ["decrypt"]
      );
      const decriptado = await crypto.subtle.decrypt(
        {{ name: "AES-CBC", iv: b64ToBytes(ivB64) }},
        chave,
        b64ToBytes(conteudoB64)
      );
      const html = new TextDecoder().decode(decriptado);
      document.open();
      document.write(html);
      document.close();
    }}

    async function entrar() {{
      const usuario = document.getElementById('usuario').value.trim().toLowerCase();
      const senha   = document.getElementById('senha').value.trim();
      const erro    = document.getElementById('erro');
      erro.style.display = 'none';

      const arquivo = ARQUIVOS[usuario];
      if (!arquivo) {{
        erro.style.display = 'block';
        return;
      }}

      try {{
        // Tenta descriptografar — se a senha estiver errada, AES falha e cai no catch
        await descriptografarEExibir(arquivo, senha);
      }} catch(e) {{
        erro.style.display = 'block';
      }}
    }}

    document.getElementById('senha').addEventListener('keydown', function(e) {{
      if (e.key === 'Enter') entrar();
    }});
  </script>
</body>
</html>"""


# ============================================================
# EXPORTAR TUDO
# ============================================================

def _salvar_html(
    mapa: folium.Map,
    path_raw: Path,
    path_out: Path,
    senha: str | None,
    criptografar: bool,
):
    """Salva um mapa HTML, criptografando se necessário."""
    mapa.save(str(path_raw))
    if criptografar and senha:
        criptografar_html(path_raw, path_out, senha)
        logger.info(f"  Criptografado: {path_out.name}")
        path_raw.unlink()
    else:
        if path_out.exists():
            path_out.unlink()
        path_raw.rename(path_out)


def exportar_mapas(
    df_mapa: pd.DataFrame,
    geojson_grande_sp: dict,
    criptografar: bool = True,
) -> dict:
    """
    Gera e salva todos os HTMLs em data/output/.
    Se criptografar=True, aplica AES-256-CBC em cada mapa.
    O index.html nunca é criptografado.
    """
    arquivos = {}

    # Master
    logger.info("Gerando master.html...")
    mapa_master = montar_mapa(df_mapa, geojson_grande_sp, areas_visiveis=None)
    _salvar_html(
        mapa_master,
        OUTPUT_DIR / "_master_raw.html",
        OUTPUT_DIR / "master.html",
        USUARIOS_MAPA["master"]["senha"],
        criptografar,
    )
    arquivos["master"] = OUTPUT_DIR / "master.html"
    logger.info("✅ master.html")

    # Um por área
    for area_nome in CORES_AREAS:
        slug = (
            area_nome.lower()
            .replace(" ", "")
            .replace("á", "a").replace("é", "e")
            .replace("ê", "e").replace("ã", "a")
        )
        usuario = next(
            (u for u, d in USUARIOS_MAPA.items() if d["arquivo"] == f"{slug}.html"),
            None,
        )
        senha = USUARIOS_MAPA[usuario]["senha"] if usuario else None

        logger.info(f"Gerando {slug}.html...")
        mapa_area = montar_mapa(df_mapa, geojson_grande_sp, areas_visiveis=[area_nome])
        _salvar_html(
            mapa_area,
            OUTPUT_DIR / f"_{slug}_raw.html",
            OUTPUT_DIR / f"{slug}.html",
            senha,
            criptografar,
        )
        arquivos[slug] = OUTPUT_DIR / f"{slug}.html"
        logger.info(f"✅ {slug}.html")

    # Index
    logger.info("Gerando index.html...")
    path_index = OUTPUT_DIR / "index.html"
    path_index.write_text(gerar_index_html(), encoding="utf-8")
    arquivos["index"] = path_index
    logger.info("✅ index.html")

    logger.info(f"\n📁 Todos os HTMLs em: {OUTPUT_DIR.resolve()}")
    return arquivos