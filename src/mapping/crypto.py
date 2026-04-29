# ============================================================
# src/mapping/crypto.py
# Criptografia AES-256-CBC com PBKDF2-SHA256.
# Gera HTML que descriptografa no navegador via WebCrypto API.
# Compatível com o padrão PageCrypt v7.
# ============================================================

import base64
import os

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes, padding
from cryptography.hazmat.backends import default_backend
from pathlib import Path


def _derivar_chave(senha: str, salt: bytes) -> bytes:
    """Deriva chave AES-256 da senha usando PBKDF2-SHA256."""
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100_000,
        backend=default_backend(),
    )
    return kdf.derive(senha.encode("utf-8"))


def _criptografar_aes(chave: bytes, iv: bytes, dados: bytes) -> bytes:
    """Criptografa dados com AES-256-CBC."""
    padder = padding.PKCS7(128).padder()
    dados_padded = padder.update(dados) + padder.finalize()
    cipher = Cipher(algorithms.AES(chave), modes.CBC(iv), backend=default_backend())
    enc = cipher.encryptor()
    return enc.update(dados_padded) + enc.finalize()


def _template_login(conteudo_cifrado_b64: str, salt_b64: str, iv_b64: str) -> str:
    """
    Gera HTML de login que descriptografa o conteúdo no navegador
    usando a Web Crypto API (AES-CBC + PBKDF2).
    """
    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Área Protegida</title>
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
      <p>Área Protegida</p>
    </div>
    <label for="senha">Senha</label>
    <input type="password" id="senha" placeholder="sua senha" autocomplete="current-password">
    <button onclick="descriptografar()">Entrar</button>
    <div class="erro" id="erro">Senha incorreta.</div>
  </div>
  <script>
    const CONTEUDO_B64 = "{conteudo_cifrado_b64}";
    const SALT_B64     = "{salt_b64}";
    const IV_B64       = "{iv_b64}";

    function b64ToBytes(b64) {{
      const bin = atob(b64);
      const arr = new Uint8Array(bin.length);
      for (let i = 0; i < bin.length; i++) arr[i] = bin.charCodeAt(i);
      return arr;
    }}

    async function descriptografar() {{
      const senha = document.getElementById('senha').value;
      const erro  = document.getElementById('erro');
      erro.style.display = 'none';
      try {{
        const enc    = new TextEncoder();
        const keyMat = await crypto.subtle.importKey(
          "raw", enc.encode(senha), "PBKDF2", false, ["deriveKey"]
        );
        const chave = await crypto.subtle.deriveKey(
          {{ name: "PBKDF2", salt: b64ToBytes(SALT_B64), iterations: 100000, hash: "SHA-256" }},
          keyMat,
          {{ name: "AES-CBC", length: 256 }},
          false, ["decrypt"]
        );
        const decriptado = await crypto.subtle.decrypt(
          {{ name: "AES-CBC", iv: b64ToBytes(IV_B64) }},
          chave,
          b64ToBytes(CONTEUDO_B64)
        );
        const html = new TextDecoder().decode(decriptado);
        document.open();
        document.write(html);
        document.close();
      }} catch (e) {{
        erro.style.display = 'block';
      }}
    }}

    document.getElementById('senha').addEventListener('keydown', function(e) {{
      if (e.key === 'Enter') descriptografar();
    }});
  </script>
</body>
</html>"""


def criptografar_html(path_input: Path, path_output: Path, senha: str) -> None:
    """
    Lê um HTML, criptografa com AES-256-CBC e gera um novo HTML
    que descriptografa no navegador via WebCrypto API.

    Parâmetros
    ----------
    path_input  : HTML original a ser criptografado
    path_output : destino do HTML protegido
    senha       : senha usada para derivar a chave AES
    """
    conteudo = path_input.read_bytes()

    salt = os.urandom(16)
    iv   = os.urandom(16)

    chave   = _derivar_chave(senha, salt)
    cifrado = _criptografar_aes(chave, iv, conteudo)

    html_final = _template_login(
        conteudo_cifrado_b64=base64.b64encode(cifrado).decode("ascii"),
        salt_b64=base64.b64encode(salt).decode("ascii"),
        iv_b64=base64.b64encode(iv).decode("ascii"),
    )
    path_output.write_text(html_final, encoding="utf-8")
