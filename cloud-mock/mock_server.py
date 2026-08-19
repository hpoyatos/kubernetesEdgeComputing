#!/usr/bin/env python3
"""
☁️ Cloud Mock API Server (Ambiente 3 - Nuvem)
Servidor simples com Dashboard Web para simular o Datacenter na Nuvem recebendo telemetrias da Borda.
"""

import os
import json
import logging
from datetime import datetime, timezone
from flask import Flask, request, jsonify, render_template_string

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [CLOUD-MOCK] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("CloudMockAPI")

app = Flask(__name__)
PORT = int(os.getenv("PORT", "8080"))

# Armazena os últimos 20 payloads recebidos da borda
received_cloud_history = []

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>☁️ Cloud Datacenter - Telemetry Mock API</title>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700;900&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #090d16;
      --card-bg: rgba(18, 25, 38, 0.85);
      --border: rgba(255, 255, 255, 0.08);
      --accent: #38bdf8;
      --accent-glow: rgba(56, 189, 248, 0.25);
      --text: #f8fafc;
      --text-muted: #94a3b8;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: radial-gradient(circle at 80% 20%, #1e293b 0%, var(--bg) 100%);
      color: var(--text);
      font-family: 'Inter', sans-serif;
      min-height: 100vh;
      padding: 24px;
    }
    .header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding-bottom: 18px;
      border-bottom: 1px solid var(--border);
      margin-bottom: 24px;
      flex-wrap: wrap;
      gap: 10px;
    }
    .title-group h1 {
      font-size: 1.5rem;
      font-weight: 800;
      background: linear-gradient(135deg, #38bdf8 0%, #818cf8 100%);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .badge {
      padding: 6px 14px;
      border-radius: 9999px;
      font-size: 0.75rem;
      font-weight: 600;
      background: var(--card-bg);
      border: 1px solid var(--accent);
      color: var(--accent);
    }
    .grid { display: flex; flex-direction: column; gap: 16px; }
    .card {
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 18px;
      backdrop-filter: blur(8px);
      box-shadow: 0 4px 20px rgba(0,0,0,0.3);
    }
    .card-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 12px;
      font-size: 0.85rem;
      color: var(--text-muted);
    }
    .temp-highlight {
      font-size: 1.6rem;
      font-weight: 800;
      color: #34d399;
      font-family: 'JetBrains Mono', monospace;
    }
    pre {
      background: #050811;
      padding: 14px;
      border-radius: 8px;
      font-size: 0.78rem;
      color: #a5f3fc;
      overflow-x: auto;
      font-family: 'JetBrains Mono', monospace;
      margin-top: 10px;
    }
    .empty-state { text-align: center; padding: 40px; color: var(--text-muted); }
  </style>
</head>
<body>

  <div class="header">
    <div class="title-group">
      <h1>☁️ Cloud Datacenter - Smart Greenhouse Telemetry Ingestion</h1>
      <p style="font-size: 0.85rem; color: var(--text-muted); margin-top: 4px;">Ambiente 3: Recebimento de 1 payload consolidado por minuto vindo da Borda</p>
    </div>
    <div class="badge">Status: Online | Porta 8080</div>
  </div>

  <div class="grid">
    {% if not history %}
      <div class="card empty-state">
        <h3>⏳ Aguardando primeira transmissão da Borda (Edge Gateway)...</h3>
        <p style="margin-top: 8px; font-size: 0.85rem;">O Edge Processor envia 1 resumo a cada 60 segundos.</p>
      </div>
    {% endif %}

    {% for item in history %}
      <div class="card">
        <div class="card-header">
          <div><strong>📦 Gateway:</strong> {{ item.payload.gateway_id | default('greenhouse-edge-01') }} | <strong>Janela:</strong> {{ item.payload.window_start }}</div>
          <div><strong>Recebido em:</strong> {{ item.received_at }}</div>
        </div>
        <div style="display: flex; gap: 20px; align-items: baseline; flex-wrap: wrap;">
          <div>
            <span style="font-size: 0.75rem; color: var(--text-muted);">MÉDIA SANITIZADA:</span>
            <div class="temp-highlight">{{ item.payload.temperature.average_clean | default('--') }} °C</div>
          </div>
          <div>
            <span style="font-size: 0.75rem; color: var(--text-muted);">LEITURAS BRUTAS:</span>
            <div style="font-size: 1.1rem; font-weight: 700; color: #60a5fa;">{{ item.payload.telemetry_stats.total_raw_collected | default('--') }}</div>
          </div>
          <div>
            <span style="font-size: 0.75rem; color: var(--text-muted);">OUTLIERS REMOVIDOS:</span>
            <div style="font-size: 1.1rem; font-weight: 700; color: #f87171;">{{ item.payload.telemetry_stats.outliers_filtered | default('0') }}</div>
          </div>
        </div>
        <pre>{{ item.raw_json }}</pre>
      </div>
    {% endfor %}
  </div>

  <script>
    // Atualiza a página a cada 5 segundos
    setTimeout(() => { window.location.reload(); }, 5000);
  </script>
</body>
</html>
"""


@app.route("/", methods=["GET"])
def web_dashboard():
    return render_template_string(HTML_TEMPLATE, history=received_cloud_history)


@app.route("/", defaults={"path": ""}, methods=["POST", "PUT"])
@app.route("/<path:path>", methods=["POST", "PUT"])
def catch_all(path):
    payload = request.get_json(silent=True) or request.form.to_dict() or request.data.decode("utf-8")
    now_str = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

    logger.info("=" * 70)
    logger.info(f"☁️ [RECEBIDO NA NUVEM] Endpoint: /{path} | Método: {request.method}")
    logger.info(f"📦 Payload Consolidado:\n{json.dumps(payload, indent=2, ensure_ascii=False)}")
    logger.info("=" * 70)

    # Armazena histórico
    received_cloud_history.insert(0, {
        "received_at": now_str,
        "payload": payload if isinstance(payload, dict) else {},
        "raw_json": json.dumps(payload, indent=2, ensure_ascii=False)
    })
    if len(received_cloud_history) > 20:
        received_cloud_history.pop()

    return jsonify({
        "status": "success",
        "message": "Telemetria sanitizada processada com sucesso no Datacenter Cloud.",
        "received_at": datetime.now(timezone.utc).isoformat()
    }), 200


if __name__ == "__main__":
    logger.info(f"🚀 Iniciando Mock API Server na porta {PORT}...")
    app.run(host="0.0.0.0", port=PORT)
