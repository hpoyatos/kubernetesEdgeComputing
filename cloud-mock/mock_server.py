#!/usr/bin/env python3
"""
☁️ Cloud Datacenter - Telemetry Mock API Server (Ambiente 3 - Nuvem)
Servidor com Dashboard Web em tempo real para simular o Datacenter na Nuvem
recebendo as telemetrias consolidadas e higienizadas da Borda.
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone
from flask import Flask, request, jsonify, render_template_string

# Configura encoding para UTF-8 no Windows
if sys.platform.startswith("win"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [CLOUD-MOCK] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("CloudMockAPI")

app = Flask(__name__)
PORT = int(os.getenv("PORT", "8080"))

# Armazena os últimos 30 payloads recebidos da borda
received_cloud_history = []

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>☁️ Cloud Datacenter - Telemetry Ingestion API</title>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700;900&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #090d16;
      --card-bg: rgba(18, 25, 38, 0.85);
      --border: rgba(255, 255, 255, 0.08);
      --accent: #38bdf8;
      --accent-glow: rgba(56, 189, 248, 0.25);
      --success: #34d399;
      --danger: #f87171;
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
      gap: 12px;
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
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }
    .badge.live::before {
      content: '';
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: #38bdf8;
      box-shadow: 0 0 8px #38bdf8;
      animation: pulse 1.5s infinite;
    }
    @keyframes pulse { 0% { opacity: 0.3; } 50% { opacity: 1; } 100% { opacity: 0.3; } }

    .summary-bar {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 14px;
      margin-bottom: 24px;
    }
    .summary-card {
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 16px;
      backdrop-filter: blur(8px);
    }
    .summary-card .label { font-size: 0.72rem; color: var(--text-muted); text-transform: uppercase; font-weight: 600; }
    .summary-card .val { font-size: 1.6rem; font-weight: 900; margin-top: 6px; font-family: 'JetBrains Mono', monospace; }

    .grid { display: flex; flex-direction: column; gap: 16px; }
    .card {
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 20px;
      backdrop-filter: blur(8px);
      box-shadow: 0 4px 20px rgba(0,0,0,0.3);
      animation: slideIn 0.3s ease-out;
    }
    @keyframes slideIn { from { opacity: 0; transform: translateY(-8px); } to { opacity: 1; transform: translateY(0); } }

    .card-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 14px;
      font-size: 0.85rem;
      color: var(--text-muted);
      border-bottom: 1px solid rgba(255,255,255,0.05);
      padding-bottom: 10px;
    }
    .stats-row {
      display: flex;
      gap: 24px;
      align-items: baseline;
      flex-wrap: wrap;
      margin-bottom: 14px;
    }
    .temp-highlight {
      font-size: 1.8rem;
      font-weight: 800;
      color: var(--success);
      font-family: 'JetBrains Mono', monospace;
    }
    .stat-box { display: flex; flex-direction: column; }
    .stat-label { font-size: 0.72rem; color: var(--text-muted); text-transform: uppercase; font-weight: 600; }
    .stat-num { font-size: 1.2rem; font-weight: 700; font-family: 'JetBrains Mono', monospace; margin-top: 2px; }

    pre {
      background: #050811;
      padding: 14px;
      border-radius: 8px;
      font-size: 0.78rem;
      color: #a5f3fc;
      overflow-x: auto;
      font-family: 'JetBrains Mono', monospace;
      max-height: 250px;
      border: 1px solid rgba(255,255,255,0.05);
    }
    .empty-state { text-align: center; padding: 50px 20px; color: var(--text-muted); }
  </style>
</head>
<body>

  <div class="header">
    <div class="title-group">
      <h1>☁️ Cloud Datacenter - Smart Greenhouse Telemetry Ingestion</h1>
      <p style="font-size: 0.85rem; color: var(--text-muted); margin-top: 4px;">Ambiente 3: Recebimento de 1 payload consolidado e sanitizado por minuto vindo da Borda</p>
    </div>
    <div class="badge live" id="liveBadge">Nuvem Online (Porta 8080)</div>
  </div>

  <div class="summary-bar">
    <div class="summary-card">
      <div class="label">Pacotes Recebidos da Borda</div>
      <div class="val" style="color: #38bdf8;" id="totalReceived">0</div>
    </div>
    <div class="summary-card">
      <div class="label">Última Média Recebida</div>
      <div class="val" style="color: #34d399;" id="lastAvg">-- °C</div>
    </div>
    <div class="summary-card">
      <div class="label">Economia de Rede (Data Minimization)</div>
      <div class="val" style="color: #818cf8;">99.83%</div>
    </div>
    <div class="summary-card">
      <div class="label">Status de Segurança</div>
      <div class="val" style="color: #34d399; font-size: 1.1rem; margin-top: 10px;">🛡️ Sanitizado na Borda</div>
    </div>
  </div>

  <div class="grid" id="historyContainer">
    <div class="card empty-state">
      <h3>⏳ Aguardando primeira transmissão da Borda (Edge Gateway)...</h3>
      <p style="margin-top: 8px; font-size: 0.85rem;">O Edge Processor agrupa as 600 leituras do minuto e envia 1 resumo consolidado a cada 60 segundos.</p>
    </div>
  </div>

  <script>
    async function fetchCloudHistory() {
      try {
        const res = await fetch('/api/history');
        const items = await res.json();
        
        document.getElementById('totalReceived').textContent = items.length;
        
        if (items.length > 0) {
          const first = items[0];
          const avgTemp = (first.payload && first.payload.temperature && first.payload.temperature.average_clean !== undefined)
            ? first.payload.temperature.average_clean + ' °C'
            : (first.avg_clean || '-- °C');
          document.getElementById('lastAvg').textContent = avgTemp;

          const container = document.getElementById('historyContainer');
          container.innerHTML = items.map((item, idx) => {
            const p = item.payload || {};
            const temp = p.temperature || {};
            const stats = p.telemetry_stats || {};
            const security = p.security_insights || {};
            
            const avgVal = temp.average_clean !== undefined ? temp.average_clean + ' °C' : '--';
            const rawCount = stats.total_raw_collected || '--';
            const outlierCount = stats.outliers_filtered || '0';
            const reduction = stats.data_reduction_ratio || '99.83%';

            return `
              <div class="card">
                <div class="card-header">
                  <div>
                    <strong>📦 Pacote #${items.length - idx}</strong> | Gateway: <code>${p.gateway_id || 'edge-gateway'}</code>
                  </div>
                  <div><strong>Recebido às:</strong> ${item.received_at}</div>
                </div>
                <div class="stats-row">
                  <div class="stat-box">
                    <span class="stat-label">Média Sanitizada da Estufa</span>
                    <div class="temp-highlight">${avgVal}</div>
                  </div>
                  <div class="stat-box">
                    <span class="stat-label">Leituras Brutas Agrupadas</span>
                    <div class="stat-num" style="color: #60a5fa;">${rawCount}</div>
                  </div>
                  <div class="stat-box">
                    <span class="stat-label">Outliers / Ataques Expurgados</span>
                    <div class="stat-num" style="color: #f87171;">${outlierCount}</div>
                  </div>
                  <div class="stat-box">
                    <span class="stat-label">Redução de Tráfego</span>
                    <div class="stat-num" style="color: #a78bfa;">${reduction}</div>
                  </div>
                </div>
                <pre>${item.raw_json}</pre>
              </div>
            `;
          }).join('');
        }
      } catch (err) {
        console.error('Erro ao atualizar nuvem:', err);
      }
    }

    // Atualiza via AJAX a cada 2 segundos
    setInterval(fetchCloudHistory, 2000);
    fetchCloudHistory();
  </script>
</body>
</html>
"""


@app.route("/", methods=["GET"])
def web_dashboard():
    return render_template_string(HTML_TEMPLATE)


@app.route("/api/history", methods=["GET"])
def api_history():
    return jsonify(received_cloud_history)


@app.route("/", defaults={"path": ""}, methods=["POST", "PUT"])
@app.route("/<path:path>", methods=["POST", "PUT"])
def catch_all(path):
    try:
        payload = request.get_json(force=True, silent=True)
    except Exception:
        payload = None

    if payload is None:
        payload = request.form.to_dict() or request.data.decode("utf-8", errors="replace")

    now_str = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

    logger.info("=" * 70)
    logger.info(f"☁️ [RECEBIDO NA NUVEM] Endpoint: /{path} | Método: {request.method}")
    logger.info(f"📦 Payload Consolidado:\n{json.dumps(payload, indent=2, ensure_ascii=False) if isinstance(payload, (dict, list)) else payload}")
    logger.info("=" * 70)

    # Armazena no histórico
    item_record = {
        "received_at": now_str,
        "endpoint": f"/{path}",
        "payload": payload if isinstance(payload, dict) else {"raw": str(payload)},
        "raw_json": json.dumps(payload, indent=2, ensure_ascii=False) if isinstance(payload, (dict, list)) else str(payload)
    }
    received_cloud_history.insert(0, item_record)
    if len(received_cloud_history) > 30:
        received_cloud_history.pop()

    return jsonify({
        "status": "success",
        "message": "Telemetria sanitizada processada com sucesso no Datacenter Cloud.",
        "received_at": datetime.now(timezone.utc).isoformat()
    }), 200


if __name__ == "__main__":
    logger.info(f"🚀 Iniciando Mock API Server na porta {PORT}...")
    app.run(host="0.0.0.0", port=PORT)
