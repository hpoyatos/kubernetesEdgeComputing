#!/usr/bin/env python3
"""
🌿 Smart Greenhouse - Edge Computing Processor & Gateway (Ambiente 2)
Responsabilidades:
1. Ingestão de telemetria via MQTT (tópico greenhouse/sensors/temperature).
2. Armazenamento local imediato em banco de dados SQLite (/data/edge_telemetry.db).
3. Processamento de Borda a cada minuto:
   - Coleta todas as leituras acumuladas no minuto.
   - Aplica algoritmo estatístico de detecção e remoção de Outliers (IQR / Limites Físicos).
   - Calcula métricas sanitizadas (média, mín, máx, descarte de anomalias).
   - Registra o sumário consolidado no SQLite.
   - Envia apenas 1 requisição POST por minuto para o Mock Server na Nuvem (Ambiente 3).
4. Dashboard Web Interativo em tempo real na porta 5000 para visualização amigável em sala de aula!
"""

import os
import sys
import time
import json
import sqlite3
import logging
import threading
from datetime import datetime, timezone

# Configura encoding para UTF-8 no Windows
if sys.platform.startswith("win"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

try:
    import requests
except ImportError:
    requests = None

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None

try:
    from flask import Flask, jsonify, render_template_string
except ImportError:
    Flask = None

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [EDGE-GATEWAY] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("EdgeProcessor")

# Configurações do Ambiente
MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "localhost")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "greenhouse/sensors/temperature")

SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "/data/edge_telemetry.db")
CLOUD_API_URL = os.getenv("CLOUD_API_URL", "http://cloud-mock:8080/api/v1/greenhouse/telemetry")
AGGREGATION_INTERVAL_SEC = int(os.getenv("AGGREGATION_INTERVAL_SEC", "60"))
GATEWAY_ID = os.getenv("GATEWAY_ID", "greenhouse-edge-gateway-01")
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "5000"))

# Estado em memória para exibição rápida no Dashboard
latest_runtime_state = {
    "gateway_id": GATEWAY_ID,
    "mqtt_connected": False,
    "last_batch_received_at": None,
    "total_raw_stored": 0,
    "total_outliers_blocked": 0,
    "last_avg_clean_temp": 24.0,
    "last_min_temp": 22.0,
    "last_max_temp": 26.0,
    "last_cloud_status": "Aguardando primeiro ciclo...",
    "last_cloud_timestamp": None,
    "cloud_api_url": CLOUD_API_URL,
    "active_outlier_sensors": {},
    "recent_points": []  # últimos 100 pontos para o gráfico
}


def init_sqlite_db():
    """Garante que o diretório e as tabelas SQLite existam."""
    db_dir = os.path.dirname(SQLITE_DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    with sqlite3.connect(SQLITE_DB_PATH) as conn:
        cursor = conn.cursor()
        
        # Tabela 1: Telemetria bruta recebida dos sensores (Borda Extrema)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_id TEXT NOT NULL,
                temperature REAL NOT NULL,
                unit TEXT NOT NULL,
                sensor_timestamp TEXT,
                received_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                processed INTEGER DEFAULT 0
            )
        """)

        # Tabela 2: Agregados consolidados por minuto (Pós-Filtragem de Segurança)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS minute_aggregates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                window_start TEXT NOT NULL,
                window_end TEXT NOT NULL,
                total_raw_readings INTEGER NOT NULL,
                valid_readings_count INTEGER NOT NULL,
                outliers_detected_count INTEGER NOT NULL,
                outlier_sensors_json TEXT NOT NULL,
                avg_clean_temperature REAL NOT NULL,
                min_clean_temperature REAL NOT NULL,
                max_clean_temperature REAL NOT NULL,
                cloud_dispatch_status TEXT NOT NULL,
                cloud_response_code INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
    logger.info(f"💾 Banco de dados SQLite inicializado em '{SQLITE_DB_PATH}'.")


def insert_raw_reading(sensor_id: str, temperature: float, unit: str, sensor_timestamp: str):
    """Insere uma leitura de sensor no SQLite."""
    try:
        with sqlite3.connect(SQLITE_DB_PATH, timeout=10.0) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO raw_telemetry (sensor_id, temperature, unit, sensor_timestamp, processed)
                VALUES (?, ?, ?, ?, 0)
            """, (sensor_id, temperature, unit, sensor_timestamp))
            conn.commit()

        # Atualiza métricas rápidas
        latest_runtime_state["total_raw_stored"] += 1
        latest_runtime_state["last_batch_received_at"] = datetime.now(timezone.utc).strftime("%H:%M:%S")

    except Exception as e:
        logger.error(f"Erro ao salvar telemetria bruta no SQLite: {e}")


def filter_outliers_iqr(readings: list) -> tuple:
    """
    Algoritmo estatístico de detecção de outliers por IQR (Interquartile Range).
    Também aplica sanity check de limites físicos de uma estufa (0°C a 50°C).
    Retorna: (clean_readings, outlier_readings)
    """
    if len(readings) < 4:
        return readings, []

    temps = sorted([r["temperature"] for r in readings])
    n = len(temps)
    
    # Cálculo aproximado dos quartis Q1 e Q3
    q1 = temps[int(n * 0.25)]
    q3 = temps[int(n * 0.75)]
    iqr = q3 - q1
    
    lower_bound = max(q1 - 1.5 * iqr, 0.0)    # Limite inferior com sanity check (mín 0°C)
    upper_bound = min(q3 + 1.5 * iqr, 50.0)   # Limite superior com sanity check (máx 50°C)

    clean_readings = []
    outlier_readings = []

    for r in readings:
        temp = r["temperature"]
        if temp < lower_bound or temp > upper_bound:
            r["outlier_reason"] = f"Fora do intervalo seguro [{lower_bound:.1f}°C, {upper_bound:.1f}°C]"
            outlier_readings.append(r)
        else:
            clean_readings.append(r)

    return clean_readings, outlier_readings


def perform_aggregation_and_dispatch():
    """Executa a filtragem de segurança das leituras pendentes e envia para a Nuvem."""
    try:
        window_end = datetime.now(timezone.utc).isoformat()
        
        with sqlite3.connect(SQLITE_DB_PATH, timeout=15.0) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Busca leituras não processadas
            cursor.execute("""
                SELECT id, sensor_id, temperature, unit, sensor_timestamp, received_at
                FROM raw_telemetry
                WHERE processed = 0
                ORDER BY id ASC
            """)
            rows = cursor.fetchall()

            if not rows:
                logger.info("ℹ️ Nenhuma leitura nova para agregar no momento.")
                return {"status": "empty", "message": "Nenhuma leitura pendente"}

            readings = [dict(row) for row in rows]
            row_ids = [r["id"] for r in readings]
            window_start = readings[0]["received_at"]

            # 1. Executa Filtragem de Segurança e Outliers
            clean_readings, outliers = filter_outliers_iqr(readings)
            
            if not clean_readings:
                logger.warning("⚠️ Todas as leituras foram marcadas como outliers! Usando todas para evitar média nula.")
                clean_readings = readings

            clean_temps = [r["temperature"] for r in clean_readings]
            avg_temp = round(sum(clean_temps) / len(clean_temps), 2)
            min_temp = round(min(clean_temps), 2)
            max_temp = round(max(clean_temps), 2)

            # Identifica os sensores que causaram anomalias
            outlier_summary = {}
            for out in outliers:
                sid = out["sensor_id"]
                outlier_summary.setdefault(sid, []).append(out["temperature"])

            # Atualiza estado em memória para o Dashboard
            latest_runtime_state["last_avg_clean_temp"] = avg_temp
            latest_runtime_state["last_min_temp"] = min_temp
            latest_runtime_state["last_max_temp"] = max_temp
            latest_runtime_state["total_outliers_blocked"] += len(outliers)
            latest_runtime_state["active_outlier_sensors"] = outlier_summary

            # 2. Monta o Payload Consolidado para a Nuvem (Ambiente 3)
            cloud_payload = {
                "gateway_id": GATEWAY_ID,
                "metric": "greenhouse_temperature_summary",
                "window_start": window_start,
                "window_end": window_end,
                "unit": "Celsius",
                "temperature": {
                    "average_clean": avg_temp,
                    "min_clean": min_temp,
                    "max_clean": max_temp
                },
                "telemetry_stats": {
                    "total_raw_collected": len(readings),
                    "valid_readings": len(clean_readings),
                    "outliers_filtered": len(outliers),
                    "data_reduction_ratio": f"{((len(readings)-1)/len(readings))*100:.2f}%"
                },
                "security_insights": {
                    "anomalies_detected": len(outliers) > 0,
                    "flagged_sensors": outlier_summary,
                    "data_poisoning_prevented": True,
                    "edge_security_action": "Outliers expurgados antes da transmissão para a Nuvem"
                }
            }

            # 3. Dispara para o Mock API Server na Nuvem
            status_desc = "SUCCESS"
            http_code = None
            
            logger.info("=" * 70)
            logger.info(f"📊 [EDGE ANALYTICS] Janela processada:")
            logger.info(f"   📥 Leituras Brutas Recebidas: {len(readings)}")
            logger.info(f"   🛡️ Outliers Detectados e Removidos: {len(outliers)}")
            if outlier_summary:
                logger.info(f"   🚨 Sensores Anômalos Flagrados: {list(outlier_summary.keys())}")
            logger.info(f"   ✅ Média Sanitizada da Estufa: {avg_temp}°C (Min: {min_temp}°C, Max: {max_temp}°C)")
            logger.info(f"   🌐 Enviando 1 pacote consolidado para a Nuvem: {CLOUD_API_URL}")
            
            if requests:
                try:
                    resp = requests.post(CLOUD_API_URL, json=cloud_payload, timeout=8.0)
                    http_code = resp.status_code
                    status_desc = f"SENT_HTTP_{resp.status_code}"
                    latest_runtime_state["last_cloud_status"] = f"✅ Sucesso (HTTP {resp.status_code})"
                    logger.info(f"   ☁️ Resposta da Nuvem: HTTP {resp.status_code}")
                except Exception as net_err:
                    status_desc = f"OFFLINE_FAIL_{type(net_err).__name__}"
                    latest_runtime_state["last_cloud_status"] = f"⚠️ Falha de Conexão ({type(net_err).__name__})"
                    logger.warning(f"   ⚠️ Nuvem indisponível ou endpoint mock offline ({net_err}).")
                    logger.info("   🛡️ Resiliência da Borda: Os dados continuam salvos com segurança no SQLite local!")
            else:
                status_desc = "NO_REQUESTS_LIB"
                latest_runtime_state["last_cloud_status"] = "Simulado (sem lib requests)"

            latest_runtime_state["last_cloud_timestamp"] = datetime.now(timezone.utc).strftime("%H:%M:%S")

            # 4. Registra a agregação no SQLite e marca os brutos como processados
            cursor.execute("""
                INSERT INTO minute_aggregates (
                    window_start, window_end, total_raw_readings, valid_readings_count,
                    outliers_detected_count, outlier_sensors_json, avg_clean_temperature,
                    min_clean_temperature, max_clean_temperature, cloud_dispatch_status, cloud_response_code
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                window_start, window_end, len(readings), len(clean_readings),
                len(outliers), json.dumps(outlier_summary), avg_temp,
                min_temp, max_temp, status_desc, http_code
            ))

            # Marca as leituras brutas como processadas
            cursor.execute(f"""
                UPDATE raw_telemetry
                SET processed = 1
                WHERE id IN ({','.join(['?']*len(row_ids))})
            """, row_ids)

            conn.commit()
            logger.info("=" * 70)
            return {"status": "success", "raw_count": len(readings), "avg_temp": avg_temp, "outliers": len(outliers)}

    except Exception as e:
        logger.error(f"Erro no ciclo de agregação do Edge Processor: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}


def process_minute_window():
    """Worker em background que executa a agregação a cada AGGREGATION_INTERVAL_SEC segundos."""
    logger.info("⚙️ Iniciando worker de agregação por minuto (Edge Analytics)...")
    while True:
        time.sleep(AGGREGATION_INTERVAL_SEC)
        perform_aggregation_and_dispatch()


def on_mqtt_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info(f"✅ Conectado ao Broker MQTT. Assinando tópico '{MQTT_TOPIC}'...")
        client.subscribe(MQTT_TOPIC)
        latest_runtime_state["mqtt_connected"] = True
    else:
        logger.error(f"❌ Falha ao conectar no Broker MQTT, código de retorno: {rc}")
        latest_runtime_state["mqtt_connected"] = False


def on_mqtt_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        sensor_id = payload.get("sensor_id", "unknown")
        temp = float(payload.get("temperature", 0.0))
        unit = payload.get("unit", "Celsius")
        ts = payload.get("timestamp", datetime.now(timezone.utc).isoformat())

        insert_raw_reading(sensor_id, temp, unit, ts)

        # Atualiza amostra recente para o gráfico no Dashboard (mantém últimos 100)
        is_outlier = (temp < 10.0 or temp > 40.0)
        latest_runtime_state["recent_points"].append({
            "sensor_id": sensor_id,
            "temp": temp,
            "is_outlier": is_outlier,
            "time": datetime.now(timezone.utc).strftime("%H:%M:%S")
        })
        if len(latest_runtime_state["recent_points"]) > 100:
            latest_runtime_state["recent_points"].pop(0)

    except Exception as e:
        logger.error(f"Erro ao processar mensagem MQTT recebida: {e}")


# ==============================================================================
# 🌐 WEB DASHBOARD INTERATIVO (FLASK EMBEDDED)
# ==============================================================================
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>🌿 Smart Greenhouse - Edge Security Gateway</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700;900&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #0b0f19;
      --card-bg: rgba(22, 30, 49, 0.75);
      --border: rgba(255, 255, 255, 0.08);
      --accent: #10b981;
      --accent-glow: rgba(16, 185, 129, 0.25);
      --danger: #ef4444;
      --danger-glow: rgba(239, 68, 68, 0.25);
      --warning: #f59e0b;
      --text: #f3f4f6;
      --text-muted: #9ca3af;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: radial-gradient(circle at 15% 15%, #131d36 0%, var(--bg) 100%);
      color: var(--text);
      font-family: 'Inter', sans-serif;
      min-height: 100vh;
      padding: 24px;
    }
    .header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding-bottom: 20px;
      border-bottom: 1px solid var(--border);
      margin-bottom: 24px;
      flex-wrap: wrap;
      gap: 12px;
    }
    .title-group h1 {
      font-size: 1.6rem;
      font-weight: 800;
      background: linear-gradient(135deg, #34d399 0%, #60a5fa 100%);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .title-group p { font-size: 0.85rem; color: var(--text-muted); margin-top: 4px; }
    .status-badges { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
    .badge {
      padding: 6px 14px;
      border-radius: 9999px;
      font-size: 0.75rem;
      font-weight: 600;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      background: var(--card-bg);
      border: 1px solid var(--border);
    }
    .badge.green { border-color: var(--accent); color: #34d399; }
    .badge.pulse::before {
      content: '';
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: #34d399;
      box-shadow: 0 0 8px #34d399;
      animation: pulse 1.5s infinite;
    }
    @keyframes pulse { 0% { opacity: 0.4; } 50% { opacity: 1; } 100% { opacity: 0.4; } }

    .btn-action {
      background: linear-gradient(135deg, #059669 0%, #10b981 100%);
      color: white;
      border: none;
      padding: 8px 16px;
      border-radius: 8px;
      font-size: 0.8rem;
      font-weight: 700;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      transition: all 0.2s;
      box-shadow: 0 4px 12px rgba(16, 185, 129, 0.3);
    }
    .btn-action:hover { transform: scale(1.03); filter: brightness(1.1); }
    .btn-action:active { transform: scale(0.97); }

    /* Grid de Métricas */
    .grid-stats {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 16px;
      margin-bottom: 24px;
    }
    .card {
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 20px;
      backdrop-filter: blur(12px);
      box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
      transition: transform 0.2s, border-color 0.2s;
    }
    .card:hover { transform: translateY(-2px); border-color: rgba(255,255,255,0.18); }
    .card .label { font-size: 0.75rem; font-weight: 600; text-transform: uppercase; color: var(--text-muted); letter-spacing: 0.05em; }
    .card .val { font-size: 1.8rem; font-weight: 900; margin-top: 8px; font-family: 'JetBrains Mono', monospace; }
    .card .sub { font-size: 0.75rem; color: var(--text-muted); margin-top: 6px; }
    .val.green { color: #34d399; }
    .val.red { color: #f87171; }
    .val.blue { color: #60a5fa; }
    .val.yellow { color: #fbbf24; }

    /* Layout Principal */
    .main-grid {
      display: grid;
      grid-template-columns: 2fr 1fr;
      gap: 20px;
      margin-bottom: 24px;
    }
    @media (max-width: 992px) { .main-grid { grid-template-columns: 1fr; } }

    .chart-container { position: relative; height: 280px; width: 100%; margin-top: 12px; }
    
    /* Lista de Outliers */
    .outlier-list { display: flex; flex-direction: column; gap: 10px; margin-top: 12px; max-height: 280px; overflow-y: auto; }
    .outlier-item {
      background: rgba(239, 68, 68, 0.1);
      border: 1px solid rgba(239, 68, 68, 0.3);
      padding: 10px 14px;
      border-radius: 10px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      font-size: 0.82rem;
    }
    .outlier-item .sensor-name { font-weight: 700; color: #fca5a5; font-family: 'JetBrains Mono', monospace; }
    .outlier-item .sensor-temp { font-weight: 800; color: #ef4444; }

    /* Tabelas SQLite */
    .db-section { margin-top: 24px; }
    .table-wrapper { overflow-x: auto; margin-top: 12px; }
    table { width: 100%; border-collapse: collapse; font-size: 0.8rem; text-align: left; }
    th { padding: 10px 14px; background: rgba(255,255,255,0.03); color: var(--text-muted); font-weight: 600; border-bottom: 1px solid var(--border); }
    td { padding: 10px 14px; border-bottom: 1px solid rgba(255,255,255,0.04); font-family: 'JetBrains Mono', monospace; }
    tr:hover td { background: rgba(255,255,255,0.02); }
  </style>
</head>
<body>

  <div class="header">
    <div class="title-group">
      <h1>🌿 Smart Greenhouse - Edge Security Gateway</h1>
      <p>Ambiente 2: Monitoramento em Borda, Detecção de Data Poisoning & Persistência Local</p>
    </div>
    <div class="status-badges">
      <button class="btn-action" id="btnTrigger" onclick="triggerManualAggregate()">⚡ Enviar para Nuvem Agora</button>
      <div class="badge green pulse" id="mqttStatus">MQTT Ingestion Ativa</div>
      <div class="badge">SQLite: /data/edge_telemetry.db</div>
      <div class="badge" style="border-color: #3b82f6; color: #60a5fa;">Economia de Banda: 99.83%</div>
    </div>
  </div>

  <!-- Estatísticas Rápidas -->
  <div class="grid-stats">
    <div class="card">
      <div class="label">Média Sanitizada da Estufa</div>
      <div class="val green" id="cleanAvg">-- °C</div>
      <div class="sub" id="minMaxSub">Min: --°C | Max: --°C</div>
    </div>
    <div class="card">
      <div class="label">Leituras Brutas no SQLite</div>
      <div class="val blue" id="rawStored">0</div>
      <div class="sub">100 sensores a cada 10s</div>
    </div>
    <div class="card">
      <div class="label">Outliers / Ataques Expurgados</div>
      <div class="val red" id="outliersCount">0</div>
      <div class="sub">Defesa contra Data Poisoning (IQR)</div>
    </div>
    <div class="card">
      <div class="label">Status de Envio para a Nuvem</div>
      <div class="val yellow" style="font-size: 1.1rem; margin-top: 14px;" id="cloudStatus">Conectando...</div>
      <div class="sub" id="cloudTime">1 payload por minuto</div>
    </div>
  </div>

  <!-- Gráfico + Lista de Alertas -->
  <div class="main-grid">
    <div class="card">
      <div class="label">📈 Telemetria em Tempo Real (Normal vs Anomalias Detectadas)</div>
      <div class="chart-container">
        <canvas id="telemetryChart"></canvas>
      </div>
    </div>
    <div class="card">
      <div class="label">🚨 Sensores Flagrados por Data Poisoning / Falhas</div>
      <div class="outlier-list" id="outlierList">
        <div style="color: var(--text-muted); font-size: 0.8rem; padding: 10px;">Aguardando primeiro ciclo de detecção...</div>
      </div>
    </div>
  </div>

  <!-- Tabela do Banco de Dados SQLite -->
  <div class="card db-section">
    <div class="label">💾 Histórico Consolidado de 1 Minuto (Tabela 'minute_aggregates' do SQLite)</div>
    <div class="table-wrapper">
      <table>
        <thead>
          <tr>
            <th>ID</th>
            <th>Janela (UTC)</th>
            <th>Leituras Brutas</th>
            <th>Válidas</th>
            <th>Outliers</th>
            <th>Média Sanitizada</th>
            <th>Status Nuvem</th>
          </tr>
        </thead>
        <tbody id="dbTableBody">
          <tr><td colspan="7" style="text-align: center; color: var(--text-muted);">Carregando banco SQLite...</td></tr>
        </tbody>
      </table>
    </div>
  </div>

  <script>
    // Inicialização do Gráfico Chart.js
    const ctx = document.getElementById('telemetryChart').getContext('2d');
    const chart = new Chart(ctx, {
      type: 'scatter',
      data: {
        datasets: [
          {
            label: 'Leituras Válidas da Estufa (~24°C)',
            data: [],
            backgroundColor: '#10b981',
            borderColor: '#34d399',
            pointRadius: 4,
          },
          {
            label: '🚨 Outliers Expurgados (Anomalias/Spoofing)',
            data: [],
            backgroundColor: '#ef4444',
            borderColor: '#f87171',
            pointRadius: 6,
            pointStyle: 'triangle'
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          x: {
            type: 'category',
            labels: [],
            grid: { color: 'rgba(255,255,255,0.05)' },
            ticks: { color: '#9ca3af', font: { size: 10 } }
          },
          y: {
            suggestedMin: -30,
            suggestedMax: 160,
            grid: { color: 'rgba(255,255,255,0.05)' },
            ticks: { color: '#9ca3af', callback: val => val + '°C' }
          }
        },
        plugins: {
          legend: { labels: { color: '#f3f4f6', font: { size: 11 } } }
        }
      }
    });

    async function triggerManualAggregate() {
      const btn = document.getElementById('btnTrigger');
      btn.textContent = '⏳ Processando e Enviando...';
      btn.disabled = true;
      try {
        const res = await fetch('/api/trigger-aggregate', { method: 'POST' });
        const data = await res.json();
        updateDashboard();
      } catch (e) {
        console.error(e);
      } finally {
        setTimeout(() => {
          btn.textContent = '⚡ Enviar para Nuvem Agora';
          btn.disabled = false;
        }, 1000);
      }
    }

    async function updateDashboard() {
      try {
        const res = await fetch('/api/status');
        const data = await res.json();

        document.getElementById('cleanAvg').textContent = data.last_avg_clean_temp.toFixed(1) + ' °C';
        document.getElementById('minMaxSub').textContent = `Min: ${data.last_min_temp}°C | Max: ${data.last_max_temp}°C`;
        document.getElementById('rawStored').textContent = data.total_raw_stored.toLocaleString();
        document.getElementById('outliersCount').textContent = data.total_outliers_blocked.toLocaleString();
        document.getElementById('cloudStatus').textContent = data.last_cloud_status;
        if (data.last_cloud_timestamp) {
          document.getElementById('cloudTime').textContent = 'Último envio: ' + data.last_cloud_timestamp + ' UTC';
        }

        // Atualiza lista de outliers flagrados
        const outlierList = document.getElementById('outlierList');
        const sensors = Object.keys(data.active_outlier_sensors);
        if (sensors.length > 0) {
          outlierList.innerHTML = sensors.map(s => {
            const vals = data.active_outlier_sensors[s].map(v => v.toFixed(1) + '°C').join(', ');
            return `
              <div class="outlier-item">
                <div>
                  <div class="sensor-name">🚨 ${s}</div>
                  <div style="font-size:0.7rem; color:var(--text-muted);">Ação: Bloqueado pelo Edge Filter</div>
                </div>
                <div class="sensor-temp">${vals}</div>
              </div>
            `;
          }).join('');
        }

        // Atualiza pontos do Gráfico
        const normalPoints = [];
        const outlierPoints = [];
        const timeLabels = [];

        data.recent_points.forEach((p, idx) => {
          timeLabels.push(p.sensor_id);
          if (p.is_outlier) {
            outlierPoints.push({ x: p.sensor_id, y: p.temp });
          } else {
            normalPoints.push({ x: p.sensor_id, y: p.temp });
          }
        });

        chart.data.labels = timeLabels;
        chart.data.datasets[0].data = normalPoints;
        chart.data.datasets[1].data = outlierPoints;
        chart.update('none');

        // Atualiza tabela SQLite
        const dbRes = await fetch('/api/db-aggregates');
        const dbData = await dbRes.json();
        const tbody = document.getElementById('dbTableBody');
        if (dbData.length > 0) {
          tbody.innerHTML = dbData.map(r => `
            <tr>
              <td>#${r.id}</td>
              <td>${r.window_start ? r.window_start.substring(11,19) : '--'} - ${r.window_end ? r.window_end.substring(11,19) : '--'}</td>
              <td>${r.total_raw_readings}</td>
              <td style="color:#34d399;">${r.valid_readings_count}</td>
              <td style="color:#f87171;">${r.outliers_detected_count}</td>
              <td style="color:#60a5fa; font-weight:700;">${r.avg_clean_temperature}°C</td>
              <td><span class="badge ${r.cloud_dispatch_status && r.cloud_dispatch_status.includes('HTTP_200') ? 'green' : ''}">${r.cloud_dispatch_status}</span></td>
            </tr>
          `).join('');
        }

      } catch (err) {
        console.error('Erro ao atualizar dashboard:', err);
      }
    }

    // Polling a cada 2 segundos
    setInterval(updateDashboard, 2000);
    updateDashboard();
  </script>
</body>
</html>
"""


def start_flask_dashboard():
    """Inicia o servidor Flask para o Web Dashboard em background."""
    if not Flask:
        logger.warning("⚠️ Flask não está instalado. O Web Dashboard não será iniciado.")
        return

    app = Flask("EdgeDashboard")
    
    # Desativa logs excessivos do werkzeug no terminal
    log = logging.getLogger("werkzeug")
    log.setLevel(logging.ERROR)

    @app.route("/")
    def index():
        return render_template_string(DASHBOARD_HTML)

    @app.route("/api/status")
    def api_status():
        return jsonify(latest_runtime_state)

    @app.route("/api/trigger-aggregate", methods=["POST"])
    def api_trigger():
        result = perform_aggregation_and_dispatch()
        return jsonify(result)

    @app.route("/api/db-aggregates")
    def api_db_aggregates():
        try:
            with sqlite3.connect(SQLITE_DB_PATH, timeout=5.0) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute("""
                    SELECT id, window_start, window_end, total_raw_readings, valid_readings_count,
                           outliers_detected_count, avg_clean_temperature, cloud_dispatch_status
                    FROM minute_aggregates
                    ORDER BY id DESC LIMIT 10
                """)
                return jsonify([dict(r) for r in cur.fetchall()])
        except Exception as e:
            return jsonify([])

    logger.info(f"🌐 Iniciando Web Dashboard interativo em http://0.0.0.0:{DASHBOARD_PORT}")
    app.run(host="0.0.0.0", port=DASHBOARD_PORT, debug=False, use_reloader=False)


def main():
    logger.info("=" * 70)
    logger.info("🌿 INICIANDO EDGE PROCESSOR & GATEWAY (AMBIENTE 2)")
    logger.info(f"📍 MQTT Broker: {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}")
    logger.info(f"📍 Tópico Assinado: {MQTT_TOPIC}")
    logger.info(f"💾 Banco SQLite: {SQLITE_DB_PATH}")
    logger.info(f"☁️ Nuvem Externa (Cloud Mock API): {CLOUD_API_URL}")
    logger.info(f"⏱️ Intervalo de Agregação: {AGGREGATION_INTERVAL_SEC} segundos")
    logger.info(f"🖥️ Dashboard Web: http://localhost:{DASHBOARD_PORT}")
    logger.info("=" * 70)

    # 1. Inicializa o banco de dados SQLite
    init_sqlite_db()

    # 2. Inicia o worker de agregação em thread separada
    aggregator_thread = threading.Thread(target=process_minute_window, daemon=True)
    aggregator_thread.start()

    # 3. Inicia o Dashboard Web Flask em thread separada
    web_thread = threading.Thread(target=start_flask_dashboard, daemon=True)
    web_thread.start()

    # 4. Conecta ao Broker MQTT e escuta os sensores
    if not mqtt:
        logger.error("❌ Paho MQTT client não encontrado. Instale paho-mqtt.")
        while True:
            time.sleep(10)

    client = mqtt.Client(client_id="smart-greenhouse-edge-processor")
    client.on_connect = on_mqtt_connect
    client.on_message = on_mqtt_message

    while True:
        try:
            logger.info(f"Conectando ao Broker MQTT em {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}...")
            client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, keepalive=60)
            client.loop_forever()
        except Exception as e:
            logger.warning(f"⚠️ Conexão MQTT caiu ({e}). Reconectando em 3 segundos...")
            time.sleep(3)


if __name__ == "__main__":
    main()
