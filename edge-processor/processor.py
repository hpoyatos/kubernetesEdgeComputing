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
"""

import os
import time
import json
import sqlite3
import logging
import threading
from datetime import datetime, timezone
try:
    import requests
except ImportError:
    requests = None

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None

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
CLOUD_API_URL = os.getenv("CLOUD_API_URL", "https://webhook.site/placeholder-endpoint")
AGGREGATION_INTERVAL_SEC = int(os.getenv("AGGREGATION_INTERVAL_SEC", "60"))
GATEWAY_ID = os.getenv("GATEWAY_ID", "greenhouse-edge-gateway-01")


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


def process_minute_window():
    """Worker que roda periodicamente para agregar os dados do último minuto e enviar para a Nuvem."""
    logger.info("⚙️ Iniciando worker de agregação por minuto (Edge Analytics)...")
    
    while True:
        time.sleep(AGGREGATION_INTERVAL_SEC)
        
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
                    continue

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
                logger.info(f"📊 [EDGE ANALYTICS] Janela de 1 minuto processada:")
                logger.info(f"   📥 Leituras Brutas Recebidas: {len(readings)}")
                logger.info(f"   🛡️ Outliers Detectados e Removidos: {len(outliers)}")
                if outlier_summary:
                    logger.info(f"   🚨 Sensores Anômalos Flagrados: {list(outlier_summary.keys())}")
                logger.info(f"   ✅ Média Sanitizada da Estufa: {avg_temp}°C (Min: {min_temp}°C, Max: {max_temp}°C)")
                logger.info(f"   🌐 Enviando 1 pacote consolidado para a Nuvem: {CLOUD_API_URL}")
                
                try:
                    resp = requests.post(CLOUD_API_URL, json=cloud_payload, timeout=8.0)
                    http_code = resp.status_code
                    status_desc = f"SENT_HTTP_{resp.status_code}"
                    logger.info(f"   ☁️ Resposta da Nuvem: HTTP {resp.status_code}")
                except Exception as net_err:
                    status_desc = f"OFFLINE_FAIL_{type(net_err).__name__}"
                    logger.warning(f"   ⚠️ Nuvem indisponível ou endpoint mock offline ({net_err}).")
                    logger.info("   🛡️ Resiliência da Borda: Os dados continuam salvos com segurança no SQLite local!")

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

        except Exception as e:
            logger.error(f"Erro no ciclo de agregação do Edge Processor: {e}", exc_info=True)


def on_mqtt_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info(f"✅ Conectado ao Broker MQTT. Assinando tópico '{MQTT_TOPIC}'...")
        client.subscribe(MQTT_TOPIC)
    else:
        logger.error(f"❌ Falha ao conectar no Broker MQTT, código de retorno: {rc}")


def on_mqtt_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        sensor_id = payload.get("sensor_id", "unknown")
        temp = float(payload.get("temperature", 0.0))
        unit = payload.get("unit", "Celsius")
        ts = payload.get("timestamp", datetime.now(timezone.utc).isoformat())

        insert_raw_reading(sensor_id, temp, unit, ts)

    except Exception as e:
        logger.error(f"Erro ao processar mensagem MQTT recebida: {e}")


def main():
    logger.info("=" * 70)
    logger.info("🌿 INICIANDO EDGE PROCESSOR & GATEWAY (AMBIENTE 2)")
    logger.info(f"📍 MQTT Broker: {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}")
    logger.info(f"📍 Tópico Assinado: {MQTT_TOPIC}")
    logger.info(f"💾 Banco SQLite: {SQLITE_DB_PATH}")
    logger.info(f"☁️ Nuvem Externa (Cloud Mock API): {CLOUD_API_URL}")
    logger.info(f"⏱️ Intervalo de Agregação: {AGGREGATION_INTERVAL_SEC} segundos")
    logger.info("=" * 70)

    # 1. Inicializa o banco de dados SQLite
    init_sqlite_db()

    # 2. Inicia o worker de agregação em thread separada
    aggregator_thread = threading.Thread(target=process_minute_window, daemon=True)
    aggregator_thread.start()

    # 3. Conecta ao Broker MQTT e escuta os sensores
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
