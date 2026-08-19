#!/usr/bin/env python3
"""
🌿 Smart Greenhouse - Sensor Telemetry Generator (IoT Edge Simulator)
Ambiente 1: Simula 100 sensores de temperatura publicando a cada 10 segundos via MQTT.
Injeta deliberadamente 3 a 4 termômetros anômalos (outliers) para fins de teste de segurança / data poisoning.
"""

import os
import time
import json
import random
import logging
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [SENSOR-SIM] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("SensorGenerator")

# Variáveis de Ambiente e Parâmetros
MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "localhost")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "greenhouse/sensors/temperature")
READING_INTERVAL_SEC = int(os.getenv("READING_INTERVAL_SEC", "10"))
SENSOR_COUNT = int(os.getenv("SENSOR_COUNT", "100"))

# Configuração dos Sensores Normais
BASE_TEMP_TARGET = float(os.getenv("BASE_TEMP_TARGET", "24.0"))  # Média normal em °C
NORMAL_TEMP_VARIATION = float(os.getenv("NORMAL_TEMP_VARIATION", "1.5"))  # Desvio padrão

# IDs dos sensores que geram anomalias / spoofing (para demonstrar data poisoning em aula)
# 4 sensores com diferentes tipos de anomalias:
OUTLIER_PROFILES = {
    "sensor_013": {"type": "Superaquecimento / Falha de Hardware", "min": 85.0, "max": 95.0},
    "sensor_042": {"type": "Curto-Circuito / Temperatura Negativa", "min": -25.0, "max": -15.0},
    "sensor_077": {"type": "Ataque Spoofing / Data Poisoning", "min": 130.0, "max": 160.0},
    "sensor_099": {"type": "Pico Elétrico / Leitura Errática", "min": 65.0, "max": 75.0},
}


def connect_mqtt() -> mqtt.Client:
    """Conecta ao Broker MQTT com retry automático."""
    client = mqtt.Client(client_id="smart-greenhouse-sensor-generator")
    
    while True:
        try:
            logger.info(f"Conectando ao MQTT Broker em {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}...")
            client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, keepalive=60)
            client.loop_start()
            logger.info("✅ Conectado com sucesso ao Broker MQTT!")
            return client
        except Exception as e:
            logger.warning(f"⚠️ Falha ao conectar no broker MQTT ({e}). Tentando novamente em 3 segundos...")
            time.sleep(3)


def generate_sensor_readings() -> list:
    """Gera 100 leituras de temperatura com 3 a 4 anomalias intencionais."""
    now_iso = datetime.now(timezone.utc).isoformat()
    readings = []

    for i in range(1, SENSOR_COUNT + 1):
        sensor_id = f"sensor_{i:03d}"

        if sensor_id in OUTLIER_PROFILES:
            # Gera leitura discrepante (outlier)
            profile = OUTLIER_PROFILES[sensor_id]
            temp = round(random.uniform(profile["min"], profile["max"]), 2)
            is_injected_outlier = True
        else:
            # Gera leitura normal de estufa com leve ruído térmico
            temp = round(random.gauss(BASE_TEMP_TARGET, NORMAL_TEMP_VARIATION), 2)
            is_injected_outlier = False

        reading = {
            "sensor_id": sensor_id,
            "timestamp": now_iso,
            "temperature": temp,
            "unit": "Celsius",
            "_injected_outlier": is_injected_outlier  # Flag didática (o processador de borda não sabe disso!)
        }
        readings.append(reading)

    return readings


def main():
    logger.info("=" * 70)
    logger.info("🌿 INICIANDO SIMULADOR DE SENSORES DA ESTUFA INTELIGENTE (AMBIENTE 1)")
    logger.info(f"📍 Total de Sensores: {SENSOR_COUNT}")
    logger.info(f"⏱️ Intervalo de Envio: a cada {READING_INTERVAL_SEC} segundos")
    logger.info(f"🎯 Temperatura Alvo Estufa: {BASE_TEMP_TARGET}°C")
    logger.info(f"🚨 Sensores com Injeção de Anomalias: {list(OUTLIER_PROFILES.keys())}")
    logger.info("=" * 70)

    client = connect_mqtt()
    batch_count = 0

    try:
        while True:
            batch_count += 1
            readings = generate_sensor_readings()
            
            # Envia cada leitura para o tópico MQTT
            for reading in readings:
                payload = json.dumps(reading)
                client.publish(MQTT_TOPIC, payload, qos=1)

            outlier_temps = [f"{r['sensor_id']}={r['temperature']}°C" for r in readings if r["_injected_outlier"]]
            normal_temps = [r["temperature"] for r in readings if not r["_injected_outlier"]]
            avg_normal = sum(normal_temps) / len(normal_temps)

            logger.info(
                f"[Lote #{batch_count:04d}] 📤 Enviadas {len(readings)} leituras para '{MQTT_TOPIC}'. "
                f"Média normal: {avg_normal:.2f}°C | ⚠️ Outliers injetados: {', '.join(outlier_temps)}"
            )

            time.sleep(READING_INTERVAL_SEC)

    except KeyboardInterrupt:
        logger.info("🛑 Finalizando simulador de sensores...")
    finally:
        client.loop_stop()
        client.disconnect()
        logger.info("Desconectado do MQTT.")


if __name__ == "__main__":
    main()
