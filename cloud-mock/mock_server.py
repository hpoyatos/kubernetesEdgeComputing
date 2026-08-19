#!/usr/bin/env python3
"""
☁️ Cloud Mock API Server (Ambiente 3 - Nuvem)
Servidor simples para simular a API central na Nuvem recebendo telemetrias agregadas da Borda.
"""

import os
import json
import logging
from datetime import datetime, timezone
from flask import Flask, request, jsonify

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [CLOUD-MOCK] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("CloudMockAPI")

app = Flask(__name__)
PORT = int(os.getenv("PORT", "8080"))


@app.route("/", defaults={"path": ""}, methods=["GET", "POST", "PUT"])
@app.route("/<path:path>", methods=["GET", "POST", "PUT"])
def catch_all(path):
    if request.method == "GET":
        return jsonify({
            "service": "Cloud Greenhouse Telemetry Ingestion Service (Mock)",
            "status": "online",
            "time": datetime.now(timezone.utc).isoformat()
        }), 200

    payload = request.get_json(silent=True) or request.form.to_dict() or request.data.decode("utf-8")
    
    logger.info("=" * 70)
    logger.info(f"☁️ [RECEBIDO NA NUVEM] Endpoint: /{path} | Método: {request.method}")
    logger.info(f"📦 Payload Consolidado:\n{json.dumps(payload, indent=2, ensure_ascii=False)}")
    logger.info("=" * 70)

    return jsonify({
        "status": "success",
        "message": "Telemetria sanitizada processada com sucesso no Datacenter Cloud.",
        "received_at": datetime.now(timezone.utc).isoformat()
    }), 200


if __name__ == "__main__":
    logger.info(f"🚀 Iniciando Mock API Server na porta {PORT}...")
    app.run(host="0.0.0.0", port=PORT)
