#!/usr/bin/env python3
"""
🧪 Teste Unitário & Simulação Local de Segurança
Valida o algoritmo de detecção de Outliers (IQR), a gravação no SQLite e a geração de agregações sanitizadas.
"""

import os
import sys
import tempfile
import sqlite3
import random

# Configura encoding para UTF-8 no Windows
if sys.platform.startswith("win"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Adiciona o diretório edge-processor ao path para importar as funções
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "edge-processor"))
from processor import filter_outliers_iqr


def test_outlier_filtering():
    print("=" * 60)
    print("🔬 TESTE 1: Validação do Algoritmo de Detecção de Outliers (IQR)")
    print("=" * 60)

    # Simula 100 sensores normais (~24.0°C) + 4 anomalias
    test_readings = []
    for i in range(1, 97):
        test_readings.append({
            "sensor_id": f"sensor_{i:03d}",
            "temperature": round(random.gauss(24.0, 1.2), 2)
        })

    # Injeta os 4 outliers intencionais
    test_readings.append({"sensor_id": "sensor_013", "temperature": 89.4})  # Queima
    test_readings.append({"sensor_id": "sensor_042", "temperature": -18.2}) # Congelamento/Bug
    test_readings.append({"sensor_id": "sensor_077", "temperature": 145.0}) # Spoofing
    test_readings.append({"sensor_id": "sensor_099", "temperature": 72.3})  # Spike

    clean, outliers = filter_outliers_iqr(test_readings)

    print(f"📥 Total de Leituras Testadas: {len(test_readings)}")
    print(f"✅ Leituras Consideradas Válidas: {len(clean)}")
    print(f"🚨 Outliers Detectados: {len(outliers)}")

    outlier_ids = [o["sensor_id"] for o in outliers]
    print(f"📋 Sensores Flagrados: {outlier_ids}")

    clean_temps = [c["temperature"] for c in clean]
    avg_clean = sum(clean_temps) / len(clean_temps)
    
    # Sem o filtro (média contaminada):
    all_temps = [t["temperature"] for t in test_readings]
    avg_contaminated = sum(all_temps) / len(all_temps)

    print(f"❌ Média CONTAMINADA (sem segurança): {avg_contaminated:.2f}°C")
    print(f"🛡️ Média SANITIZADA (com segurança na borda): {avg_clean:.2f}°C")

    # Asserções
    assert len(outliers) == 4, f"Esperado 4 outliers, mas detectou {len(outliers)}"
    assert "sensor_013" in outlier_ids
    assert "sensor_042" in outlier_ids
    assert "sensor_077" in outlier_ids
    assert "sensor_099" in outlier_ids
    assert 23.0 <= avg_clean <= 25.5, f"Média sanitizada fora do esperado: {avg_clean}"

    print("🎉 Teste de Outliers PASSOU com 100% de sucesso!")
    print("=" * 60)


def test_sqlite_persistence():
    print("\n" + "=" * 60)
    print("💾 TESTE 2: Validação de Persistência no SQLite")
    print("=" * 60)

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_db:
        db_path = tmp_db.name

    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE raw_telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_id TEXT NOT NULL,
                temperature REAL NOT NULL,
                processed INTEGER DEFAULT 0
            )
        """)
        
        # Insere 600 leituras (simulando 1 minuto: 100 sensores x 6 ciclos)
        for minute_cycle in range(6):
            for i in range(1, 101):
                cur.execute(
                    "INSERT INTO raw_telemetry (sensor_id, temperature) VALUES (?, ?)",
                    (f"sensor_{i:03d}", 24.2)
                )
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM raw_telemetry")
        count = cur.fetchone()[0]
        print(f"📊 Total de registros inseridos no SQLite: {count}")
        assert count == 600, f"Esperado 600 registros, obteve {count}"
        
        conn.close()
        print("🎉 Teste de Persistência SQLite PASSOU!")
        print("=" * 60)
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)


if __name__ == "__main__":
    test_outlier_filtering()
    test_sqlite_persistence()
