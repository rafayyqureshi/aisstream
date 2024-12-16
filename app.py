import os
import json
from flask import Flask, jsonify
from google.cloud import bigquery
from datetime import datetime, timedelta

app = Flask(__name__)

# Inicjalizacja klienta BigQuery
client = bigquery.Client()

@app.route('/ships')
def ships():
    # Pobieramy ostatnie dane pozycji statków np. z ostatnich 2 minut
    query = """
    SELECT
      mmsi,
      latitude,
      longitude,
      cog,
      sog,
      timestamp,
      COALESCE(ship_name, CAST(mmsi AS STRING)) AS ship_name,
      ship_length
    FROM `ais_dataset.ships_positions`
    WHERE timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 MINUTE)
    """
    rows = list(client.query(query).result())
    result = []
    for r in rows:
        result.append({
            'mmsi': r.mmsi,
            'latitude': r.latitude,
            'longitude': r.longitude,
            'cog': r.cog,
            'sog': r.sog,
            'timestamp': r.timestamp.isoformat() if r.timestamp else None,
            'ship_name': r.ship_name,
            'ship_length': r.ship_length
        })
    return jsonify(result)

@app.route('/collisions')
def collisions():
    # Pobieramy kolizje z ostatnich 5 minut i dołączamy nazwy statków (o ile istnieją)
    query = """
    WITH ship_names AS (
      SELECT
        mmsi,
        ANY_VALUE(ship_name) AS ship_name
      FROM `ais_dataset.ships_positions`
      WHERE ship_name IS NOT NULL
      GROUP BY mmsi
    )
    SELECT
      c.mmsi_a,
      c.mmsi_b,
      COALESCE(s1.ship_name, CAST(c.mmsi_a AS STRING)) AS ship1_name,
      COALESCE(s2.ship_name, CAST(c.mmsi_b AS STRING)) AS ship2_name,
      c.timestamp,
      c.cpa,
      c.tcpa,
      c.geohash,
      c.latitude_a,
      c.longitude_a,
      c.latitude_b,
      c.longitude_b
    FROM `ais_dataset.collisions` c
    LEFT JOIN ship_names s1 ON c.mmsi_a = s1.mmsi
    LEFT JOIN ship_names s2 ON c.mmsi_b = s2.mmsi
    WHERE c.timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
    ORDER BY c.timestamp DESC
    """
    rows = list(client.query(query).result())
    result = []
    for r in rows:
        result.append({
            'mmsi_a': r.mmsi_a,
            'mmsi_b': r.mmsi_b,
            'ship1_name': r.ship1_name,
            'ship2_name': r.ship2_name,
            'timestamp': r.timestamp.isoformat() if r.timestamp else None,
            'cpa': r.cpa,
            'tcpa': r.tcpa,
            'geohash': r.geohash,
            'latitude_a': r.latitude_a,
            'longitude_a': r.longitude_a,
            'latitude_b': r.latitude_b,
            'longitude_b': r.longitude_b
        })
    return jsonify(result)

@app.route('/')
def index():
    # Prosta strona index
    return "AIS Collision Detection API"

if __name__ == '__main__':
    # Upewnij się, że masz zmienne środowiskowe ustawione, np. GOOGLE_CLOUD_PROJECT
    # lub zrób to w kodzie, jeśli konieczne.
    app.run(host='0.0.0.0', port=5000, debug=True)