import os
import json
from flask import Flask, jsonify
from google.cloud import bigquery

app = Flask(__name__)

client = bigquery.Client()

@app.route('/ships')
def ships():
    query = """
    SELECT mmsi, latitude, longitude, cog, sog, timestamp, ship_name, ship_length
    FROM `ais_dataset.ships_positions`
    WHERE timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MINUTE)
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
            'timestamp': r.timestamp.isoformat(),
            'ship_name': r.ship_name,
            'ship_length': r.ship_length
        })
    return jsonify(result)

@app.route('/collisions')
def collisions():
    query = """
    SELECT
      ship1_mmsi as mmsi_a, ship2_mmsi as mmsi_b,
      ship1_name, ship2_name,
      ship1_latitude as latitude_a, ship1_longitude as longitude_a,
      ship2_latitude as latitude_b, ship2_longitude as longitude_b,
      distance_nm as cpa, tcpa_min as tcpa
    FROM `ais_dataset.collisions`
    WHERE timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
    """
    rows = list(client.query(query).result())
    result = []
    for r in rows:
        result.append({
            'mmsi_a': r.mmsi_a,
            'mmsi_b': r.mmsi_b,
            'ship1_name': r.ship1_name,
            'ship2_name': r.ship2_name,
            'ship1_mmsi': r.mmsi_a,
            'ship2_mmsi': r.mmsi_b,
            'latitude_a': r.latitude_a,
            'longitude_a': r.longitude_a,
            'latitude_b': r.latitude_b,
            'longitude_b': r.longitude_b,
            'cpa': r.cpa,
            'tcpa': r.tcpa
        })
    return jsonify(result)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)