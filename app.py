import os
import json
from flask import Flask, jsonify, render_template
from google.cloud import bigquery

app = Flask(__name__,
            static_folder='static',
            template_folder='templates')

client = bigquery.Client()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/ships')
def ships():
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
    query = """
    WITH latest_positions AS (
      SELECT mmsi, ship_name, timestamp,
      ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY timestamp DESC) as rn
      FROM `ais_dataset.ships_positions`
    )
    SELECT 
      c.mmsi_a,
      c.mmsi_b,
      c.timestamp,
      c.cpa,
      c.tcpa,
      c.geohash,
      c.latitude_a,
      c.longitude_a,
      c.latitude_b,
      c.longitude_b,
      la.ship_name AS ship1_name,
      lb.ship_name AS ship2_name
    FROM `ais_dataset.collisions` c
    LEFT JOIN latest_positions la ON la.mmsi = c.mmsi_a AND la.rn = 1
    LEFT JOIN latest_positions lb ON lb.mmsi = c.mmsi_b AND lb.rn = 1
    WHERE c.tcpa > 0
    ORDER BY c.timestamp DESC
    LIMIT 1000
    """

    rows = list(client.query(query).result())
    result=[]
    for r in rows:
        ship1_name = r.ship1_name if r.ship1_name else str(r.mmsi_a)
        ship2_name = r.ship2_name if r.ship2_name else str(r.mmsi_b)
        if ship1_name == ship2_name and r.mmsi_a != r.mmsi_b:
            ship2_name = f"{ship2_name} ({r.mmsi_b})"
        if r.mmsi_a == r.mmsi_b:
            continue

        result.append({
            'mmsi_a': r.mmsi_a,
            'mmsi_b': r.mmsi_b,
            'timestamp': r.timestamp.isoformat() if r.timestamp else None,
            'cpa': r.cpa,
            'tcpa': r.tcpa,
            'geohash': r.geohash,
            'latitude_a': r.latitude_a,
            'longitude_a': r.longitude_a,
            'latitude_b': r.latitude_b,
            'longitude_b': r.longitude_b,
            'ship1_name': ship1_name,
            'ship2_name': ship2_name
        })
    return jsonify(result)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)