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
    # Renderuje index.html jako stronę główną
    return render_template('index.html')

@app.route('/ships')
def ships():
    # Pobiera ostatnie dane pozycji statków np. z ostatnich 2 minut
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
    query = f"""
    SELECT mmsi_a,mmsi_b, timestamp, cpa, tcpa, geohash,
           latitude_a, longitude_a, latitude_b, longitude_b,
           (SELECT ship_name FROM `ais_dataset.ships_positions` 
            WHERE mmsi=mmsi_a ORDER BY timestamp DESC LIMIT 1) AS ship1_name,
           (SELECT ship_name FROM `ais_dataset.ships_positions` 
            WHERE mmsi=mmsi_b ORDER BY timestamp DESC LIMIT 1) AS ship2_name
    FROM `ais_dataset.collisions`
    WHERE tcpa>0 
    ORDER BY timestamp DESC
    LIMIT 1000
    """
    rows = list(client.query(query).result())
    result=[]
    for r in rows:
        result.append({
            'mmsi_a':r.mmsi_a,
            'mmsi_b':r.mmsi_b,
            'timestamp':r.timestamp.isoformat(),
            'cpa':r.cpa,
            'tcpa':r.tcpa,
            'geohash':r.geohash,
            'latitude_a':r.latitude_a,
            'longitude_a':r.longitude_a,
            'latitude_b':r.latitude_b,
            'longitude_b':r.longitude_b,
            'ship1_name':r.ship1_name,
            'ship2_name':r.ship2_name
        })
    return jsonify(result)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)