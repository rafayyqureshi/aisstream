import os
import math
from datetime import datetime, timedelta

from flask import Flask, jsonify, render_template, request
from google.cloud import bigquery
from google.cloud import storage

app = Flask(__name__, static_folder='static', template_folder='templates')
client = bigquery.Client()

GCS_HISTORY_BUCKET = os.getenv("GCS_HISTORY_BUCKET", "ais-collision-detection-bucket")
GCS_HISTORY_PREFIX = os.getenv("GCS_HISTORY_PREFIX", "history_collisions/hourly")

# 1) Strona główna
@app.route('/')
def index():
    return render_template('index.html')

# 2) Endpoint /ships – Łączymy ships_positions (dynamic) z ships_static
@app.route('/ships')
def ships():
    """
    Zwraca statki z ostatnich 2 minut, do modułu live.
    Pobiera dim_a..d z tabeli ships_static (LEFT JOIN).
    """
    query = """
    WITH recent AS (
      SELECT
        mmsi,
        latitude,
        longitude,
        cog,
        sog,
        timestamp,
        ANY_VALUE(ship_name) AS dyn_name
      FROM `ais_dataset_us.ships_positions`
      WHERE timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 MINUTE)
      GROUP BY mmsi, latitude, longitude, cog, sog, timestamp
    )
    SELECT
      r.mmsi,
      r.latitude,
      r.longitude,
      r.cog,
      r.sog,
      r.timestamp,
      r.dyn_name AS ship_name,
      s.dim_a,
      s.dim_b,
      s.dim_c,
      s.dim_d
    FROM recent r
    LEFT JOIN `ais_dataset_us.ships_static` s
      ON r.mmsi = s.mmsi
    ORDER BY r.timestamp DESC
    """
    rows = list(client.query(query).result())

    out = []
    for r in rows:
        out.append({
            "mmsi": r.mmsi,
            "latitude": r.latitude,
            "longitude": r.longitude,
            "cog": r.cog,
            "sog": r.sog,
            "timestamp": r.timestamp.isoformat() if r.timestamp else None,
            "ship_name": r.ship_name,   # z recent/dyn_name lub static
            "dim_a": r.dim_a,
            "dim_b": r.dim_b,
            "dim_c": r.dim_c,
            "dim_d": r.dim_d
        })
    return jsonify(out)

# 3) Endpoint /collisions
@app.route('/collisions')
def collisions():
    max_cpa = float(request.args.get('max_cpa', 0.5))
    max_tcpa = float(request.args.get('max_tcpa', 10.0))

    query = f"""
    WITH latestA AS (
      SELECT mmsi, ANY_VALUE(ship_name) AS ship_name, MAX(timestamp) AS latest_ts
      FROM `ais_dataset_us.ships_positions`
      GROUP BY mmsi
    ), latestB AS (
      SELECT mmsi, ANY_VALUE(ship_name) AS ship_name, MAX(timestamp) AS latest_ts
      FROM `ais_dataset_us.ships_positions`
      GROUP BY mmsi
    )
    SELECT
      c.mmsi_a,
      c.mmsi_b,
      c.timestamp,
      c.cpa,
      c.tcpa,
      c.latitude_a,
      c.longitude_a,
      c.latitude_b,
      c.longitude_b,
      la.ship_name AS ship1_name,
      lb.ship_name AS ship2_name
    FROM `ais_dataset_us.collisions` c
    LEFT JOIN latestA la ON la.mmsi = c.mmsi_a
    LEFT JOIN latestB lb ON lb.mmsi = c.mmsi_b
    WHERE c.cpa <= {max_cpa}
      AND c.tcpa <= {max_tcpa}
      AND c.tcpa >= 0
    ORDER BY c.timestamp DESC
    LIMIT 100
    """
    rows = list(client.query(query).result())

    result = []
    for r in rows:
        shipA = r.ship1_name or str(r.mmsi_a)
        shipB = r.ship2_name or str(r.mmsi_b)
        t_str = r.timestamp.isoformat() if r.timestamp else None
        collision_id = None
        if r.timestamp:
            collision_id = f"{r.mmsi_a}_{r.mmsi_b}_{r.timestamp.strftime('%Y%m%d%H%M%S')}"

        result.append({
            'collision_id': collision_id,
            'mmsi_a': r.mmsi_a,
            'mmsi_b': r.mmsi_b,
            'timestamp': t_str,
            'cpa': r.cpa,
            'tcpa': r.tcpa,
            'latitude_a': r.latitude_a,
            'longitude_a': r.longitude_a,
            'latitude_b': r.latitude_b,
            'longitude_b': r.longitude_b,
            'ship1_name': shipA,
            'ship2_name': shipB
        })
    return jsonify(result)

# 4) On-demand CPA/TCPA
@app.route('/calculate_cpa_tcpa')
def calculate_cpa_tcpa():
    mmsi_a = request.args.get('mmsi_a', type=int)
    mmsi_b = request.args.get('mmsi_b', type=int)
    if not mmsi_a or not mmsi_b:
        return jsonify({'error': 'Missing mmsi_a or mmsi_b'}), 400

    # W nowym schemacie, dynamicznie nie mamy "ship_length".
    # Jeśli jednak chcesz tu liczyć CPA/TCPA, musisz ewentualnie
    # zignorować warunek min. 50m (albo dołączyć static i sumować a+b).
    # Poniżej przykład z "2min" – or we do 5 min etc.
    query = f"""
    SELECT
      mmsi,
      latitude,
      longitude,
      cog,
      sog
    FROM `ais_dataset_us.ships_positions`
    WHERE mmsi IN ({mmsi_a}, {mmsi_b})
      AND timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 MINUTE)
    GROUP BY mmsi, latitude, longitude, cog, sog
    ORDER BY MAX(timestamp) DESC
    """
    rows = list(client.query(query).result())

    data_by_mmsi = {}
    for r in rows:
        data_by_mmsi[r.mmsi] = {
            'mmsi': r.mmsi,
            'latitude': r.latitude,
            'longitude': r.longitude,
            'cog': r.cog,
            'sog': r.sog
        }

    if mmsi_a not in data_by_mmsi or mmsi_b not in data_by_mmsi:
        return jsonify({'error': 'No recent data for one or both ships'}), 404

    # Prosty CPA/TCPA – lub zignoruj warunek >50m
    cpa_val = 9999
    tcpa_val = -1

    # Ewentualnie tu dopisujesz swoją logikę
    # cpa_val, tcpa_val = compute_cpa_tcpa(data_by_mmsi[mmsi_a], data_by_mmsi[mmsi_b])

    return jsonify({'cpa': cpa_val, 'tcpa': tcpa_val})

# 5) History – pliki GCS
@app.route('/history')
def history():
    return render_template('history.html')

@app.route("/history_filelist")
def history_filelist():
    days_back = int(request.args.get("days", 7))
    cutoff_utc = datetime.utcnow() - timedelta(days=days_back)

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_HISTORY_BUCKET)
    prefix = GCS_HISTORY_PREFIX.rstrip("/") + "/"

    try:
        blobs = list(bucket.list_blobs(prefix=prefix))
    except Exception as e:
        return jsonify({"error": f"Error listing blobs: {str(e)}"}), 500

    files = []
    for blob in blobs:
        if not blob.time_created:
            continue
        blob_t_utc = blob.time_created.replace(tzinfo=None)
        if blob_t_utc >= cutoff_utc:
            files.append({
                "name": blob.name,
                "time_created": blob.time_created.isoformat()
            })

    files.sort(key=lambda f: f["time_created"])
    return jsonify({"files": files})

@app.route("/history_file")
def history_file():
    filename = request.args.get("file", "")
    if not filename:
        return jsonify({"error": "Missing 'file' parameter"}), 400

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_HISTORY_BUCKET)
    blob = bucket.blob(filename)

    try:
        if not blob.exists():
            return jsonify({"error": f"File not found: {filename}"}), 404
    except Exception as e:
        return jsonify({"error": f"Error checking blob: {str(e)}"}), 500

    try:
        data_str = blob.download_as_text(encoding="utf-8")
    except Exception as e:
        return jsonify({"error": f"Error downloading blob: {str(e)}"}), 500

    return app.response_class(data_str, mimetype="application/json")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)