#!/usr/bin/env python3
import os
import math
import json
import logging
import datetime
from datetime import timedelta
from dotenv import load_dotenv

from flask import Flask, jsonify, render_template, request
from google.cloud import bigquery
from google.cloud import storage

load_dotenv()

app = Flask(__name__, static_folder='static', template_folder='templates')
client = bigquery.Client()

# Wczytanie klucza API (domyślnie "Ais-mon")
API_KEY_REQUIRED = os.getenv("API_KEY", "Ais-mon")

# Konfiguracja GCS dla modułu history
GCS_HISTORY_BUCKET = os.getenv("GCS_HISTORY_BUCKET", "ais-collision-detection-bucket")
GCS_HISTORY_PREFIX = os.getenv("GCS_HISTORY_PREFIX", "history_collisions/hourly")

# Prosty cache w pamięci dla endpointów /ships i /collisions
SHIPS_CACHE = {"last_update": None, "data": []}
COLLISIONS_CACHE = {"last_update": None, "data": []}

logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

####################################################################
# Mechanizm sprawdzania klucza API
####################################################################
@app.before_request
def require_api_key():
    # Zezwalamy na dostęp do '/', 'favicon.ico' i plików statycznych bez API Key
    if request.path in ['/', '/favicon.ico'] or request.path.startswith('/static'):
        return
    # Sprawdzamy klucz w nagłówku
    headers_key = request.headers.get("X-API-Key")
    if headers_key != API_KEY_REQUIRED:
        return jsonify({"error": "Invalid or missing API Key"}), 403

####################################################################
# Strona główna
####################################################################
@app.route('/')
def index():
    return render_template('index.html')

####################################################################
# Endpoint /ships: najnowsze dane statków (ostatnie 3 min)
####################################################################
@app.route('/ships')
def ships():
    """
    Zwraca najnowszy rekord dla każdego mmsi (z ostatnich 3 min)
    i dołącza (LEFT JOIN) dane statyczne (wymiary, ship_name).
    Dla odciążenia BigQuery używa prostego cache w pamięci na 30 s.
    """
    now = datetime.datetime.utcnow()
    cache_age_sec = 30  # 30 sekund cache

    # Sprawdzamy cache
    if SHIPS_CACHE["last_update"] and (now - SHIPS_CACHE["last_update"]).total_seconds() < cache_age_sec:
        return jsonify(SHIPS_CACHE["data"])

    query_str = """
    WITH latest_positions AS (
      SELECT *
      FROM (
        SELECT
          sp.* EXCEPT(row_num),
          ROW_NUMBER() OVER (
            PARTITION BY sp.mmsi
            ORDER BY sp.timestamp DESC
          ) AS row_num
        FROM `ais_dataset_us.ships_positions` sp
        WHERE sp.timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
      )
      WHERE row_num = 1
    )
    SELECT
      lp.mmsi,
      lp.latitude,
      lp.longitude,
      lp.cog,
      lp.sog,
      lp.heading,
      lp.timestamp,
      COALESCE(s.ship_name, lp.ship_name) AS ship_name,
      s.dim_a,
      s.dim_b,
      s.dim_c,
      s.dim_d
    FROM latest_positions lp
    LEFT JOIN `ais_dataset_us.ships_static` s
      ON lp.mmsi = s.mmsi
    ORDER BY lp.timestamp DESC
    """

    try:
        rows = list(client.query(query_str).result())
    except Exception as e:
        app.logger.error(f"Błąd zapytania BigQuery (/ships): {e}")
        return jsonify({"error": "Query failed"}), 500

    out = []
    for r in rows:
        out.append({
            "mmsi": r.mmsi,
            "latitude": r.latitude,
            "longitude": r.longitude,
            "cog": r.cog,
            "sog": r.sog,
            "heading": r.heading,
            "timestamp": r.timestamp.isoformat() if r.timestamp else None,
            "ship_name": r.ship_name,
            "dim_a": r.dim_a,
            "dim_b": r.dim_b,
            "dim_c": r.dim_c,
            "dim_d": r.dim_d
        })

    SHIPS_CACHE["data"] = out
    SHIPS_CACHE["last_update"] = now
    return jsonify(out)

####################################################################
# Endpoint /collisions: najnowsze wpisy kolizyjne (ostatnie 3 min)
####################################################################
@app.route('/collisions')
def collisions():
    """
    Zwraca najnowszy (ostatni) wpis kolizyjny dla każdej pary mmsi_a, mmsi_b
    z tabeli collisions, ograniczając się do zdarzeń nie starszych niż 3 min.
    Parametry max_cpa i max_tcpa pozwalają dodatkowo odfiltrować wyniki.
    Cache w pamięci na 10 s, aby zmniejszyć liczbę zapytań do BigQuery.
    """
    now = datetime.datetime.utcnow()
    cache_age_sec = 10

    # Sprawdzenie cache
    if COLLISIONS_CACHE["last_update"] and (now - COLLISIONS_CACHE["last_update"]).total_seconds() < cache_age_sec:
        return jsonify(COLLISIONS_CACHE["data"])

    max_cpa = float(request.args.get('max_cpa', 0.5))
    max_tcpa = float(request.args.get('max_tcpa', 10.0))

    query_str = f"""
    WITH recent_collisions AS (
      SELECT
        c.* EXCEPT(row_num),
        ROW_NUMBER() OVER (
          PARTITION BY LEAST(c.mmsi_a,c.mmsi_b), GREATEST(c.mmsi_a,c.mmsi_b)
          ORDER BY c.timestamp DESC
        ) AS row_num
      FROM `ais_dataset_us.collisions` c
      WHERE c.timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
    )
    SELECT
      mmsi_a,
      mmsi_b,
      timestamp,
      cpa,
      tcpa,
      latitude_a,
      longitude_a,
      latitude_b,
      longitude_b
    FROM recent_collisions
    WHERE row_num = 1
      AND cpa <= @param_cpa
      AND tcpa <= @param_tcpa
      AND tcpa >= 0
    ORDER BY timestamp DESC
    LIMIT 100
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("param_cpa",  "FLOAT64", max_cpa),
            bigquery.ScalarQueryParameter("param_tcpa", "FLOAT64", max_tcpa),
        ]
    )

    try:
        rows = list(client.query(query_str, job_config=job_config).result())
    except Exception as e:
        app.logger.error(f"Błąd zapytania BigQuery (/collisions): {e}")
        return jsonify({"error": "Query failed"}), 500

    # W tym wariancie nie dołączamy statycznych nazw statków,
    # zakładamy że collisions ma minimalne dane (mmsi, cpa, itp.).
    result = []
    for r in rows:
        t_str = r.timestamp.isoformat() if r.timestamp else None
        collision_id = None
        if r.timestamp:
            collision_id = f"{r.mmsi_a}_{r.mmsi_b}_{r.timestamp.strftime('%Y%m%d%H%M%S')}"
        result.append({
            "collision_id": collision_id,
            "mmsi_a": r.mmsi_a,
            "mmsi_b": r.mmsi_b,
            "timestamp": t_str,
            "cpa": r.cpa,
            "tcpa": r.tcpa,
            "latitude_a": r.latitude_a,
            "longitude_a": r.longitude_a,
            "latitude_b": r.latitude_b,
            "longitude_b": r.longitude_b
        })

    COLLISIONS_CACHE["data"] = result
    COLLISIONS_CACHE["last_update"] = now
    return jsonify(result)

####################################################################
# Endpoint /calculate_cpa_tcpa: obliczenia lokalne
####################################################################
@app.route('/calculate_cpa_tcpa')
def calculate_cpa_tcpa_endpoint():
    """
    Wywoływane z front-endu (np. app.js), żeby na żądanie
    obliczyć cpa/tcpa/dystans między 2 statkami.
    """
    mmsi_a = request.args.get('mmsi_a', type=int)
    mmsi_b = request.args.get('mmsi_b', type=int)
    if not mmsi_a or not mmsi_b:
        return jsonify({'error': 'Missing mmsi_a or mmsi_b'}), 400

    query = f"""
    WITH latest_positions AS (
      SELECT * EXCEPT(row_num)
      FROM (
        SELECT
          sp.*,
          ROW_NUMBER() OVER (PARTITION BY sp.mmsi ORDER BY sp.timestamp DESC) AS row_num
        FROM `ais_dataset_us.ships_positions` sp
        WHERE sp.timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
          AND sp.mmsi IN ({mmsi_a}, {mmsi_b})
      )
      WHERE row_num = 1
    )
    SELECT
      mmsi,
      latitude,
      longitude,
      cog,
      sog
    FROM latest_positions
    """
    try:
        rows = list(client.query(query).result())
    except Exception as e:
        app.logger.error(f"Błąd zapytania BigQuery (/calculate_cpa_tcpa): {e}")
        return jsonify({"error": "Query failed"}), 500

    data_by_mmsi = {}
    for r in rows:
        data_by_mmsi[r.mmsi] = {
            'mmsi': r.mmsi,
            'latitude': r.latitude,
            'longitude': r.longitude,
            'cog': r.cog,
            'sog': r.sog
        }

    # Upewniamy się, że oba statki są w data_by_mmsi
    if mmsi_a not in data_by_mmsi or mmsi_b not in data_by_mmsi:
        return jsonify({'error': 'No recent data for one or both ships'}), 404

    # Lokalne obliczenia cpa/tcpa/distance
    shipA = data_by_mmsi[mmsi_a]
    shipB = data_by_mmsi[mmsi_b]

    def to_xy(lat, lon):
        x = lon * 60 * math.cos(math.radians(lat))
        y = lat * 60
        return x, y

    def cog_to_vector(cog, sog):
        rad = math.radians(cog)
        vx = sog * math.sin(rad)
        vy = sog * math.cos(rad)
        return vx, vy

    def haversine_distance(lat1, lon1, lat2, lon2):
        R = 3440.065
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
        c = 2*math.atan2(math.sqrt(a), math.sqrt(1-a))
        return R*c

    def compute_cpa_tcpa(sA, sB):
        xA, yA = to_xy(sA["latitude"], sA["longitude"])
        xB, yB = to_xy(sB["latitude"], sB["longitude"])
        dx = xA - xB
        dy = yA - yB
        vxA, vyA = cog_to_vector(sA["cog"], sA["sog"])
        vxB, vyB = cog_to_vector(sB["cog"], sB["sog"])
        dvx = vxA - vxB
        dvy = vyA - vyB
        v2 = dvx**2 + dvy**2
        if v2 == 0:
            tcpa = 0
        else:
            tcpa = - (dx*dvx + dy*dvy)/v2
        if tcpa < 0:
            tcpa = 0
        xA_cpa = xA + vxA*tcpa
        yA_cpa = yA + vyA*tcpa
        xB_cpa = xB + vxB*tcpa
        yB_cpa = yB + vyB*tcpa
        cpa = math.sqrt((xA_cpa - xB_cpa)**2 + (yA_cpa - yB_cpa)**2)
        return cpa, tcpa

    cpa_val, tcpa_val = compute_cpa_tcpa(shipA, shipB)
    dist_val = haversine_distance(shipA["latitude"], shipA["longitude"],
                                  shipB["latitude"], shipB["longitude"])

    return jsonify({
        'cpa': cpa_val,
        'tcpa': tcpa_val,
        'distance': dist_val
    })

####################################################################
# Endpointy modułu HISTORY (opcjonalne)
####################################################################
@app.route('/history')
def history():
    return render_template('history.html')

@app.route("/history_filelist")
def history_filelist():
    days_back = int(request.args.get("days", 7))
    cutoff_utc = datetime.datetime.utcnow() - timedelta(days=days_back)
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_HISTORY_BUCKET)
    prefix = GCS_HISTORY_PREFIX.rstrip("/") + "/"
    try:
        blobs = list(bucket.list_blobs(prefix=prefix))
    except Exception as e:
        app.logger.error(f"Błąd przy listowaniu blobów GCS: {e}")
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
        app.logger.error(f"Błąd przy sprawdzaniu blobu GCS: {e}")
        return jsonify({"error": f"Error checking blob: {str(e)}"}), 500
    try:
        data_str = blob.download_as_text(encoding="utf-8")
    except Exception as e:
        app.logger.error(f"Błąd pobierania pliku GCS: {e}")
        return jsonify({"error": f"Error downloading blob: {str(e)}"}), 500
    return app.response_class(data_str, mimetype="application/json")

####################################################################
# Uruchom serwer
####################################################################
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)