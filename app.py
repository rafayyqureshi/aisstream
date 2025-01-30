import os
import math
import logging
from datetime import datetime, timedelta

from flask import Flask, jsonify, render_template, request
from google.cloud import bigquery
from google.cloud import storage

# Import wspólnej funkcji CPA/TCPA z cpa_utils.py
from cpa_utils import compute_cpa_tcpa

app = Flask(__name__, static_folder='static', template_folder='templates')
client = bigquery.Client()

# Wczytanie klucza API z .env (domyślnie "Ais-mon")
API_KEY_REQUIRED = os.getenv("API_KEY", "Ais-mon")

# Konfiguracja GCS do modułu history
GCS_HISTORY_BUCKET = os.getenv("GCS_HISTORY_BUCKET", "ais-collision-detection-bucket")
GCS_HISTORY_PREFIX = os.getenv("GCS_HISTORY_PREFIX", "history_collisions/hourly")

# Prosty cache w pamięci dla /ships i /collisions
SHIPS_CACHE = {
    "last_update": None,
    "data": []
}

COLLISIONS_CACHE = {
    "last_update": None,
    "data": []
}

# Ustaw poziom logowania (opcjonalnie)
logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)


###########################################################
# Mechanizm sprawdzania klucza API
###########################################################
@app.before_request

def require_api_key():
    # 1. Zezwól na dostęp do /, /favicon.ico i plików statycznych (katalog /static).
    #    Bo tam nie potrzebujesz klucza – to ma być publicznie dostępne, by strona mogła się wczytać.
    if request.path == '/' or request.path.startswith('/static') or request.path == '/favicon.ico':
        return
    
    # 2. Dla pozostałych endpointów sprawdź klucz
    headers_key = request.headers.get("X-API-Key")
    if headers_key != API_KEY_REQUIRED:
        return jsonify({"error": "Invalid or missing API Key"}), 403


###########################################################
# Endpoints
###########################################################

# 1) Strona główna – Live Map
@app.route('/')
def index():
    """
    Frontend LIVE (index.html).
    """
    return render_template('index.html')


# 2) Endpoint /ships
@app.route('/ships')
def ships():
    """
    Zwraca statki z ostatnich 2 minut, do modułu LIVE.
    Dołączamy heading (z positions) i wymiary (dim_a..d) z ships_static.
    Wprowadzony jest caching na 15 sekund, aby zredukować obciążenie BigQuery.
    """
    now = datetime.utcnow()
    cache_age = 15  # sekundy

    # Jeśli cache aktualny (mniej niż 15s), zwracamy z pamięci
    if (SHIPS_CACHE["last_update"] is not None and
        (now - SHIPS_CACHE["last_update"]).total_seconds() < cache_age):
        return jsonify(SHIPS_CACHE["data"])

    query = """
    WITH recent AS (
      SELECT
        mmsi,
        latitude,
        longitude,
        cog,
        sog,
        heading,
        timestamp,
        ANY_VALUE(ship_name) AS dyn_name
      FROM `ais_dataset_us.ships_positions`
      WHERE timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 MINUTE)
      GROUP BY mmsi, latitude, longitude, cog, sog, heading, timestamp
    )
    SELECT
      r.mmsi,
      r.latitude,
      r.longitude,
      r.cog,
      r.sog,
      r.heading,
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

    try:
        rows = list(client.query(query).result())
    except Exception as e:
        app.logger.error(f"Błąd zapytania BigQuery (ships): {e}")
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

    # Zapis do cache
    SHIPS_CACHE["data"] = out
    SHIPS_CACHE["last_update"] = now

    return jsonify(out)


# 3) Endpoint /collisions
@app.route('/collisions')
def collisions():
    """
    Zwraca listę kolizji w collisions,
    filtrowaną parametrami max_cpa, max_tcpa.
    Odrzucamy kolizje, jeśli "moment kolizji" (timestamp + tcpa) już minął.
    Wprowadzono caching (np. 15 sekund).
    """
    now = datetime.utcnow()
    cache_age = 15  # sekundy

    # Sprawdzamy cache
    if (COLLISIONS_CACHE["last_update"] is not None and
        (now - COLLISIONS_CACHE["last_update"]).total_seconds() < cache_age):
        return jsonify(COLLISIONS_CACHE["data"])

    max_cpa = float(request.args.get('max_cpa', 0.5))
    max_tcpa = float(request.args.get('max_tcpa', 10.0))

    query_str = """
    WITH latestA AS (
      SELECT
        mmsi,
        ANY_VALUE(ship_name) AS ship_name,
        MAX(timestamp) AS latest_ts
      FROM `ais_dataset_us.ships_positions`
      GROUP BY mmsi
    ),
    latestB AS (
      SELECT
        mmsi,
        ANY_VALUE(ship_name) AS ship_name,
        MAX(timestamp) AS latest_ts
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
    WHERE c.cpa <= @param_cpa
      AND c.tcpa <= @param_tcpa
      AND c.tcpa >= 0
      AND TIMESTAMP_ADD(c.timestamp, INTERVAL CAST(c.tcpa AS INT64) MINUTE) > CURRENT_TIMESTAMP()
    ORDER BY c.timestamp DESC
    LIMIT 100
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("param_cpa", "FLOAT64", max_cpa),
            bigquery.ScalarQueryParameter("param_tcpa", "FLOAT64", max_tcpa),
        ]
    )

    try:
        rows = list(client.query(query_str, job_config=job_config).result())
    except Exception as e:
        app.logger.error(f"Błąd zapytania BigQuery (collisions): {e}")
        return jsonify({"error": "Query failed"}), 500

    result = []
    for r in rows:
        shipA = r.ship1_name or str(r.mmsi_a)
        shipB = r.ship2_name or str(r.mmsi_b)
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
            "longitude_b": r.longitude_b,
            "ship1_name": shipA,
            "ship2_name": shipB
        })

    # Zapis do cache
    COLLISIONS_CACHE["data"] = result
    COLLISIONS_CACHE["last_update"] = now

    return jsonify(result)


# 4) On-demand CPA/TCPA
@app.route('/calculate_cpa_tcpa')
def calculate_cpa_tcpa_endpoint():
    """
    Wywoływane z front-endu (app.js)
    w momencie zaznaczenia 2 statków (selectedShips).
    Używamy cpa_utils.compute_cpa_tcpa().
    """
    mmsi_a = request.args.get('mmsi_a', type=int)
    mmsi_b = request.args.get('mmsi_b', type=int)
    if not mmsi_a or not mmsi_b:
        return jsonify({'error': 'Missing mmsi_a or mmsi_b'}), 400

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

    try:
        rows = list(client.query(query).result())
    except Exception as e:
        app.logger.error(f"Błąd zapytania BigQuery (calculate_cpa_tcpa): {e}")
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

    if mmsi_a not in data_by_mmsi or mmsi_b not in data_by_mmsi:
        return jsonify({'error': 'No recent data for one or both ships'}), 404

    # Wywołanie wspólnej funkcji z cpa_utils
    cpa_val, tcpa_val = compute_cpa_tcpa(data_by_mmsi[mmsi_a], data_by_mmsi[mmsi_b])
    return jsonify({'cpa': cpa_val, 'tcpa': tcpa_val})


# 5) Moduł HISTORY (pliki GCS)
@app.route('/history')
def history():
    """
    Frontend HISTORY (history.html).
    """
    return render_template('history.html')

@app.route("/history_filelist")
def history_filelist():
    """
    Zwraca listę plików w GCS (prefix=GCS_HISTORY_PREFIX)
    z ostatnich X dni (domyślnie 7), do front-endu history_app.js
    """
    days_back = int(request.args.get("days", 7))
    cutoff_utc = datetime.utcnow() - timedelta(days=days_back)

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
    """
    Zwraca zawartość konkretnego pliku JSON w GCS,
    generowanego przez pipeline history.
    """
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


###########################################################
# Uruchom
###########################################################
if __name__ == '__main__':
    # W produkcji debug=False
    app.run(host='0.0.0.0', port=5000, debug=False)