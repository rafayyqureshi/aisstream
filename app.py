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

##################################################
# Inicjalizacja Flask i BigQuery
##################################################
app = Flask(__name__, static_folder='static', template_folder='templates')
client = bigquery.Client()

##################################################
# Wczytanie klucza API + inne zmienne
##################################################
load_dotenv()
API_KEY_REQUIRED = os.getenv("API_KEY", "Ais-mon")
GCS_HISTORY_BUCKET = os.getenv("GCS_HISTORY_BUCKET", "ais-collision-detection-bucket")
GCS_HISTORY_PREFIX = os.getenv("GCS_HISTORY_PREFIX", "history_collisions/hourly")

##################################################
# Globalne Cache w pamięci
##################################################
SHIPS_CACHE = {"last_update": None, "data": []}
COLLISIONS_CACHE = {"last_update": None, "data": []}

# Słownik statycznych danych statków (mmsi -> {...})
STATIC_DICT = {}
LAST_STATIC_UPDATE = None
STATIC_REFRESH_HOURS = 12  # co 12h odświeżamy dane z tabeli ships_static

logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

##################################################
# Funkcje pomocnicze — obliczenia CPA/TCPA lokalnie
##################################################
def haversine_distance(lat1, lon1, lat2, lon2):
    R = 3440.065
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    c = 2*math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R*c

def to_xy(lat, lon):
    x = lon * 60 * math.cos(math.radians(lat))
    y = lat * 60
    return x, y

def cog_to_vector(cog, sog):
    rad = math.radians(cog)
    vx = sog * math.sin(rad)
    vy = sog * math.cos(rad)
    return vx, vy

def compute_cpa_tcpa(shipA, shipB):
    xA, yA = to_xy(shipA["latitude"], shipA["longitude"])
    xB, yB = to_xy(shipB["latitude"], shipB["longitude"])
    dx, dy = xA - xB, yA - yB

    vxA, vyA = cog_to_vector(shipA["cog"], shipA["sog"])
    vxB, vyB = cog_to_vector(shipB["cog"], shipB["sog"])
    dvx, dvy = vxA - vxB, vyA - vyB
    v2 = dvx**2 + dvy**2

    if v2 == 0:
        tcpa = 0
    else:
        tcpa = - (dx*dvx + dy*dvy) / v2
        if tcpa < 0:
            tcpa = 0

    xA_cpa = xA + vxA * tcpa
    yA_cpa = yA + vyA * tcpa
    xB_cpa = xB + vxB * tcpa
    yB_cpa = yB + vyB * tcpa
    cpa = math.sqrt((xA_cpa - xB_cpa)**2 + (yA_cpa - yB_cpa)**2)
    return cpa, tcpa

##################################################
# Załadowanie ships_static do pamięci
##################################################
def load_ships_static():
    global STATIC_DICT, LAST_STATIC_UPDATE
    query_str = """
    SELECT
      mmsi, ship_name, dim_a, dim_b, dim_c, dim_d
    FROM `ais_dataset_us.ships_static`
    """
    try:
        rows = list(client.query(query_str).result())
        tmp = {}
        for r in rows:
            tmp[r.mmsi] = {
                "ship_name": r.ship_name,
                "dim_a": r.dim_a,
                "dim_b": r.dim_b,
                "dim_c": r.dim_c,
                "dim_d": r.dim_d
            }
        STATIC_DICT = tmp
        LAST_STATIC_UPDATE = datetime.datetime.utcnow()
        app.logger.info(f"[load_ships_static] Załadowano {len(tmp)} wpisów z ships_static.")
    except Exception as e:
        app.logger.error(f"[load_ships_static] Błąd: {e}")

def maybe_refresh_static_dict():
    global LAST_STATIC_UPDATE
    now = datetime.datetime.utcnow()
    if LAST_STATIC_UPDATE is None or (now - LAST_STATIC_UPDATE) > timedelta(hours=STATIC_REFRESH_HOURS):
        load_ships_static()

##################################################
# Mechanizm klucza API
##################################################
@app.before_request
def require_api_key():
    if request.path == '/' or request.path.startswith('/static') or request.path == '/favicon.ico':
        return
    if request.headers.get("X-API-Key") != API_KEY_REQUIRED:
        return jsonify({"error": "Invalid or missing API Key"}), 403

##################################################
# Index
##################################################
@app.route('/')
def index():
    return render_template('index.html')

##################################################
# Endpoint /ships
##################################################
@app.route('/ships')
def ships():
    """
    Pobiera ostatnie wpisy (<= 4 min) z ships_positions,
    wybiera najnowszy per MMSI (ROW_NUMBER), dołącza dane statyczne z pamięci,
    cache 30 sek.
    """
    maybe_refresh_static_dict()

    now = datetime.datetime.utcnow()
    cache_age = 30
    if SHIPS_CACHE["last_update"] and (now - SHIPS_CACHE["last_update"]).total_seconds() < cache_age:
        return jsonify(SHIPS_CACHE["data"])

    query_str = """
    WITH latest_positions AS (
      SELECT
        sp.mmsi,
        sp.latitude,
        sp.longitude,
        sp.cog,
        sp.sog,
        sp.heading,
        sp.timestamp,
        sp.ship_name,
        ROW_NUMBER() OVER (
          PARTITION BY sp.mmsi
          ORDER BY sp.timestamp DESC
        ) AS rn
      FROM `ais_dataset_us.ships_positions` sp
      WHERE sp.timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
    )
    SELECT
      mmsi,
      latitude,
      longitude,
      cog,
      sog,
      heading,
      timestamp,
      ship_name
    FROM latest_positions
    WHERE rn = 1
    ORDER BY timestamp DESC
    """

    try:
        rows = list(client.query(query_str).result())
    except Exception as e:
        app.logger.error(f"Error BQ (/ships): {e}")
        return jsonify({"error": "Query failed"}), 500

    out = []
    for r in rows:
        mmsi = r.mmsi
        # Dopinamy dane statyczne z pamięci
        st = STATIC_DICT.get(mmsi, {})
        final_name = r.ship_name or st.get("ship_name") or "Unknown"
        out.append({
            "mmsi": mmsi,
            "latitude": r.latitude,
            "longitude": r.longitude,
            "cog": r.cog,
            "sog": r.sog,
            "heading": r.heading,
            "timestamp": r.timestamp.isoformat() if r.timestamp else None,
            "ship_name": final_name,
            "dim_a": st.get("dim_a"),
            "dim_b": st.get("dim_b"),
            "dim_c": st.get("dim_c"),
            "dim_d": st.get("dim_d")
        })

    SHIPS_CACHE["data"] = out
    SHIPS_CACHE["last_update"] = now
    return jsonify(out)

##################################################
# Endpoint /collisions
##################################################
@app.route('/collisions')
def collisions():
    """
    Pobiera ostatnie wpisy (<= 4 min) z collisions,
    najnowszy per para (ROW_NUMBER), filtruje po cpa/tcpa, cache 10 sek.
    collisions zawiera już ship_name_a, ship_name_b, lat/long itp.
    """
    maybe_refresh_static_dict()

    now = datetime.datetime.utcnow()
    cache_age = 10
    if COLLISIONS_CACHE["last_update"] and (now - COLLISIONS_CACHE["last_update"]).total_seconds() < cache_age:
        return jsonify(COLLISIONS_CACHE["data"])

    max_cpa = float(request.args.get('max_cpa', 0.5))
    max_tcpa = float(request.args.get('max_tcpa', 10.0))

    query_str = """
    WITH recent_collisions AS (
      SELECT
        c.mmsi_a,
        c.ship_name_a,
        c.mmsi_b,
        c.ship_name_b,
        c.timestamp,
        c.cpa,
        c.tcpa,
        c.distance,
        c.is_active,
        c.latitude_a,
        c.longitude_a,
        c.latitude_b,
        c.longitude_b,
        ROW_NUMBER() OVER (
          PARTITION BY LEAST(c.mmsi_a, c.mmsi_b), GREATEST(c.mmsi_a, c.mmsi_b)
          ORDER BY c.timestamp DESC
        ) AS rn
      FROM `ais_dataset_us.collisions` c
      WHERE c.timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
    )
    SELECT
      mmsi_a,
      ship_name_a,
      mmsi_b,
      ship_name_b,
      timestamp,
      cpa,
      tcpa,
      distance,
      is_active,
      latitude_a,
      longitude_a,
      latitude_b,
      longitude_b
    FROM recent_collisions
    WHERE rn = 1
      AND cpa <= @param_cpa
      AND tcpa <= @param_tcpa
      AND tcpa >= 0
    ORDER BY timestamp DESC
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
        app.logger.error(f"Error BQ (/collisions): {e}")
        return jsonify({"error": "Query failed"}), 500

    out = []
    for r in rows:
        t_str = r.timestamp.isoformat() if r.timestamp else None
        collision_id = None
        if r.timestamp:
            collision_id = f"{r.mmsi_a}_{r.mmsi_b}_{r.timestamp.strftime('%Y%m%d%H%M%S')}"
        out.append({
            "collision_id": collision_id,
            "mmsi_a": r.mmsi_a,
            "ship_name_a": r.ship_name_a,
            "mmsi_b": r.mmsi_b,
            "ship_name_b": r.ship_name_b,
            "timestamp": t_str,
            "cpa": r.cpa,
            "tcpa": r.tcpa,
            "distance": r.distance,
            "is_active": r.is_active,
            "latitude_a": r.latitude_a,
            "longitude_a": r.longitude_a,
            "latitude_b": r.latitude_b,
            "longitude_b": r.longitude_b
        })

    COLLISIONS_CACHE["data"] = out
    COLLISIONS_CACHE["last_update"] = now
    return jsonify(out)



##################################################
# Moduł HISTORY (opcjonalny)
##################################################
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
        app.logger.error(f"[/history_filelist] GCS error: {e}")
        return jsonify({"error": str(e)}), 500

    files = []
    for blob in blobs:
        if blob.time_created and blob.time_created.replace(tzinfo=None) >= cutoff_utc:
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
        app.logger.error(f"[/history_file] GCS check error: {e}")
        return jsonify({"error": str(e)}), 500

    try:
        data_str = blob.download_as_text(encoding="utf-8")
    except Exception as e:
        app.logger.error(f"[/history_file] GCS download error: {e}")
        return jsonify({"error": str(e)}), 500

    return app.response_class(data_str, mimetype="application/json")

##################################################
# Uruchomienie
##################################################
if __name__ == '__main__':
    load_ships_static()
    app.run(host='0.0.0.0', port=5000, debug=False)