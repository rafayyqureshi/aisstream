import os
import json
import math
from datetime import datetime, timedelta

from flask import Flask, jsonify, render_template, request
from google.cloud import bigquery
from google.cloud import storage  # ← Dodatkowo, do obsługi GCS

app = Flask(
    __name__,
    static_folder='static',
    template_folder='templates'
)

# Klient BigQuery (moduł live i ewentualnie “old” history)
client = bigquery.Client()

# Parametry – np. nazwa bucketu z plikami generowanymi przez pipeline history
# Możesz to ustawić w zmiennej środowiskowej, np. GCS_HISTORY_BUCKET="..."
GCS_HISTORY_BUCKET = os.getenv("GCS_HISTORY_BUCKET", "ais-collision-detection-bucket")
GCS_HISTORY_PREFIX = os.getenv("GCS_HISTORY_PREFIX", "history_collisions/hourly")

########################################################################
# Funkcje pomocnicze (dla modułu live), NIE USUWAMY
########################################################################

def compute_cpa_tcpa(ship_a, ship_b):
    """
    Oblicza (CPA, TCPA) przybliżoną metodą geograficzną.
    Zwraca (9999, -1) jeśli ship_length < 50 lub brakuje danych, lub TCPA < 0.
    """
    if ship_a.get('ship_length') is None or ship_b.get('ship_length') is None:
        return (9999, -1)
    if ship_a['ship_length'] < 50 or ship_b['ship_length'] < 50:
        return (9999, -1)

    latRef = (ship_a['latitude'] + ship_b['latitude']) / 2.0
    scaleLat = 111000.0
    scaleLon = 111000.0 * math.cos(math.radians(latRef))

    def toXY(lat, lon):
        return [lon * scaleLon, lat * scaleLat]

    xA, yA = toXY(ship_a['latitude'], ship_a['longitude'])
    xB, yB = toXY(ship_b['latitude'], ship_b['longitude'])

    sogA = float(ship_a.get('sog', 0) or 0)
    sogB = float(ship_b.get('sog', 0) or 0)

    def cogToVector(cogDeg, sogKn):
        r = math.radians(cogDeg or 0)
        vx = sogKn * math.sin(r)
        vy = sogKn * math.cos(r)
        return vx, vy

    vxA, vyA = cogToVector(ship_a['cog'], sogA)
    vxB, vyB = cogToVector(ship_b['cog'], sogB)

    dx = xA - xB
    dy = yA - yB
    speedScale = 1852.0 / 60.0  # nm/h => m/min

    dvx = (vxA - vxB) * speedScale
    dvy = (vyA - vyB) * speedScale

    VV = dvx**2 + dvy**2
    PV = dx*dvx + dy*dvy

    if VV == 0:
        tcpa = 0.0
    else:
        tcpa = -PV / VV
    if tcpa < 0:
        return (9999, -1)

    xA2 = xA + vxA*speedScale*tcpa
    yA2 = yA + vyA*speedScale*tcpa
    xB2 = xB + vxB*speedScale*tcpa
    yB2 = yB + vyB*speedScale*tcpa

    dist_m = math.sqrt((xA2 - xB2)**2 + (yA2 - yB2)**2)
    distNm = dist_m / 1852.0
    return (distNm, tcpa)

########################################################################
# ENDPOINTY MODUŁU LIVE – NIE ZMIENIAMY
########################################################################

@app.route('/')
def index():
    """Strona główna – live map."""
    return render_template('index.html')

@app.route('/ships')
def ships():
    """
    Zwraca statki (ostatnie 2 min), do modułu live.
    """
    query = """
    SELECT
      mmsi,
      latitude,
      longitude,
      cog,
      sog,
      timestamp,
      ANY_VALUE(ship_name) AS ship_name,
      ANY_VALUE(ship_length) AS ship_length
    FROM `ais_dataset_us.ships_positions`
    WHERE timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 MINUTE)
    GROUP BY mmsi, latitude, longitude, cog, sog, timestamp
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
    """
    Zwraca kolizje (filtrowalne cpa/tcpa) – do modułu live.
    """
    max_cpa = float(request.args.get('max_cpa', 0.5))
    max_tcpa = float(request.args.get('max_tcpa', 10.0))

    query = f"""
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
        ts    = r.timestamp.isoformat() if r.timestamp else None

        collision_id = None
        if r.timestamp:
            collision_id = f"{r.mmsi_a}_{r.mmsi_b}_{r.timestamp.strftime('%Y%m%d%H%M%S')}"

        result.append({
            'collision_id': collision_id,
            'mmsi_a': r.mmsi_a,
            'mmsi_b': r.mmsi_b,
            'timestamp': ts,
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

@app.route('/calculate_cpa_tcpa')
def calculate_cpa_tcpa():
    """
    On-demand CPA/TCPA (ostatnie 5 min) – do modułu live.
    """
    mmsi_a = request.args.get('mmsi_a', type=int)
    mmsi_b = request.args.get('mmsi_b', type=int)
    if not mmsi_a or not mmsi_b:
        return jsonify({'error': 'Missing mmsi_a or mmsi_b'}), 400

    query = f"""
    SELECT
      mmsi, latitude, longitude, cog, sog,
      ANY_VALUE(ship_length) as ship_length
    FROM `ais_dataset_us.ships_positions`
    WHERE mmsi IN ({mmsi_a}, {mmsi_b})
      AND timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
    GROUP BY mmsi, latitude, longitude, cog, sog
    ORDER BY MAX(timestamp) DESC
    """
    rows = list(client.query(query).result())

    data_by_mmsi = {}
    for r in rows:
        if r.mmsi not in data_by_mmsi:
            data_by_mmsi[r.mmsi] = {
                'mmsi': r.mmsi,
                'latitude': r.latitude,
                'longitude': r.longitude,
                'cog': r.cog,
                'sog': r.sog,
                'ship_length': r.ship_length
            }

    if mmsi_a not in data_by_mmsi or mmsi_b not in data_by_mmsi:
        return jsonify({'error': 'No recent data for one or both ships'}), 404

    cpa, tcpa = compute_cpa_tcpa(data_by_mmsi[mmsi_a], data_by_mmsi[mmsi_b])
    return jsonify({'cpa': cpa, 'tcpa': tcpa})

########################################################################
# HISTORY – dawny endpoint w BQ (opcjonalnie zostawiamy)
########################################################################

@app.route('/history')
def history():
    """Strona startowa dla modułu history – wczytuje history.html"""
    return render_template('history.html')

@app.route('/history_collisions')
def history_collisions():
    """
    Stary /history_collisions – z BQ, ewentualnie do wycofania.
    Pozostawiamy, bo był w poprzedniej wersji.
    """
    day_offset = int(request.args.get('day', 0))
    max_cpa = float(request.args.get('max_cpa', 0.5))
    the_date = (datetime.utcnow() + timedelta(days=day_offset)).date()

    query = f"""
    WITH base AS (
      SELECT
        c.mmsi_a, c.mmsi_b, c.timestamp AS coll_ts,
        c.cpa, c.tcpa,
        c.latitude_a, c.longitude_a,
        c.latitude_b, c.longitude_b
      FROM `ais_dataset_us.collisions` c
      WHERE DATE(c.timestamp) = '{the_date}'
        AND c.cpa <= {max_cpa}
        AND c.tcpa >= 0
        AND c.tcpa <= 10
    )
    SELECT *,
      CONCAT(CAST(mmsi_a AS STRING), '_', CAST(mmsi_b AS STRING), '_',
             FORMAT_TIMESTAMP('%Y%m%d%H%M%S', coll_ts)) AS collision_id
    FROM base
    ORDER BY coll_ts DESC
    LIMIT 200
    """
    rows = list(client.query(query).result())
    result = []
    for r in rows:
        result.append({
            'collision_id': r.collision_id,
            'mmsi_a': r.mmsi_a,
            'mmsi_b': r.mmsi_b,
            'timestamp': r.coll_ts.isoformat() if r.coll_ts else None,
            'cpa': r.cpa,
            'tcpa': r.tcpa,
            'latitude_a': r.latitude_a,
            'longitude_a': r.longitude_a,
            'latitude_b': r.latitude_b,
            'longitude_b': r.longitude_b
        })
    return jsonify(result)

########################################################################
# NOWE DLA MODUŁU HISTORY – PLIKOWE (z GCS)
########################################################################

@app.route("/history_filelist")
def history_filelist():
    """
    Zwraca listę plików z GCS (np. z ostatnich X dni).
    Pipeline history generuje pliki w GCS w folderze `GCS_HISTORY_PREFIX`.
    Przykład: collisions_YYYYmmddHHMMSS.json
    """
    days_back = int(request.args.get("days", 7))
    cutoff = datetime.utcnow() - timedelta(days=days_back)

    from google.cloud import storage
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_HISTORY_BUCKET)

    prefix = GCS_HISTORY_PREFIX.rstrip("/") + "/"
    blobs = list(bucket.list_blobs(prefix=prefix))

    files = []
    for blob in blobs:
        if blob.time_created and blob.time_created >= cutoff:
            files.append({
                "name": blob.name,
                "time_created": blob.time_created.isoformat()
            })
    # Sortuj chronologicznie
    files.sort(key=lambda f: f["time_created"])
    return jsonify({"files": files})

@app.route("/history_file")
def history_file():
    """
    Zwraca zawartość konkretnego pliku (JSON) z GCS,
    wygenerowanego przez pipeline history.
    Załóżmy, że pipeline zapisuje JSON w formacie:
      {
        "collisions": [
           { "collision_id": "...", "frames": [...], ... },
           ...
        ]
      }
    """
    filename = request.args.get("file", "")
    if not filename:
        return jsonify({"error": "Missing 'file' param"}), 400

    from google.cloud import storage
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_HISTORY_BUCKET)

    blob = bucket.blob(filename)
    if not blob.exists():
        return jsonify({"error": "File not found"}), 404

    data_str = blob.download_as_text(encoding="utf-8")
    # Niczego nie przerabiamy, zwracamy w oryginale
    return app.response_class(
        data_str,
        mimetype="application/json"
    )

########################################################################

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)