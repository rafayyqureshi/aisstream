import os
import json
import math
from flask import Flask, jsonify, render_template, request
from google.cloud import bigquery
from datetime import datetime, timedelta

app = Flask(
    __name__,
    static_folder='static',
    template_folder='templates'
)

client = bigquery.Client()

def compute_cpa_tcpa(ship_a, ship_b):
    """
    Computes (CPA, TCPA) using approximate geographic calculations.
    Returns (9999, -1) if ship_length < 50 or missing data, or if TCPA < 0.
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

    sogA = float(ship_a['sog'] or 0)
    sogB = float(ship_b['sog'] or 0)

    def cogToVector(cogDeg, sogNmH):
        r = math.radians(cogDeg or 0)
        vx = sogNmH * math.sin(r)
        vy = sogNmH * math.cos(r)
        return vx, vy

    vxA, vyA = cogToVector(ship_a['cog'], sogA)
    vxB, vyB = cogToVector(ship_b['cog'], sogB)

    dx = xA - xB
    dy = yA - yB

    speedScale = 1852.0 / 60.0
    dvx = (vxA - vxB) * speedScale
    dvy = (vyA - vyB) * speedScale

    VV_m = dvx**2 + dvy**2
    PV_m = dx*dvx + dy*dvy

    if VV_m == 0:
        tcpa = 0.0
    else:
        tcpa = -PV_m / VV_m

    if tcpa < 0:
        return (9999, -1)

    xA2 = xA + vxA*speedScale*tcpa
    yA2 = yA + vyA*speedScale*tcpa
    xB2 = xB + vxB*speedScale*tcpa
    yB2 = yB + vyB*speedScale*tcpa

    dist_m = math.sqrt((xA2 - xB2)**2 + (yA2 - yB2)**2)
    distNm = dist_m / 1852.0
    return (distNm, tcpa)

@app.route('/')
def index():
    """
    Main front-end (Live map).
    """
    return render_template('index.html')

@app.route('/ships')
def ships():
    """
    Returns current ships (last 2 minutes).
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
    Returns collisions, filtered by user-provided cpa/tcpa thresholds.
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
    # Możemy usunąć duplikaty tej samej pary – np. bierzemy minimum CPA albo ostatni timestamp
    # Ale tutaj upraszczamy – lub zrobimy w front-endzie.
    for r in rows:
        shipA = r.ship1_name or str(r.mmsi_a)
        shipB = r.ship2_name or str(r.mmsi_b)
        ts = r.timestamp.isoformat() if r.timestamp else None

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
    On-demand CPA/TCPA for a pair of ships (mmsi_a, mmsi_b) - last 5 minutes.
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
    WHERE mmsi IN ({mmsi_a},{mmsi_b})
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

@app.route('/history')
def history():
    return render_template('history.html')

@app.route('/history_collisions')
def history_collisions():
    """
    Example endpoint to fetch collisions historically with day offset, cpa filter, etc.
    """
    day_offset = int(request.args.get('day', 0))
    max_cpa = float(request.args.get('max_cpa', 0.5))
    the_date = (datetime.utcnow() + timedelta(days=day_offset)).date()

    query = f"""
    WITH base AS (
      SELECT
        c.mmsi_a, c.mmsi_b, c.timestamp AS coll_ts, c.cpa, c.tcpa,
        c.latitude_a, c.longitude_a, c.latitude_b, c.longitude_b
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)