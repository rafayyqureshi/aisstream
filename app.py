import os
import json
import math
from flask import Flask, jsonify, render_template, request
from google.cloud import bigquery
from datetime import datetime, timedelta

app = Flask(__name__,
            static_folder='static',
            template_folder='templates')

client = bigquery.Client()

def compute_cpa_tcpa(ship_a, ship_b):
    if ship_a.get('ship_length') is None or ship_b.get('ship_length') is None:
        return (9999, -1)
    if ship_a['ship_length'] < 50 or ship_b['ship_length'] < 50:
        return (9999, -1)

    latRef = (ship_a['latitude']+ship_b['latitude'])/2
    scaleLat = 111000
    scaleLon = 111000*math.cos(math.radians(latRef))

    def toXY(lat, lon):
        return [lon*scaleLon, lat*scaleLat]

    xA,yA = toXY(ship_a['latitude'], ship_a['longitude'])
    xB,yB = toXY(ship_b['latitude'], ship_b['longitude'])

    sogA = ship_a['sog']
    sogB = ship_b['sog']

    def cogToVector(cogDeg, sogNmH):
        r = math.radians(cogDeg)
        vx = sogNmH*math.sin(r)
        vy = sogNmH*math.cos(r)
        return vx, vy

    vxA, vyA = cogToVector(ship_a['cog'], sogA)
    vxB, vyB = cogToVector(ship_b['cog'], sogB)

    dx = xA - xB
    dy = yA - yB
    dvx = vxA - vxB
    dvy = vyA - vyB

    speedScale = 1852/60
    dvx_mpm = dvx*speedScale
    dvy_mpm = dvy*speedScale

    VV_m = dvx_mpm**2 + dvy_mpm**2
    PV_m = dx*dvx_mpm + dy*dvy_mpm

    if VV_m == 0:
        tcpa = 0.0
    else:
        tcpa = -PV_m/VV_m
    if tcpa < 0:
        return (9999, -1)

    vxA_mpm = vxA*speedScale
    vyA_mpm = vyA*speedScale
    vxB_mpm = vxB*speedScale
    vyB_mpm = vyB*speedScale

    xA2 = xA + vxA_mpm*tcpa
    yA2 = yA + vyA_mpm*tcpa
    xB2 = xB + vxB_mpm*tcpa
    yB2 = yB + vyB_mpm*tcpa

    dist = math.sqrt((xA2-xB2)**2 + (yA2-yB2)**2)
    distNm = dist/1852
    return (distNm, tcpa)

@app.route('/')
def index():
    # Główna strona front-end (LIVE)
    return render_template('index.html')

@app.route('/ships')
def ships():
    # Pobiera statki z ostatnich 2 min
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
    rows = client.query(query).result()
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
    max_cpa = float(request.args.get('max_cpa', 0.5))
    max_tcpa = float(request.args.get('max_tcpa', 10.0))

    query = f"""
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
      spA.ship_name AS ship1_name,
      spB.ship_name AS ship2_name
    FROM `ais_dataset.collisions` c
    LEFT JOIN (
      SELECT mmsi, ship_name, MAX(timestamp) as latest_ts
      FROM `ais_dataset.ships_positions`
      GROUP BY mmsi
    ) lastA ON lastA.mmsi = c.mmsi_a
    LEFT JOIN `ais_dataset.ships_positions` spA
      ON spA.mmsi = lastA.mmsi AND spA.timestamp = lastA.latest_ts

    LEFT JOIN (
      SELECT mmsi, ship_name, MAX(timestamp) as latest_ts
      FROM `ais_dataset.ships_positions`
      GROUP BY mmsi
    ) lastB ON lastB.mmsi = c.mmsi_b
    LEFT JOIN `ais_dataset.ships_positions` spB
      ON spB.mmsi = lastB.mmsi AND spB.timestamp = lastB.latest_ts

    WHERE c.cpa <= {max_cpa}
      AND c.tcpa <= {max_tcpa}
      AND tcpa >= 0
    ORDER BY c.timestamp DESC
    LIMIT 100
    """
    rows = client.query(query).result()
    result=[]
    for r in rows:
        shipA = r.ship1_name or str(r.mmsi_a)
        shipB = r.ship2_name or str(r.mmsi_b)
        ts = r.timestamp.isoformat() if r.timestamp else None
        result.append({
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
    mmsi_a = request.args.get('mmsi_a', type=int)
    mmsi_b = request.args.get('mmsi_b', type=int)
    if not mmsi_a or not mmsi_b:
        return jsonify({'error':'Missing mmsi_a or mmsi_b'}), 400

    # Bierzemy najświeższe rekordy do 5 min wstecz
    query = f"""
    SELECT mmsi, latitude, longitude, cog, sog, ship_length
    FROM `ais_dataset.ships_positions`
    WHERE mmsi IN ({mmsi_a},{mmsi_b})
      AND timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
    ORDER BY timestamp DESC
    """
    rows = list(client.query(query).result())
    data_by_mmsi = {}
    for r in rows:
        if r.mmsi not in data_by_mmsi:  # weźmy pierwszy (najświeższy)
            data_by_mmsi[r.mmsi] = {
                'mmsi': r.mmsi,
                'latitude': r.latitude,
                'longitude': r.longitude,
                'cog': r.cog,
                'sog': r.sog,
                'ship_length': r.ship_length
            }

    if mmsi_a not in data_by_mmsi or mmsi_b not in data_by_mmsi:
        return jsonify({'error':'No recent data for one or both ships'}), 404

    cpa, tcpa = compute_cpa_tcpa(data_by_mmsi[mmsi_a], data_by_mmsi[mmsi_b])
    return jsonify({'cpa': cpa, 'tcpa': tcpa})

# (Możesz dopisać tutaj /history, /history_collisions itp. jeśli potrzebujesz.)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)