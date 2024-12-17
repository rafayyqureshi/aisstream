import os
import json
import math
from flask import Flask, jsonify, render_template, request
from google.cloud import bigquery
from datetime import datetime, timezone

app = Flask(__name__,
            static_folder='static',
            template_folder='templates')

client = bigquery.Client()

def compute_cpa_tcpa(ship_a, ship_b):
    # Funkcja obliczająca CPA/TCPA przeniesiona z pipeline.py
    # Wykorzystujemy te same wzory
    if ship_a['ship_length'] is None or ship_b['ship_length'] is None:
        return (9999, -1)
    if ship_a['ship_length'] < 50 or ship_b['ship_length'] < 50:
        return (9999, -1)

    CPA_THRESHOLD = 0.5
    # Nie musimy tu thresholdów sprawdzać, tylko zwrócić wartości

    latRef = (ship_a['latitude']+ship_b['latitude'])/2
    scaleLat = 111000
    scaleLon = 111000*math.cos(latRef*math.pi/180)

    def toXY(lat,lon):
        return [lon*scaleLon, lat*scaleLat]

    xA,yA = toXY(ship_a['latitude'], ship_a['longitude'])
    xB,yB = toXY(ship_b['latitude'], ship_b['longitude'])

    sogA = ship_a['sog']
    sogB = ship_b['sog']

    def cogToVector(cogDeg, sogNmH):
        cog = math.radians(cogDeg)
        vx = sogNmH*math.sin(cog)
        vy = sogNmH*math.cos(cog)
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
    # Pokazujemy tylko sytuacje z ostatnich 5 minut i tcpa > 0
    # Zakładamy, że kolizje są aktualizowane często, jeśli tcpa <= 0 to sytuacja już minęła
    max_cpa = request.args.get('max_cpa', 0.5, type=float)
    max_tcpa = request.args.get('max_tcpa', 10.0, type=float)

    query = f"""
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
      AND c.tcpa <= {max_tcpa}
      AND c.cpa <= {max_cpa}
      AND c.timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
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
            'latitude_a': r.latitude_a,
            'longitude_a': r.longitude_a,
            'latitude_b': r.latitude_b,
            'longitude_b': r.longitude_b,
            'ship1_name': ship1_name,
            'ship2_name': ship2_name
        })
    return jsonify(result)

@app.route('/calculate_cpa_tcpa')
def calculate_cpa_tcpa():
    # Endpoint do obliczania CPA/TCPA na żądanie dla dwóch wybranych statków
    mmsi_a = request.args.get('mmsi_a', type=int)
    mmsi_b = request.args.get('mmsi_b', type=int)
    if not mmsi_a or not mmsi_b:
        return jsonify({'error':'Missing mmsi_a or mmsi_b'}),400

    query = f"""
    SELECT mmsi, latitude, longitude, cog, sog, timestamp, ship_length
    FROM `ais_dataset.ships_positions`
    WHERE mmsi IN ({mmsi_a},{mmsi_b})
    AND timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 MINUTE)
    ORDER BY timestamp DESC
    """

    rows = list(client.query(query).result())
    # Oczekujemy ostatnie dane obu statków
    data_by_mmsi = {}
    for r in rows:
        if r.mmsi not in data_by_mmsi:
            data_by_mmsi[r.mmsi]={
                'mmsi':r.mmsi,
                'latitude':r.latitude,
                'longitude':r.longitude,
                'cog':r.cog,
                'sog':r.sog,
                'timestamp':r.timestamp,
                'ship_length':r.ship_length
            }

    if mmsi_a not in data_by_mmsi or mmsi_b not in data_by_mmsi:
        return jsonify({'error':'No recent data for one or both ships'}),404

    cpa,tcpa = compute_cpa_tcpa(data_by_mmsi[mmsi_a], data_by_mmsi[mmsi_b])
    return jsonify({'cpa': cpa, 'tcpa': tcpa})
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)