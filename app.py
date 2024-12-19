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
    if ship_a['ship_length'] is None or ship_b['ship_length'] is None:
        return (9999, -1)
    if ship_a['ship_length'] < 50 or ship_b['ship_length'] < 50:
        return (9999, -1)

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

@app.route('/calculate_cpa_tcpa')
def calculate_cpa_tcpa():
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

@app.route('/history')
def history():
    return render_template('history.html')

@app.route('/history_collisions')
def history_collisions():
    day = request.args.get('day', 0, type=int)
    max_cpa = request.args.get('max_cpa', 0.5, type=float)
    max_tcpa = request.args.get('max_tcpa', 10.0, type=float)

    query = f"""
    WITH base AS (
      SELECT 
        c.mmsi_a, c.mmsi_b, c.timestamp AS coll_ts, c.cpa, c.tcpa,
        c.latitude_a, c.longitude_a, c.latitude_b, c.longitude_b
      FROM `ais_dataset.collisions` c
      WHERE DATE(c.timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL {abs(day)} DAY)
        AND c.cpa <= {max_cpa}
        AND c.tcpa <= {max_tcpa}
    ),
    pairs AS (
      SELECT *,
      IF(mmsi_a<mmsi_b,mmsi_a,mmsi_b) as m_a,
      IF(mmsi_a<mmsi_b,mmsi_b,mmsi_a) as m_b
      FROM base
    ),
    minimal AS (
      SELECT m_a,m_b, MIN(cpa) as min_cpa
      FROM pairs
      GROUP BY m_a,m_b
    ),
    filtered AS (
      SELECT p.*
      FROM pairs p
      JOIN minimal on minimal.m_a=p.m_a AND minimal.m_b=p.m_b AND minimal.min_cpa=p.cpa
    ),
    latest_positions AS (
      SELECT mmsi, ship_name, ship_length, timestamp AS sp_timestamp,
      ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY timestamp DESC) as rpos
      FROM `ais_dataset.ships_positions`
    )
    SELECT 
      CONCAT(CAST(f.mmsi_a AS STRING),'_',CAST(f.mmsi_b AS STRING),'_',FORMAT_TIMESTAMP('%Y%m%d%H%M%S', f.coll_ts)) as collision_id,
      f.mmsi_a, f.mmsi_b,
      f.coll_ts as timestamp,
      f.cpa, f.tcpa,
      f.latitude_a, f.longitude_a, f.latitude_b, f.longitude_b,
      la.ship_name AS ship1_name,
      la.ship_length AS ship1_length,
      lb.ship_name AS ship2_name,
      lb.ship_length AS ship2_length
    FROM filtered f
    LEFT JOIN latest_positions la ON la.mmsi = f.mmsi_a AND la.rpos=1
    LEFT JOIN latest_positions lb ON lb.mmsi = f.mmsi_b AND lb.rpos=1
    ORDER BY f.cpa ASC
    LIMIT 1000
    """

    rows = list(client.query(query).result())
    result=[]
    for r in rows:
        ship1_name = r.ship1_name if r.ship1_name else str(r.mmsi_a)
        ship2_name = r.ship2_name if r.ship2_name else str(r.mmsi_b)

        if r.mmsi_a == r.mmsi_b:
            continue
        if ship1_name == ship2_name:
            continue

        result.append({
            'collision_id': r.collision_id,
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
            'ship2_name': ship2_name,
            'ship1_length': r.ship1_length,
            'ship2_length': r.ship2_length
        })
    return jsonify(result)

@app.route('/history_data')
def history_data():
    collision_id = request.args.get('collision_id', type=str)
    if not collision_id:
        return jsonify({'error':'Missing collision_id'}),400

    parts = collision_id.split('_')
    if len(parts)<3:
        return jsonify([])

    mmsi_a = int(parts[0])
    mmsi_b = int(parts[1])
    ts_str = parts[2]
    collision_time = datetime.strptime(ts_str, '%Y%m%d%H%M%S')

    # Chcemy dokładnie 10 klatek:
    # -6,-5,-4,-3,-2,-1,0,+1,+2,+3 min relative to collision_time (T=0 minimal approach)
    frame_times = [collision_time + timedelta(minutes=offset) for offset in [-6,-5,-4,-3,-2,-1,0,1,2,3]]

    q_data = f"""
    SELECT mmsi, latitude, longitude, sog, cog, timestamp
    FROM `ais_dataset.ships_positions`
    WHERE mmsi IN ({mmsi_a},{mmsi_b})
      AND timestamp BETWEEN TIMESTAMP_SUB(TIMESTAMP('{collision_time.isoformat()}'), INTERVAL 7 MINUTE)
                       AND TIMESTAMP_ADD(TIMESTAMP('{collision_time.isoformat()}'), INTERVAL 3 MINUTE)
    ORDER BY timestamp
    """

    positions = list(client.query(q_data).result())
    # positions posortowane po timestamp
    # Musimy zinterpolować do frame_times
    # Rozdzielamy dane na dwa statki
    data_a = [(p.timestamp,p.latitude,p.longitude,p.sog,p.cog) for p in positions if p.mmsi==mmsi_a]
    data_b = [(p.timestamp,p.latitude,p.longitude,p.sog,p.cog) for p in positions if p.mmsi==mmsi_b]

    def interpolate(data, t):
        # data sorted by timestamp
        if not data:
            return None
        # jeśli t <= data[0].timestamp
        if t<=data[0][0]:
            return data[0]
        # jeśli t>=data[-1].timestamp
        if t>=data[-1][0]:
            return data[-1]
        # w innym wypadku szukamy przedziału
        for i in range(len(data)-1):
            t1=data[i][0]
            t2=data[i+1][0]
            if t1<=t<=t2:
                # linear interpolate lat,lon,sog,cog
                dt=(t2-t1).total_seconds()
                if dt==0:
                    return data[i]
                ratio=(t-t1).total_seconds()/dt
                lat=data[i][1]+(data[i+1][1]-data[i][1])*ratio
                lon=data[i][2]+(data[i+1][2]-data[i][2])*ratio
                sog=data[i][3]+(data[i+1][3]-data[i][3])*ratio
                cog=data[i][4]+(data[i+1][4]-data[i][4])*ratio
                return (t,lat,lon,sog,cog)
        return data[-1]

    result_data=[]
    for ft in frame_times:
        pa=interpolate(data_a,ft)
        pb=interpolate(data_b,ft)
        ships=[]
        if pa is not None:
            ships.append({'mmsi':mmsi_a,'lat':pa[1],'lon':pa[2],'sog':pa[3],'cog':pa[4]})
        if pb is not None:
            ships.append({'mmsi':mmsi_b,'lat':pb[1],'lon':pb[2],'sog':pb[3],'cog':pb[4]})
        result_data.append({
            'time': ft.isoformat(),
            'shipPositions': ships
        })

    return jsonify(result_data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)