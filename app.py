@app.route('/history_collisions')
def history_collisions():
    day = request.args.get('day', 0, type=int)
    max_cpa = request.args.get('max_cpa', 0.5, type=float)
    max_tcpa = request.args.get('max_tcpa', 10.0, type=float)

    query = f"""
    WITH base AS (
      SELECT 
        c.mmsi_a, c.mmsi_b, c.timestamp, c.cpa, c.tcpa,
        c.latitude_a, c.longitude_a, c.latitude_b, c.longitude_b
      FROM `ais_dataset.collisions` c
      WHERE DATE(c.timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL {abs(day)} DAY)
        AND c.cpa <= {max_cpa}
        AND c.tcpa <= {max_tcpa}
    ),
    -- Najnowsze nazwy statków:
    latest_positions AS (
      SELECT mmsi, ship_name, timestamp,
      ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY timestamp DESC) as rpos
      FROM `ais_dataset.ships_positions`
    )
    SELECT
      CONCAT(CAST(mmsi_a AS STRING),'_',CAST(mmsi_b AS STRING),'_',FORMAT_TIMESTAMP('%Y%m%d%H%M%S', timestamp)) as collision_id,
      b.mmsi_a, b.mmsi_b, b.timestamp, b.cpa, b.tcpa,
      b.latitude_a, b.longitude_a, b.latitude_b, b.longitude_b,
      la.ship_name AS ship1_name,
      lb.ship_name AS ship2_name
    FROM base b
    LEFT JOIN latest_positions la ON la.mmsi = b.mmsi_a AND la.rpos=1
    LEFT JOIN latest_positions lb ON lb.mmsi = b.mmsi_b AND lb.rpos=1
    ORDER BY b.cpa ASC
    LIMIT 1000
    """

    rows = list(client.query(query).result())
    result=[]
    for r in rows:
        ship1_name = r.ship1_name if r.ship1_name else str(r.mmsi_a)
        ship2_name = r.ship2_name if r.ship2_name else str(r.mmsi_b)
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
            'ship2_name': ship2_name
        })
    return jsonify(result)