#!/usr/bin/env python3
import os
import json
import math
import time

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.transforms.userstate import BagStateSpec
import apache_beam.coders

# Parametry wykrywania kolizji
CPA_THRESHOLD = 0.5        # mile morskie
TCPA_THRESHOLD = 10.0      # minuty
STATE_RETENTION_SEC = 30   # ile sekund w BagState

def parse_ais(record):
    """
    Parsuje wiadomość AIS w JSON z Pub/Sub.
    Wymaga pól: mmsi, latitude, longitude, cog, sog, timestamp.
    Zwraca None, gdy braki => odfiltrowane w potoku.
    """
    try:
        data = json.loads(record.decode('utf-8'))
        required = ['mmsi', 'latitude', 'longitude', 'cog', 'sog', 'timestamp']
        if not all(k in data for k in required):
            return None

        mmsi = int(data['mmsi'])
        lat = float(data['latitude'])
        lon = float(data['longitude'])
        cog = float(data['cog'])
        sog = float(data['sog'])

        # Opcjonalnie length
        ship_len = None
        if 'ship_length' in data:
            try:
                ship_len = float(data['ship_length'])
            except:
                pass

        return {
            'mmsi': mmsi,
            'latitude': lat,
            'longitude': lon,
            'cog': cog,
            'sog': sog,
            'timestamp': data['timestamp'],  # ISO8601
            'ship_name': data.get('ship_name'),
            'ship_length': ship_len,
            'geohash': data.get('geohash', 'none')
        }
    except:
        return None

def compute_cpa_tcpa(ship_a, ship_b):
    """
    Oblicza (CPA, TCPA) w milach morskich i minutach.
    Jeśli statki <50m => (9999, -1).
    Jeśli TCPA <0 => (9999, -1).
    """
    # Minimalny rozmiar statku
    la = ship_a.get('ship_length') or 0
    lb = ship_b.get('ship_length') or 0
    if la < 50 or lb < 50:
        return (9999, -1)

    latRef = (ship_a['latitude'] + ship_b['latitude']) / 2.0
    scaleLat = 111000.0
    scaleLon = 111000.0 * math.cos(math.radians(latRef))

    def to_xy(lat, lon):
        return (lon * scaleLon, lat * scaleLat)

    xA, yA = to_xy(ship_a['latitude'], ship_a['longitude'])
    xB, yB = to_xy(ship_b['latitude'], ship_b['longitude'])

    sogA = float(ship_a.get('sog') or 0)
    sogB = float(ship_b.get('sog') or 0)

    def cog_to_vec(cog_deg, sog_nm_h):
        r = math.radians(cog_deg or 0)
        vx = sog_nm_h * math.sin(r)
        vy = sog_nm_h * math.cos(r)
        return vx, vy

    vxA, vyA = cog_to_vec(ship_a['cog'], sogA)
    vxB, vyB = cog_to_vec(ship_b['cog'], sogB)

    # Różnica pozycji w metrach
    dx = xA - xB
    dy = yA - yB

    # SOG w nm/h => m/min
    speed_scale = 1852.0 / 60.0
    dvx_mpm = (vxA - vxB) * speed_scale
    dvy_mpm = (vyA - vyB) * speed_scale

    VV_m = dvx_mpm**2 + dvy_mpm**2
    PV_m = dx * dvx_mpm + dy * dvy_mpm

    if VV_m == 0:
        tcpa = 0.0
    else:
        tcpa = - PV_m / VV_m
    if tcpa < 0:
        return (9999, -1)

    xA2 = xA + vxA*speed_scale*tcpa
    yA2 = yA + vyA*speed_scale*tcpa
    xB2 = xB + vxB*speed_scale*tcpa
    yB2 = yB + vyB*speed_scale*tcpa

    dist_m = math.sqrt((xA2 - xB2)**2 + (yA2 - yB2)**2)
    dist_nm = dist_m / 1852.0
    return (dist_nm, tcpa)

class CollisionDoFn(beam.DoFn):
    """
    Stan BagState do przechowywania statków w danej komórce geohash
    i obliczania kolizji z nowo przybyłym statkiem.
    """
    RECORDS_STATE = BagStateSpec('records_state', beam.coders.FastPrimitivesCoder())

    def process(self, element, records_state=beam.DoFn.StateParam(RECORDS_STATE)):
        geohash, current_ship = element
        now_sec = time.time()

        # Dodaj bieżący statek do stanu
        records_state.add((current_ship, now_sec))

        old_list = list(records_state.read())
        fresh_list = []
        for (s, t) in old_list:
            if (now_sec - t) <= STATE_RETENTION_SEC:
                fresh_list.append((s, t))

        # wyczyść i zapisz tylko świeże
        records_state.clear()
        for s_t in fresh_list:
            records_state.add(s_t)

        # Porównaj bieżący statek z innymi statkami we fresh_list
        for (old_ship, _) in fresh_list:
            if old_ship['mmsi'] == current_ship['mmsi']:
                continue  # ten sam statek
            cpa, tcpa = compute_cpa_tcpa(old_ship, current_ship)
            if cpa < CPA_THRESHOLD and 0 <= tcpa < TCPA_THRESHOLD:
                yield {
                    'mmsi_a': old_ship['mmsi'],
                    'mmsi_b': current_ship['mmsi'],
                    'timestamp': current_ship['timestamp'],
                    'cpa': cpa,
                    'tcpa': tcpa,
                    'latitude_a': old_ship['latitude'],
                    'longitude_a': old_ship['longitude'],
                    'latitude_b': current_ship['latitude'],
                    'longitude_b': current_ship['longitude']
                }

def run():
    # Konfiguracja potoku
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True

    # Możemy pobrać parametry z ENV, by nie wpisywać na sztywno
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'my-project-id')
    dataset_id = os.getenv('DATASET_ID', 'ais_dataset_us')
    input_sub = os.getenv('LIVE_INPUT_SUB', 'projects/my-project-id/subscriptions/ais-data-sub')
    collisions_topic = os.getenv('COLLISIONS_TOPIC', 'projects/my-project-id/topics/collisions-topic')

    table_positions = f"{project_id}:{dataset_id}.ships_positions"
    table_collisions = f"{project_id}:{dataset_id}.collisions"

    with beam.Pipeline(options=pipeline_options) as p:

        # (1) Odczyt z Pub/Sub
        lines = p | 'ReadPubSub' >> beam.io.ReadFromPubSub(subscription=input_sub)

        # (2) Parsowanie
        parsed = (
            lines
            | 'ParseAIS' >> beam.Map(parse_ais)
            | 'FilterNone' >> beam.Filter(lambda x: x is not None)
        )

        # (3) Zapis do BigQuery: ships_positions (zapisujemy wszystkie, także <50m)
        parsed | 'WritePositionsToBQ' >> WriteToBigQuery(
            table=table_positions,
            schema=(
                'mmsi:INTEGER,'
                'latitude:FLOAT,'
                'longitude:FLOAT,'
                'cog:FLOAT,'
                'sog:FLOAT,'
                'timestamp:TIMESTAMP,'
                'ship_name:STRING,'
                'ship_length:FLOAT'
            ),
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
        )

        # (4) Grupowanie wg geohash
        keyed = parsed | 'KeyByGeohash' >> beam.Map(lambda r: (r['geohash'], r))

        # (5) Wykrywanie kolizji (tylko statki >=50m realnie trafiają do collisions)
        collisions = keyed | 'DetectCollisions' >> beam.ParDo(CollisionDoFn())

        # (6) Zapis kolizji do BigQuery
        collisions | 'WriteCollisionsToBQ' >> WriteToBigQuery(
            table=table_collisions,
            schema=(
                'mmsi_a:INTEGER,'
                'mmsi_b:INTEGER,'
                'timestamp:TIMESTAMP,'
                'cpa:FLOAT,'
                'tcpa:FLOAT,'
                'latitude_a:FLOAT,'
                'longitude_a:FLOAT,'
                'latitude_b:FLOAT,'
                'longitude_b:FLOAT'
            ),
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
        )

        # (7) Opcjonalna publikacja kolizji do innego topicu
        def collision_to_json(col):
            """
            Przykład konwersji do JSON (z prostym collision_id).
            """
            cid = f"{col['mmsi_a']}_{col['mmsi_b']}_{col['timestamp']}"
            data = {
                'collision_id': cid,
                'mmsi_a': col['mmsi_a'],
                'mmsi_b': col['mmsi_b'],
                'timestamp': col['timestamp'],
                'cpa': col['cpa'],
                'tcpa': col['tcpa'],
                # itp...
            }
            return json.dumps(data).encode('utf-8')

        collisions_str = collisions | 'ToJson' >> beam.Map(collision_to_json)
        collisions_str | 'PublishCollisions' >> beam.io.WriteToPubSub(topic=collisions_topic)