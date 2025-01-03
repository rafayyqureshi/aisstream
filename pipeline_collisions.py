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

# --- Parametry kolizji ---
CPA_THRESHOLD = 0.5        # mile morskie
TCPA_THRESHOLD = 10.0      # minuty
STATE_RETENTION_SEC = 30   # ile sekund pamiętamy statki w danej komórce geohash

def parse_ais(record):
    """
    Parsuje rekord AIS z Pub/Sub w formacie JSON. 
    Wymagane pola: mmsi, latitude, longitude, cog, sog, timestamp.
    Dodatkowo może mieć: ship_name, ship_length, geohash, itp.
    """
    try:
        data = json.loads(record.decode('utf-8'))
        required = ['mmsi','latitude','longitude','cog','sog','timestamp']
        if not all(k in data for k in required):
            return None

        mmsi = int(data['mmsi'])
        lat = float(data['latitude'])
        lon = float(data['longitude'])
        cog = float(data['cog'])
        sog = float(data['sog'])

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
            'timestamp': data['timestamp'],  
            'ship_name': data.get('ship_name'),
            'ship_length': ship_len,
            'geohash': data.get('geohash','none')
        }
    except:
        return None

def compute_cpa_tcpa(ship_a, ship_b):
    """
    Oblicza (cpa, tcpa). Jeśli statki <50m, zwraca (9999, -1). 
    Zwraca (9999, -1), gdy tcpa <0 (tzn. statki już się minęły).
    """
    try:
        la = float(ship_a.get('ship_length') or 0)
        lb = float(ship_b.get('ship_length') or 0)
    except:
        return (9999, -1)

    if la < 50 or lb < 50:
        return (9999, -1)

    latRef = (ship_a['latitude'] + ship_b['latitude']) / 2.0
    scaleLat = 111000.0
    scaleLon = 111000.0 * math.cos(math.radians(latRef))

    def to_xy(lat, lon):
        return (lon * scaleLon, lat * scaleLat)

    xA, yA = to_xy(ship_a['latitude'], ship_a['longitude'])
    xB, yB = to_xy(ship_b['latitude'], ship_b['longitude'])

    sogA = float(ship_a['sog'] or 0)
    sogB = float(ship_b['sog'] or 0)

    def cog_to_vec(cog_deg, sog_nm):
        r = math.radians(cog_deg)
        vx = sog_nm * math.sin(r)
        vy = sog_nm * math.cos(r)
        return vx, vy

    vxA, vyA = cog_to_vec(ship_a['cog'], sogA)
    vxB, vyB = cog_to_vec(ship_b['cog'], sogB)

    # pozycje w metrach
    dx = xA - xB
    dy = yA - yB

    # prędkości względne: nm/h => m/min
    speed_scale = 1852.0 / 60.0
    dvx = (vxA - vxB)*speed_scale
    dvy = (vyA - vyB)*speed_scale

    VV = dvx*dvx + dvy*dvy
    PV = dx*dvx + dy*dvy

    if VV == 0:
        tcpa = 0.0
    else:
        tcpa = -PV / VV

    if tcpa < 0:
        return (9999, -1)

    xA2 = xA + (vxA*speed_scale)*tcpa
    yA2 = yA + (vyA*speed_scale)*tcpa
    xB2 = xB + (vxB*speed_scale)*tcpa
    yB2 = yB + (vyB*speed_scale)*tcpa

    dist = math.sqrt((xA2 - xB2)**2 + (yA2 - yB2)**2)
    dist_nm = dist / 1852.0
    return (dist_nm, tcpa)

class CollisionDoFn(beam.DoFn):
    """
    Przechowuje w stanie BagState poprzednie statki w tej samej komórce geohash
    i sprawdza kolizje z nowym statkiem.
    """
    RECORDS_STATE = BagStateSpec('records_state', beam.coders.FastPrimitivesCoder())

    def process(self, element, records_state=beam.DoFn.StateParam(RECORDS_STATE)):
        geohash, ship = element
        now_sec = time.time()

        # Dodaj do stanu
        records_state.add((ship, now_sec))

        old_list = list(records_state.read())
        fresh = []
        for (s, t) in old_list:
            if (now_sec - t) <= STATE_RETENTION_SEC:
                fresh.append((s, t))

        # Oczyść i zapisz 'fresh'
        records_state.clear()
        for fr in fresh:
            records_state.add(fr)

        # Sprawdź kolizje z nowym
        for (old_ship, _) in fresh:
            if old_ship['mmsi'] == ship['mmsi']:
                continue

            cpa, tcpa = compute_cpa_tcpa(old_ship, ship)
            if cpa < CPA_THRESHOLD and 0 <= tcpa < TCPA_THRESHOLD:
                yield {
                    'mmsi_a': old_ship['mmsi'],
                    'mmsi_b': ship['mmsi'],
                    'timestamp': ship['timestamp'],
                    'cpa': cpa,
                    'tcpa': tcpa,
                    'latitude_a': old_ship['latitude'],
                    'longitude_a': old_ship['longitude'],
                    'latitude_b': ship['latitude'],
                    'longitude_b': ship['longitude']
                }

def run():
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True

    # Użyj nowego datasetu w regionie us
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT','ais-collision-detection')
    dataset = 'ais_dataset_us'  # <-- zakładamy, że tak się nazywa Twój dataset w regionie us
    input_subscription = os.getenv('LIVE_INPUT_SUB','projects/ais-collision-detection/subscriptions/ais-data-sub')
    collisions_topic = os.getenv('COLLISIONS_TOPIC','projects/ais-collision-detection/topics/collisions-topic')

    table_positions = f"{project_id}:{dataset}.ships_positions"
    table_collisions = f"{project_id}:{dataset}.collisions"

    with beam.Pipeline(options=pipeline_options) as p:
        # (1) Czytanie z Pub/Sub
        lines = p | 'ReadPubSub' >> beam.io.ReadFromPubSub(subscription=input_subscription)

        # (2) Parsowanie
        parsed = (
            lines
            | 'ParseAIS' >> beam.Map(parse_ais)
            | 'FilterNone' >> beam.Filter(lambda x: x is not None)
        )

        # (3) Zapis do BQ -> ships_positions
        parsed | 'WritePositions' >> WriteToBigQuery(
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

        # (4) Grupujemy wg geohash
        keyed = parsed | 'KeyByGeohash' >> beam.Map(lambda r: (r.get('geohash','none'), r))

        # (5) Detekcja kolizji
        collisions = keyed | 'DetectCollisions' >> beam.ParDo(CollisionDoFn())

        # (6) Zapis kolizji do BQ -> collisions
        collisions | 'WriteCollisions' >> WriteToBigQuery(
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

        # (7) Publikacja kolizji do Pub/Sub (opcjonalne)
        def collision_to_json(c):
            collision_id = f"{c['mmsi_a']}_{c['mmsi_b']}_{c['timestamp']}"
            payload = {
                'collision_id': collision_id,
                'mmsi_a': c['mmsi_a'],
                'mmsi_b': c['mmsi_b'],
                'timestamp': c['timestamp'],
                'cpa': c['cpa'],
                'tcpa': c['tcpa'],
                # ewentualnie więcej pól
            }
            return json.dumps(payload).encode('utf-8')

        (
            collisions
            | 'CollisionToJson' >> beam.Map(collision_to_json)
            | 'PublishCollisions' >> beam.io.WriteToPubSub(topic=collisions_topic)
        )


if __name__ == '__main__':
    run()