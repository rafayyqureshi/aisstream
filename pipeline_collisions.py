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

# Parametry kolizji (możesz je modyfikować)
CPA_THRESHOLD = 0.5       # mile morskie
TCPA_THRESHOLD = 10.0     # minuty
STATE_RETENTION_SEC = 30  # ile sekund trzymamy poprzednie statki w stanie

def parse_ais(record):
    """
    Parsuje AIS z Pub/Sub (JSON).
    Zwraca dict z polami: mmsi, latitude, longitude, cog, sog, timestamp, ship_name, ship_length, geohash.
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
            'timestamp': data['timestamp'],  # string w ISO8601
            'ship_name': data.get('ship_name'),
            'ship_length': ship_len,
            'geohash': data.get('geohash','none')
        }
    except:
        return None

def compute_cpa_tcpa(ship_a, ship_b):
    """
    Oblicza (cpa, tcpa) bazując na sog/cog i prostym przybliżeniu geograficznym.
    Zwraca (9999, -1), jeśli statki <50m lub brak sensu liczyć (tcpa <0).
    """
    try:
        sl_a = float(ship_a.get('ship_length', 0))
        sl_b = float(ship_b.get('ship_length', 0))
    except:
        return (9999, -1)

    # Pomijamy statki krótsze niż 50m (jeśli tak ustaliłeś w projekcie)
    if sl_a < 50 or sl_b < 50:
        return (9999, -1)

    latRef = (ship_a['latitude'] + ship_b['latitude']) / 2.0
    scaleLat = 111000.0
    scaleLon = 111000.0 * math.cos(math.radians(latRef))

    def to_xy(lat, lon):
        return (lon*scaleLon, lat*scaleLat)

    xA, yA = to_xy(ship_a['latitude'], ship_a['longitude'])
    xB, yB = to_xy(ship_b['latitude'], ship_b['longitude'])

    sogA = float(ship_a['sog'])
    sogB = float(ship_b['sog'])

    def cog_to_vec(cog_deg, sog_nm_h):
        rad = math.radians(cog_deg)
        vx = sog_nm_h * math.sin(rad)
        vy = sog_nm_h * math.cos(rad)
        return vx, vy

    vxA_nm, vyA_nm = cog_to_vec(float(ship_a['cog']), sogA)
    vxB_nm, vyB_nm = cog_to_vec(float(ship_b['cog']), sogB)

    dx = xA - xB
    dy = yA - yB
    dvx_nm = vxA_nm - vxB_nm
    dvy_nm = vyA_nm - vyB_nm

    speed_scale = 1852.0 / 60.0  # nm/h -> m/min
    dvx_mpm = dvx_nm * speed_scale
    dvy_mpm = dvy_nm * speed_scale

    VV_m = dvx_mpm**2 + dvy_mpm**2
    PV_m = dx*dvx_mpm + dy*dvy_mpm

    if VV_m == 0:
        tcpa = 0.0
    else:
        tcpa = - PV_m / VV_m

    if tcpa < 0:
        return (9999, -1)

    vxA_mpm = vxA_nm * speed_scale
    vyA_mpm = vyA_nm * speed_scale
    vxB_mpm = vxB_nm * speed_scale
    vyB_mpm = vyB_nm * speed_scale

    xA2 = xA + vxA_mpm * tcpa
    yA2 = yA + vyA_mpm * tcpa
    xB2 = xB + vxB_mpm * tcpa
    yB2 = yB + vyB_mpm * tcpa

    dist_m = math.sqrt((xA2 - xB2)**2 + (yA2 - yB2)**2)
    dist_nm = dist_m / 1852.0
    return (dist_nm, tcpa)

class CollisionDoFn(beam.DoFn):
    """
    Przechowuje w stanie poprzednie statki w tej samej komórce geohash,
    obliczając kolizje z nowym statkiem.
    """
    RECORDS_STATE = BagStateSpec('records_state', beam.coders.FastPrimitivesCoder())

    def process(self, element, records_state=beam.DoFn.StateParam(RECORDS_STATE)):
        geohash, ship = element
        now_sec = time.time()

        # Dodaj nowy statek do stanu
        records_state.add((ship, now_sec))

        # Wczytujemy co jest w stanie
        old_list = list(records_state.read())
        fresh = []
        for (s, t) in old_list:
            if now_sec - t <= STATE_RETENTION_SEC:
                fresh.append((s, t))

        # Czyścimy i zapisujemy tylko świeże
        records_state.clear()
        for fr in fresh:
            records_state.add(fr)

        # Oblicz kolizje między nowym statkiem a innymi "fresh"
        for (old_ship, _) in fresh:
            if old_ship is ship:
                continue
            cpa, tcpa = compute_cpa_tcpa(old_ship, ship)
            if cpa < CPA_THRESHOLD and 0 <= tcpa < TCPA_THRESHOLD:
                yield {
                    'mmsi_a': old_ship['mmsi'],
                    'mmsi_b': ship['mmsi'],
                    'timestamp': ship['timestamp'],  # timestamp z nowszego
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

    # Pobieramy parametry z ENV lub ustawiamy domyślne
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'ais-collision-detection')
    dataset = os.getenv('LIVE_DATASET', 'ais_dataset')
    input_subscription = os.getenv('LIVE_INPUT_SUB', 'projects/ais-collision-detection/subscriptions/ais-data-sub')
    collisions_topic = os.getenv('COLLISIONS_TOPIC', 'projects/ais-collision-detection/topics/collisions-topic')

    table_positions = f"{project_id}:{dataset}.ships_positions"
    table_collisions = f"{project_id}:{dataset}.collisions"

    with beam.Pipeline(options=pipeline_options) as p:
        # (1) Odczyt z Pub/Sub
        lines = p | 'ReadPubSub' >> beam.io.ReadFromPubSub(subscription=input_subscription)

        # (2) Parsowanie
        parsed = (
            lines
            | 'ParseAIS' >> beam.Map(parse_ais)
            | 'FilterNone' >> beam.Filter(lambda x: x is not None)
        )

        # (3) Zapis statków do BigQuery
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

        # (4) Grupujemy wg geohash (lub 'none')
        keyed = parsed | 'KeyByGeohash' >> beam.Map(lambda r: (r.get('geohash','none'), r))

        # (5) Detekcja kolizji
        collisions = keyed | 'DetectCollisions' >> beam.ParDo(CollisionDoFn())

        # (6) Zapis kolizji do BigQuery
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
        collisions_str = collisions | 'ToJson' >> beam.Map(lambda x: json.dumps(x).encode('utf-8'))
        collisions_str | 'PublishCollisions' >> beam.io.WriteToPubSub(topic=collisions_topic)


if __name__ == '__main__':
    run()