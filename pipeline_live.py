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

# Domyślne parametry kolizyjne (dostosuj według potrzeb)
CPA_THRESHOLD = 0.5        # w milach morskich
TCPA_THRESHOLD = 10.0      # w minutach
STATE_RETENTION_SEC = 30   # ile sekund trzymamy poprzednie statki w stanie (geohash)

def parse_ais(record: bytes):
    """
    Parsuje rekord AIS z Pub/Sub (JSON).
    Wymagane pola: mmsi, latitude, longitude, cog, sog, timestamp.
    Opcjonalnie: ship_name, ship_length, geohash.
    """
    try:
        data = json.loads(record.decode('utf-8'))
        # Sprawdzamy minimalny zestaw pól:
        required = ['mmsi', 'latitude', 'longitude', 'cog', 'sog', 'timestamp']
        if not all(k in data for k in required):
            return None

        # Konwersje na float/int
        data['mmsi'] = int(data['mmsi'])
        data['latitude'] = float(data['latitude'])
        data['longitude'] = float(data['longitude'])
        data['cog'] = float(data['cog'])
        data['sog'] = float(data['sog'])

        # Długość statku – jeśli istnieje i jest poprawna
        if 'ship_length' in data:
            try:
                data['ship_length'] = float(data['ship_length'])
            except:
                data['ship_length'] = None
        else:
            data['ship_length'] = None

        # geohash – jeśli brak, ustawiamy 'none'
        if 'geohash' not in data:
            data['geohash'] = 'none'

        return data
    except:
        return None


def compute_cpa_tcpa(ship_a, ship_b):
    """
    Przykładowy kod liczący (CPA, TCPA) – wstaw tu swój poprawny algorytm.
    Zwraca (cpa_nm, tcpa_min). Jeśli warunki są niewłaściwe (np. statek <50m), zwraca (9999, -1).
    """
    try:
        la = float(ship_a.get('ship_length') or 0)
        lb = float(ship_b.get('ship_length') or 0)
    except:
        return (9999, -1)

    # Pomijamy statki krótsze niż 50m
    if la < 50 or lb < 50:
        return (9999, -1)

    # Przybliżenie geograficzne
    latRef = (ship_a['latitude'] + ship_b['latitude']) / 2.0
    scaleLat = 111000.0
    scaleLon = 111000.0 * math.cos(math.radians(latRef))

    def to_xy(lat, lon):
        return (lon * scaleLon, lat * scaleLat)

    xA, yA = to_xy(ship_a['latitude'], ship_a['longitude'])
    xB, yB = to_xy(ship_b['latitude'], ship_b['longitude'])

    sogA = float(ship_a['sog'] or 0)
    sogB = float(ship_b['sog'] or 0)

    def cog_to_vec(cog_deg, sog_nm_h):
        r = math.radians(cog_deg)
        vx = sog_nm_h * math.sin(r)
        vy = sog_nm_h * math.cos(r)
        return vx, vy

    vxA_nm, vyA_nm = cog_to_vec(ship_a['cog'], sogA)
    vxB_nm, vyB_nm = cog_to_vec(ship_b['cog'], sogB)

    dx = xA - xB
    dy = yA - yB

    # nm/h -> m/min
    speed_scale = 1852.0 / 60.0
    dvx_mpm = (vxA_nm - vxB_nm) * speed_scale
    dvy_mpm = (vyA_nm - vyB_nm) * speed_scale

    VV_m = dvx_mpm**2 + dvy_mpm**2
    PV_m = dx * dvx_mpm + dy * dvy_mpm

    if VV_m == 0:
        tcpa = 0.0
    else:
        tcpa = -PV_m / VV_m  # w minutach (skala od prędkości)
    if tcpa < 0:
        return (9999, -1)

    # Pozycje w chwili tcpa
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
    Stan (BagState) trzyma statki z ostatnich STATE_RETENTION_SEC w danej komórce geohash.
    Dla każdego nowego statku sprawdzamy kolizje z poprzednimi.
    """
    RECORDS_STATE = BagStateSpec('records_state', beam.coders.FastPrimitivesCoder())

    def process(self, element, records_state=beam.DoFn.StateParam(RECORDS_STATE)):
        geohash, ship = element
        now_sec = time.time()

        # Dodajemy nowy statek do stanu
        records_state.add((ship, now_sec))

        # Wczytujemy, filtrujemy i nadpisujemy stan
        old_records = list(records_state.read())
        fresh = []
        for (s, t) in old_records:
            if (now_sec - t) <= STATE_RETENTION_SEC:
                fresh.append((s, t))

        records_state.clear()
        for (s, t) in fresh:
            records_state.add((s, t))

        # Obliczamy ewentualne kolizje z nowym statkiem
        for (old_ship, _) in fresh:
            # Pomijamy, jeśli to ten sam mmsi
            if old_ship['mmsi'] == ship['mmsi']:
                continue

            cpa, tcpa = compute_cpa_tcpa(old_ship, ship)
            if cpa < CPA_THRESHOLD and 0 <= tcpa < TCPA_THRESHOLD:
                yield {
                    'mmsi_a': old_ship['mmsi'],
                    'mmsi_b': ship['mmsi'],
                    'timestamp': ship['timestamp'],  # bierzemy timestamp z "nowszego"
                    'cpa': cpa,
                    'tcpa': tcpa,
                    'latitude_a': old_ship['latitude'],
                    'longitude_a': old_ship['longitude'],
                    'latitude_b': ship['latitude'],
                    'longitude_b': ship['longitude']
                }


def run():
    # Podstawowe parametry potoku
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True  # KLUCZOWE!

    # Wczytujemy zmienne z .env (Makefile/export)
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'ais-collision-detection')
    dataset = os.getenv('LIVE_DATASET', 'ais_dataset_us')
    input_subscription = os.getenv('INPUT_SUBSCRIPTION', 'projects/ais-collision-detection/subscriptions/ais-data-sub')
    collisions_topic = os.getenv('COLLISIONS_TOPIC', 'projects/ais-collision-detection/topics/collisions-topic')

    # Tabele w BigQuery:
    table_positions = f"{project_id}:{dataset}.ships_positions"
    table_collisions = f"{project_id}:{dataset}.collisions"

    # Budowa potoku
    with beam.Pipeline(options=pipeline_options) as p:
        # 1. Odczyt z Pub/Sub
        lines = p | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=input_subscription)

        # 2. Parsowanie AIS
        parsed = (
            lines
            | "ParseAIS" >> beam.Map(parse_ais)
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
        )

        # 3. Zapis statków do BigQuery (ships_positions)
        (
            parsed
            | "WritePositionsBQ" >> WriteToBigQuery(
                table=table_positions,
                schema=(
                    "mmsi:INTEGER,"
                    "latitude:FLOAT,"
                    "longitude:FLOAT,"
                    "cog:FLOAT,"
                    "sog:FLOAT,"
                    "timestamp:TIMESTAMP,"
                    "ship_name:STRING,"
                    "ship_length:FLOAT"
                ),
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

        # 4. Grupowanie po geohash (kolizje)
        keyed = parsed | "KeyByGeohash" >> beam.Map(lambda r: (r['geohash'], r))

        collisions = keyed | "DetectCollisions" >> beam.ParDo(CollisionDoFn())

        # 5. Zapis kolizji do BigQuery (collisions)
        (
            collisions
            | "WriteCollisionsBQ" >> WriteToBigQuery(
                table=table_collisions,
                schema=(
                    "mmsi_a:INTEGER,"
                    "mmsi_b:INTEGER,"
                    "timestamp:TIMESTAMP,"
                    "cpa:FLOAT,"
                    "tcpa:FLOAT,"
                    "latitude_a:FLOAT,"
                    "longitude_a:FLOAT,"
                    "latitude_b:FLOAT,"
                    "longitude_b:FLOAT"
                ),
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

        # 6. Publikacja do Pub/Sub (opcjonalne)
        # collisions_str = collisions | "CollisionToJson" >> beam.Map(lambda x: json.dumps(x).encode('utf-8'))
        # collisions_str | "PublishCollisions" >> beam.io.WriteToPubSub(topic=collisions_topic)


# Warunek konieczny do uruchomienia potoku po wywołaniu "python pipeline_live.py ..."
if __name__ == '__main__':
    run()