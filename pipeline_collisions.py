import os
import json
import math
import time

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.userstate import BagStateSpec

# Parametry do wykrywania kolizji
CPA_THRESHOLD = 0.5      # mile morskie
TCPA_THRESHOLD = 10.0    # minuty
STATE_RETENTION_SEC = 30 # ile sekund trzymamy poprzednie rekordy w stanie

def parse_message(record):
    """
    Parsuje wiadomość Pub/Sub (AIS). Jeśli cokolwiek jest nieprawidłowe, zwraca None.
    """
    try:
        data = json.loads(record.decode('utf-8'))
        required = ['mmsi','latitude','longitude','cog','sog','timestamp']
        if not all(k in data for k in required):
            return None
        return data
    except:
        return None

def compute_cpa_tcpa(ship_a, ship_b):
    """
    Oblicza CPA i TCPA wykorzystując skalowanie geograficzne i SOG/COG w nm/h.
    Zwraca (cpa_nm, tcpa_min). Jeśli kolizja jest nieistotna lub statek <50m, zwraca (9999, -1).
    """
    # Konwersja ship_length na float
    try:
        sl_a = float(ship_a.get('ship_length', 0))
        sl_b = float(ship_b.get('ship_length', 0))
    except:
        return (9999, -1)

    # Pomijamy <50 m
    if sl_a < 50 or sl_b < 50:
        return (9999, -1)

    latRef = (ship_a['latitude'] + ship_b['latitude']) / 2.0
    scaleLat = 111000.0
    scaleLon = 111000.0 * math.cos(math.radians(latRef))

    def to_xy(lat, lon):
        return (lon * scaleLon, lat * scaleLat)

    xA, yA = to_xy(ship_a['latitude'], ship_a['longitude'])
    xB, yB = to_xy(ship_b['latitude'], ship_b['longitude'])

    sogA = ship_a['sog']
    sogB = ship_b['sog']

    def cog_to_vec(cog_deg, sog_nm_h):
        rad = math.radians(cog_deg)
        vx = sog_nm_h * math.sin(rad)
        vy = sog_nm_h * math.cos(rad)
        return vx, vy

    vxA_nm, vyA_nm = cog_to_vec(ship_a['cog'], sogA)
    vxB_nm, vyB_nm = cog_to_vec(ship_b['cog'], sogB)

    dx = xA - xB
    dy = yA - yB
    dvx_nm = vxA_nm - vxB_nm
    dvy_nm = vyA_nm - vyB_nm

    # nm/h => m/min
    speed_scale = 1852.0 / 60.0
    dvx_mpm = dvx_nm * speed_scale
    dvy_mpm = dvy_nm * speed_scale

    VV_m = dvx_mpm**2 + dvy_mpm**2
    PV_m = dx * dvx_mpm + dy * dvy_mpm

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
    Z wykorzystaniem stanu w Beam – porównuje nowy statek z tymi ostatnio widzianymi.
    """
    RECORDS_STATE = BagStateSpec('records_state', beam.coders.FastPrimitivesCoder())

    def process(self, element, records_state=beam.DoFn.StateParam(RECORDS_STATE)):
        geohash, ship = element
        now_sec = time.time()

        # Dodaj do stanu (ship, now_sec)
        records_state.add((ship, now_sec))

        # Odczytaj i wyfiltruj stare rekordy
        old_records = list(records_state.read())
        fresh_records = []
        for (s, t) in old_records:
            if now_sec - t <= STATE_RETENTION_SEC:
                fresh_records.append((s, t))

        # Zapisz tylko świeże
        records_state.clear()
        for fr in fresh_records:
            records_state.add(fr)

        # Porównuj z nowym ship
        for (old_ship, _) in fresh_records:
            if old_ship is ship:
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

class MyPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_subscription', type=str, required=True)
        parser.add_argument('--collisions_topic', type=str, required=True)

def run():
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True

    input_subscription = pipeline_options.view_as(MyPipelineOptions).input_subscription
    collisions_topic = pipeline_options.view_as(MyPipelineOptions).collisions_topic

    with beam.Pipeline(options=pipeline_options) as p:
        # Odczyt z Pub/Sub
        lines = (
            p
            | 'ReadPubSub' >> beam.io.ReadFromPubSub(subscription=input_subscription)
        )

        # Parsowanie
        parsed = (
            lines
            | 'ParseAIS' >> beam.Map(parse_message)
            | 'FilterNone' >> beam.Filter(lambda x: x is not None)
        )

        # Klucz geohash (lub 'none' jeśli brak)
        keyed = (
            parsed
            | 'KeyByGeohash' >> beam.Map(lambda r: (r.get('geohash', 'none'), r))
        )

        # Wykrywanie kolizji
        collisions = (
            keyed
            | 'DetectCollisions' >> beam.ParDo(CollisionDoFn())
        )

        # Publikacja do collisions-topic
        collisions_str = (
            collisions
            | 'CollisionsToJson' >> beam.Map(lambda x: json.dumps(x).encode('utf-8'))
        )
        (
            collisions_str
            | 'PubCollisions' >> beam.io.WriteToPubSub(topic=collisions_topic)
        )

if __name__ == '__main__':
    run()