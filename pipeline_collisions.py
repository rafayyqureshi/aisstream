import os
import json
import math
import time
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.metrics import Metrics
from apache_beam.transforms.userstate import BagStateSpec

# Parametry
CPA_THRESHOLD = 0.5       # mile morskie
TCPA_THRESHOLD = 10.0     # minuty
STATE_RETENTION_SEC = 30  # retencja 30s

def parse_message(record):
    # Metryka do zliczania ogólnej liczby "odebranych" rekordów
    parse_counter = Metrics.counter('CollisionPipeline', 'parse_message_calls')

    try:
        data = json.loads(record.decode('utf-8'))
        required = ['mmsi','latitude','longitude','cog','sog','timestamp']
        if not all(k in data for k in required):
            return None
        parse_counter.inc(1)
        return data
    except:
        logging.warning("Failed to parse JSON record.")
        return None

def compute_cpa_tcpa(ship_a, ship_b):
    cpa_counter = Metrics.counter('CollisionPipeline', 'compute_cpa_tcpa_calls')
    cpa_counter.inc()

    # Bezpieczna konwersja długości
    sl_a = ship_a.get('ship_length')
    sl_b = ship_b.get('ship_length')
    try:
        sl_a = float(sl_a)
    except:
        return (9999, -1)
    try:
        sl_b = float(sl_b)
    except:
        return (9999, -1)

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

    def cog_to_vector(cog_deg, sog_nm_h):
        cog_rad = math.radians(cog_deg)
        vx = sog_nm_h * math.sin(cog_rad)
        vy = sog_nm_h * math.cos(cog_rad)
        return vx, vy

    vxA_nm_h, vyA_nm_h = cog_to_vector(ship_a['cog'], sogA)
    vxB_nm_h, vyB_nm_h = cog_to_vector(ship_b['cog'], sogB)

    dx = xA - xB
    dy = yA - yB
    dvx_nm_h = vxA_nm_h - vxB_nm_h
    dvy_nm_h = vyA_nm_h - vyB_nm_h

    speed_scale = 1852.0 / 60.0
    dvx_mpm = dvx_nm_h * speed_scale
    dvy_mpm = dvy_nm_h * speed_scale

    VV_m = dvx_mpm**2 + dvy_mpm**2
    PV_m = dx*dvx_mpm + dy*dvy_mpm

    if VV_m == 0:
        tcpa = 0.0
    else:
        tcpa = - PV_m / VV_m

    if tcpa < 0:
        return (9999, -1)

    vxA_mpm = vxA_nm_h * speed_scale
    vyA_mpm = vyA_nm_h * speed_scale
    vxB_mpm = vxB_nm_h * speed_scale
    vyB_mpm = vyB_nm_h * speed_scale

    xA2 = xA + vxA_mpm * tcpa
    yA2 = yA + vyA_mpm * tcpa
    xB2 = xB + vxB_mpm * tcpa
    yB2 = yB + vyB_mpm * tcpa

    dist_m = math.sqrt((xA2 - xB2)**2 + (yA2 - yB2)**2)
    dist_nm = dist_m / 1852.0

    return (dist_nm, tcpa)

class CollisionDoFn(beam.DoFn):
    RECORDS_STATE = BagStateSpec('records_state', beam.coders.FastPrimitivesCoder())

    def process(self, element, records_state=beam.DoFn.StateParam(RECORDS_STATE)):
        geohash, ship = element
        now_sec = time.time()

        # Dodaj do stanu (ship, now_sec)
        records_state.add((ship, now_sec))

        old_records = list(records_state.read())
        fresh_records = []
        for (s, t) in old_records:
            if now_sec - t <= STATE_RETENTION_SEC:
                fresh_records.append((s, t))

        # Wyczyść i zapisz tylko "świeże"
        records_state.clear()
        for fr in fresh_records:
            records_state.add(fr)

        # Sprawdź kolizje
        for (old_ship, _) in fresh_records:
            if old_ship is ship:
                continue
            cpa, tcpa = compute_cpa_tcpa(old_ship, ship)
            if cpa < CPA_THRESHOLD and 0 <= tcpa < TCPA_THRESHOLD:
                logging.info(f"Collision detected: cpa={cpa:.2f}, tcpa={tcpa:.2f}, mmsi=({old_ship['mmsi']},{ship['mmsi']})")
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
    logging.getLogger().setLevel(logging.INFO)

    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True

    input_subscription = pipeline_options.view_as(MyPipelineOptions).input_subscription
    collisions_topic = pipeline_options.view_as(MyPipelineOptions).collisions_topic

    with beam.Pipeline(options=pipeline_options) as p:
        lines = (
            p
            | 'ReadPubSub' >> beam.io.ReadFromPubSub(subscription=input_subscription)
        )

        parsed = (
            lines
            | 'ParseAIS' >> beam.Map(parse_message)
            | 'FilterNone' >> beam.Filter(lambda x: x is not None)
        )

        keyed = (
            parsed
            | 'KeyByGeohash' >> beam.Map(lambda r: (r.get('geohash','none'), r))
        )

        collisions = (
            keyed
            | 'DetectCollisions' >> beam.ParDo(CollisionDoFn())
        )

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