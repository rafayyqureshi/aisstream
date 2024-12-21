import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterWatermark, AccumulationMode
from apache_beam import window
import math
import json
from datetime import datetime

CPA_THRESHOLD = 0.5
TCPA_THRESHOLD = 10.0

def parse_message_with_timestamp(message):
    """
    1) Dekoduj JSON.
    2) Sprawdź pola required_fields.
    3) Parsuj 'timestamp' do event_time w sekundach (POSIX).
    Zwracaj tuple: (record_dict, event_time).
    """
    try:
        record = json.loads(message.decode('utf-8'))
        required_fields = ['mmsi', 'latitude', 'longitude', 'cog', 'sog', 'timestamp']
        for f in required_fields:
            if f not in record:
                return None, None

        lat = record['latitude']
        lon = record['longitude']
        if not (isinstance(lat, (int,float)) and isinstance(lon, (int,float))
                and -90 <= lat <= 90 and -180 <= lon <= 180):
            return None, None

        sl = record.get('ship_length')
        if sl == "Unknown":
            sl = None
        elif isinstance(sl, str):
            try:
                sl = float(sl)
            except:
                sl = None

        # Parsowanie event-time
        timestamp_str = record['timestamp']
        if timestamp_str.endswith('+00:00'):
            timestamp_str = timestamp_str.replace('+00:00','Z')

        try:
            dt = datetime.fromisoformat(timestamp_str.replace('Z',''))
            event_ts = dt.timestamp()
        except:
            # fallback, np. 0 lub "processing time"
            event_ts = 0

        parsed_record = {
            'mmsi': record['mmsi'],
            'latitude': lat,
            'longitude': lon,
            'cog': record['cog'],
            'sog': record['sog'],
            'timestamp': timestamp_str,
            'ship_name': record.get('ship_name'),
            'ship_length': sl,
            'geohash': record.get('geohash')
        }
        return parsed_record, event_ts
    except:
        return None, None

def to_timestamped_value(record_tuple):
    """
    Zamieniamy (dict, event_ts) na TimestampedValue(dict, event_ts),
    co pozwala Beam używać event-time.
    """
    rec, et = record_tuple
    if rec is None:
        return None
    return window.TimestampedValue(rec, et)

def compute_cpa_tcpa(ship_a, ship_b):
    if ship_a['ship_length'] is None or ship_b['ship_length'] is None:
        return (9999, -1)
    if ship_a['ship_length'] < 50 or ship_b['ship_length'] < 50:
        return (9999, -1)

    latRef = (ship_a['latitude'] + ship_b['latitude']) / 2
    scaleLat = 111000
    scaleLon = 111000 * math.cos(latRef * math.pi / 180)

    def toXY(lat, lon):
        return [lon * scaleLon, lat * scaleLat]

    xA, yA = toXY(ship_a['latitude'], ship_a['longitude'])
    xB, yB = toXY(ship_b['latitude'], ship_b['longitude'])

    sogA = ship_a['sog']
    sogB = ship_b['sog']

    def cogToVector(cogDeg, sogNmH):
        cog_rad = math.radians(cogDeg)
        vx = sogNmH * math.sin(cog_rad)
        vy = sogNmH * math.cos(cog_rad)
        return vx, vy

    vxA, vyA = cogToVector(ship_a['cog'], sogA)
    vxB, vyB = cogToVector(ship_b['cog'], sogB)

    dx = xA - xB
    dy = yA - yB
    dvx = vxA - vxB
    dvy = vyA - vyB

    speedScale = 1852 / 60
    dvx_mpm = dvx * speedScale
    dvy_mpm = dvy * speedScale

    VV_m = dvx_mpm**2 + dvy_mpm**2
    PV_m = dx * dvx_mpm + dy * dvy_mpm

    if VV_m == 0:
        tcpa = 0.0
    else:
        tcpa = -PV_m / VV_m
    if tcpa < 0:
        return 9999, -1

    vxA_mpm = vxA * speedScale
    vyA_mpm = vyA * speedScale
    vxB_mpm = vxB * speedScale
    vyB_mpm = vyB * speedScale

    xA2 = xA + vxA_mpm * tcpa
    yA2 = yA + vyA_mpm * tcpa
    xB2 = xB + vxB_mpm * tcpa
    yB2 = yB + vyB_mpm * tcpa

    dist = math.sqrt((xA2 - xB2)**2 + (yA2 - yB2)**2)
    distNm = dist / 1852
    return distNm, tcpa

def find_collisions(geohash_record):
    gh, records = geohash_record
    ships_in_cell = list(records)
    # Filtr statków <50m
    filtered_ships = [s for s in ships_in_cell if s.get('ship_length') is not None and s['ship_length'] >= 50.0]

    results = []
    for i in range(len(filtered_ships)):
        for j in range(i + 1, len(filtered_ships)):
            ship_a = filtered_ships[i]
            ship_b = filtered_ships[j]
            cpa, tcpa = compute_cpa_tcpa(ship_a, ship_b)
            if tcpa >= 0 and cpa < CPA_THRESHOLD and tcpa < TCPA_THRESHOLD:
                results.append({
                    'mmsi_a': ship_a['mmsi'],
                    'mmsi_b': ship_b['mmsi'],
                    'timestamp': ship_a['timestamp'],  # minimalnie z ship_a
                    'cpa': cpa,
                    'tcpa': tcpa,
                    'latitude_a': ship_a['latitude'],
                    'longitude_a': ship_a['longitude'],
                    'latitude_b': ship_b['latitude'],
                    'longitude_b': ship_b['longitude']
                })
    return results

from apache_beam.options.pipeline_options import PipelineOptions

class MyPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_subscription', type=str, required=True)

def format_ships_csv(record):
    def safe_str(x):
        return '' if x is None else str(x)
    return f"{safe_str(record['mmsi'])},{safe_str(record['latitude'])},{safe_str(record['longitude'])},{safe_str(record['cog'])},{safe_str(record['sog'])},{safe_str(record['timestamp'])},{safe_str(record['ship_name'])},{safe_str(record['ship_length'])}"

def format_collisions_csv(record):
    def safe_str(x):
        return '' if x is None else str(x)
    return f"{safe_str(record['mmsi_a'])},{safe_str(record['mmsi_b'])},{safe_str(record['timestamp'])},{safe_str(record['cpa'])},{safe_str(record['tcpa'])},{safe_str(record['latitude_a'])},{safe_str(record['longitude_a'])},{safe_str(record['latitude_b'])},{safe_str(record['longitude_b'])}"

def run():
    import sys
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
    if not project_id:
        raise ValueError("GOOGLE_CLOUD_PROJECT not set")

    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True

    input_subscription = pipeline_options.view_as(MyPipelineOptions).input_subscription

    with beam.Pipeline(options=pipeline_options) as p:
        # 1) Odczyt z Pub/Sub
        parsed = (
            p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription=input_subscription)
            | 'ParseWithTimestamp' >> beam.Map(parse_message_with_timestamp)  # (dict, event_ts)
            | 'FilterValid' >> beam.Filter(lambda x: x[0] is not None)       # (record, ts)
            | 'ToTimestampedValue' >> beam.Map(to_timestamped_value)         # window.TimestampedValue(...)
            | 'FilterIfNone' >> beam.Filter(lambda x: x is not None)
        )

        # 2) Ships window 1 min
        ships_windowed = (
            parsed
            | 'WindowForShips' >> beam.WindowInto(
                FixedWindows(60),
                allowed_lateness=0,
                trigger=AfterWatermark(),
                accumulation_mode=AccumulationMode.DISCARDING
            )
        )

        (
            ships_windowed
            | 'FormatShipsCSV' >> beam.Map(format_ships_csv)
            | 'WriteShipsToGCS' >> beam.io.WriteToText(
                file_path_prefix='gs://ais-collision-detection-bucket/ais_data/ships/ships',
                file_name_suffix='.csv',
                shard_name_template='-SSSS-of-NNNN'
            )
        )

        # 3) Collisions window 1 min, group by geohash
        collisions_windowed = (
            parsed
            | 'WindowForCollisions' >> beam.WindowInto(
                FixedWindows(60),
                allowed_lateness=0,
                trigger=AfterWatermark(),
                accumulation_mode=AccumulationMode.DISCARDING
            )
            | 'KeyByGeohash' >> beam.Map(lambda r: (r['geohash'], r))
            | 'GroupByGeohash' >> beam.GroupByKey()
            | 'FindCollisions' >> beam.FlatMap(find_collisions)
        )

        (
            collisions_windowed
            | 'FormatCollisionsCSV' >> beam.Map(format_collisions_csv)
            | 'WriteCollisionsToGCS' >> beam.io.WriteToText(
                file_path_prefix='gs://ais-collision-detection-bucket/ais_data/collisions/collisions',
                file_name_suffix='.csv',
                shard_name_template='-SSSS-of-NNNN'
            )
        )

if __name__ == '__main__':
    run()