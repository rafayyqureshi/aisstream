import os
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterWatermark, AccumulationMode
from apache_beam import window
import math
import json
from datetime import datetime

# Ustawienia progów
CPA_THRESHOLD = 0.5
TCPA_THRESHOLD = 10.0

def parse_message_with_timestamp(message):
    """
    1) Dekoduj JSON
    2) Sprawdź czy zawiera wymagane pola
    3) Sparsuj 'timestamp' -> event_time w sekundach
    4) Zwróć (record_dict, event_ts)
    """
    try:
        record = json.loads(message.decode('utf-8'))
        required_fields = ['mmsi', 'latitude', 'longitude', 'cog', 'sog', 'timestamp']
        for f in required_fields:
            if f not in record:
                return None, None

        lat = record['latitude']
        lon = record['longitude']
        # weryfikacja lat/lon
        if not (isinstance(lat, (int, float)) and isinstance(lon, (int, float))
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

        # Parsuj timestamp
        timestamp_str = record['timestamp']
        if timestamp_str.endswith('+00:00'):
            timestamp_str = timestamp_str.replace('+00:00','Z')

        try:
            dt = datetime.fromisoformat(timestamp_str.replace('Z',''))
            event_ts = dt.timestamp()
        except:
            # fallback -> 0
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
    Zamieniamy (record, event_ts) -> TimestampedValue(record, event_ts).
    """
    rec, et = record_tuple
    if rec is None:
        return None
    return window.TimestampedValue(rec, et)

def compute_cpa_tcpa(ship_a, ship_b):
    # Pomijamy statki < 50 m
    if (ship_a['ship_length'] is None or ship_b['ship_length'] is None
        or ship_a['ship_length'] < 50 or ship_b['ship_length'] < 50):
        return 9999, -1

    latRef = (ship_a['latitude'] + ship_b['latitude'])/2
    scaleLat = 111000
    scaleLon = 111000 * math.cos(latRef*math.pi/180)

    def toXY(lat, lon):
        return [lon*scaleLon, lat*scaleLat]

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

    speedScale = 1852/60
    dvx_mpm = dvx * speedScale
    dvy_mpm = dvy * speedScale

    VV_m = dvx_mpm**2 + dvy_mpm**2
    PV_m = dx*dvx_mpm + dy*dvy_mpm

    if VV_m == 0:
        tcpa = 0.0
    else:
        tcpa = -PV_m / VV_m
    if tcpa < 0:
        return 9999, -1

    vxA_mpm = vxA*speedScale
    vyA_mpm = vyA*speedScale
    vxB_mpm = vxB*speedScale
    vyB_mpm = vyB*speedScale

    xA2 = xA + vxA_mpm*tcpa
    yA2 = yA + vyA_mpm*tcpa
    xB2 = xB + vxB_mpm*tcpa
    yB2 = yB + vyB_mpm*tcpa

    dist = math.sqrt((xA2 - xB2)**2 + (yA2 - yB2)**2)
    distNm = dist / 1852
    return distNm, tcpa

def find_collisions(geohash_record):
    gh, records = geohash_record
    ships_in_cell = list(records)

    filtered_ships = [s for s in ships_in_cell if s.get('ship_length') and s['ship_length'] >= 50]
    results = []
    for i in range(len(filtered_ships)):
        for j in range(i+1, len(filtered_ships)):
            ship_a = filtered_ships[i]
            ship_b = filtered_ships[j]
            cpa, tcpa = compute_cpa_tcpa(ship_a, ship_b)
            if tcpa >= 0 and cpa < CPA_THRESHOLD and tcpa < TCPA_THRESHOLD:
                results.append({
                    'mmsi_a': ship_a['mmsi'],
                    'mmsi_b': ship_b['mmsi'],
                    'timestamp': ship_a['timestamp'],
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
    return (f"{safe_str(record['mmsi'])},{safe_str(record['latitude'])},"
            f"{safe_str(record['longitude'])},{safe_str(record['cog'])},"
            f"{safe_str(record['sog'])},{safe_str(record['timestamp'])},"
            f"{safe_str(record['ship_name'])},{safe_str(record['ship_length'])}")

def format_collisions_csv(record):
    def safe_str(x):
        return '' if x is None else str(x)
    return (f"{safe_str(record['mmsi_a'])},{safe_str(record['mmsi_b'])},"
            f"{safe_str(record['timestamp'])},{safe_str(record['cpa'])},"
            f"{safe_str(record['tcpa'])},{safe_str(record['latitude_a'])},"
            f"{safe_str(record['longitude_a'])},{safe_str(record['latitude_b'])},"
            f"{safe_str(record['longitude_b'])}")

def run():
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
    if not project_id:
        raise ValueError("GOOGLE_CLOUD_PROJECT not set")

    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True
    input_subscription = pipeline_options.view_as(MyPipelineOptions).input_subscription

    with beam.Pipeline(options=pipeline_options) as p:
        # Odczyt z Pub/Sub i przypisywanie event-time
        parsed = (
            p
            | 'ReadPubSub' >> beam.io.ReadFromPubSub(subscription=input_subscription)
            | 'ParseMsg' >> beam.Map(parse_message_with_timestamp)  # -> (record, event_ts)
            | 'FilterNonNull' >> beam.Filter(lambda x: x[0] is not None)
            | 'MakeTimestamped' >> beam.Map(to_timestamped_value)    # -> TimestampedValue
            | 'FilterTSV' >> beam.Filter(lambda x: x is not None)
        )

        # Gałąź 1: Ships (okno 1min)
        ships_windowed = (
            parsed
            | 'WindowShips' >> beam.WindowInto(
                FixedWindows(60),
                allowed_lateness=0,
                trigger=AfterWatermark(),
                accumulation_mode=AccumulationMode.DISCARDING
            )
        )
        (
            ships_windowed
            | 'FormatShips' >> beam.Map(format_ships_csv)
            | 'WriteShipsCSV' >> beam.io.WriteToText(
                file_path_prefix='gs://ais-collision-detection-bucket/ais_data/ships/ships',
                file_name_suffix='.csv',
                shard_name_template='-SSSS-of-NNNN'
            )
        )

        # Gałąź 2: Collisions (okno 1min + GroupByKey)
        collisions_windowed = (
            parsed
            | 'WindowCollisions' >> beam.WindowInto(
                FixedWindows(60),
                allowed_lateness=0,
                trigger=AfterWatermark(),
                accumulation_mode=AccumulationMode.DISCARDING
            )
            | 'KeyByGH' >> beam.Map(lambda r: (r['geohash'], r))
            | 'GroupByGH' >> beam.GroupByKey()
            | 'FindCols' >> beam.FlatMap(find_collisions)
        )
        (
            collisions_windowed
            | 'FormatCols' >> beam.Map(format_collisions_csv)
            | 'WriteColsCSV' >> beam.io.WriteToText(
                file_path_prefix='gs://ais-collision-detection-bucket/ais_data/collisions/collisions',
                file_name_suffix='.csv',
                shard_name_template='-SSSS-of-NNNN'
            )
        )

if __name__ == '__main__':
    run()