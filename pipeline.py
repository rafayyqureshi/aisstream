import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterWatermark, AccumulationMode
import math
import json
from datetime import datetime
from apache_beam import window  # <-- kluczowa zmiana

CPA_THRESHOLD = 0.5
TCPA_THRESHOLD = 10.0

def parse_message_with_timestamp(message):
    """
    Zwracamy krotkę (record, event_time), by móc przypisać TimestampedValue później.
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
                and -90<=lat<=90 and -180<=lon<=180):
            return None, None

        sl = record.get('ship_length', None)
        if sl=="Unknown":
            sl=None
        elif isinstance(sl,str):
            try:
                sl=float(sl)
            except:
                sl=None
        
        timestamp_str=record['timestamp']
        if timestamp_str.endswith('+00:00'):
            timestamp_str=timestamp_str.replace('+00:00','Z')

        # próbujemy sparsować event time
        try:
            dt=datetime.fromisoformat(timestamp_str.replace('Z',''))
            event_ts=dt.timestamp()
        except:
            # fallback
            event_ts=0

        parsed_rec={
            'mmsi':record['mmsi'],
            'latitude':lat,
            'longitude':lon,
            'cog':record['cog'],
            'sog':record['sog'],
            'timestamp':timestamp_str,
            'ship_name':record.get('ship_name'),
            'ship_length':sl,
            'geohash':record.get('geohash')
        }
        return parsed_rec, event_ts
    except:
        return None,None

def to_timestamped_value(record_tuple):
    """
    Zamieniamy (record, event_ts) na window.TimestampedValue(record, event_ts).
    """
    record, event_ts=record_tuple
    if record is None:
        return None
    return window.TimestampedValue(record, event_ts)

def compute_cpa_tcpa(ship_a, ship_b):
    # (logika CPA/TCPA bez zmian)
    ...

def find_collisions(geohash_record):
    # (logika find_collisions bez zmian)
    ...

class MyPipelineOptions(beam.PipelineOptions):
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
    project_id=os.getenv('GOOGLE_CLOUD_PROJECT')
    if not project_id:
        raise ValueError("GOOGLE_CLOUD_PROJECT not set")

    pipeline_options=PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming=True
    gcloud_options=pipeline_options.view_as(GoogleCloudOptions)
    gcloud_options.project=project_id
    gcloud_options.region=os.getenv('REGION','us-east1')

    input_subscription=pipeline_options.view_as(MyPipelineOptions).input_subscription

    with beam.Pipeline(options=pipeline_options) as p:
        parsed=(
            p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription=input_subscription)
            | 'ParseMessage' >> beam.Map(parse_message_with_timestamp)
            | 'FilterNone' >> beam.Filter(lambda x: x[0] is not None)  # x=(record, event_ts)
            | 'AssignTimestamps' >> beam.Map(to_timestamped_value)
            | 'FilterStamp' >> beam.Filter(lambda x: x is not None)
        )

        # Ships
        ships_windowed=(
            parsed
            | 'WindowForShips' >> beam.WindowInto(
                FixedWindows(60),
                allowed_lateness=0,
                trigger=AfterWatermark(),
                accumulation_mode=AccumulationMode.DISCARDING
            )
        )
        (ships_windowed
         | 'FormatShipsCSV' >> beam.Map(format_ships_csv)
         | 'WriteShipsToGCS' >> beam.io.WriteToText(
              file_path_prefix='gs://ais-collision-detection-bucket/ais_data/ships/ships',
              file_name_suffix='.csv',
              shard_name_template='-SSSS-of-NNNN'
         )
        )

        # Collisions
        collisions_windowed=(
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
        (collisions_windowed
         | 'FormatCollisionsCSV' >> beam.Map(format_collisions_csv)
         | 'WriteCollisionsToGCS' >> beam.io.WriteToText(
              file_path_prefix='gs://ais-collision-detection-bucket/ais_data/collisions/collisions',
              file_name_suffix='.csv',
              shard_name_template='-SSSS-of-NNNN'
         )
        )

if __name__=='__main__':
    run()