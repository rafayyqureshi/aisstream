import os
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterWatermark, AccumulationMode
from apache_beam import window
import math
import json
from datetime import datetime

# Progi, jeśli chcesz ewentualnie użyć do filtracji w DoFn
CPA_THRESHOLD = 0.5
TCPA_THRESHOLD = 10.0

def parse_message(record):
    """
    Prosta funkcja parse. 
    Zakładamy, że Beam przypisze event-time z PubSub (publishTime) 
    jeśli w ReadFromPubSub użyjesz 'timestamp_attribute'.
    """
    try:
        data = json.loads(record.decode('utf-8'))
        # Minimalna weryfikacja kluczowych pól
        required = ['mmsi','latitude','longitude','cog','sog','timestamp']
        if not all(k in data for k in required):
            return None
        return data
    except:
        return None

def format_ships_csv(r):
    # Formatowanie surowych danych AIS w CSV
    def s(x): return '' if x is None else str(x)
    return (f"{s(r.get('mmsi'))},{s(r.get('latitude'))},"
            f"{s(r.get('longitude'))},{s(r.get('cog'))},"
            f"{s(r.get('sog'))},{s(r.get('timestamp'))},"
            f"{s(r.get('ship_name'))},{s(r.get('ship_length'))}")

class MyPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_subscription', type=str, required=True)

def run():
    # 1) Ustawiamy parametry
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
    if not project_id:
        raise ValueError("GOOGLE_CLOUD_PROJECT not set")

    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True

    input_subscription = pipeline_options.view_as(MyPipelineOptions).input_subscription

    # 2) Budujemy potok
    with beam.Pipeline(options=pipeline_options) as p:

        # Odczyt z Pub/Sub w trybie streaming
        # UWAGA: jeśli subskrypcja nie udostępnia publishTime, usuń 'timestamp_attribute'
        lines = (
            p
            | 'ReadPubSub' >> beam.io.ReadFromPubSub(
                  subscription=input_subscription,
                  with_attributes=False,
                  timestamp_attribute='publishTime'
              )
        )

        # Parsowanie surowych danych AIS
        parsed = (
            lines
            | 'Parse' >> beam.Map(parse_message)
            | 'FilterNotNone' >> beam.Filter(lambda r: r is not None)
        )

        # [Gałąź surowa AIS -> zapis do GCS co 1 minutę]
        # Odtąd można ewentualnie w cron/Cloud Scheduler ładować do BQ (batch load)
        ships_windowed = (
            parsed
            | 'WindowShips' >> beam.WindowInto(
                FixedWindows(60),
                trigger=AfterWatermark(),
                accumulation_mode=AccumulationMode.DISCARDING,
                allowed_lateness=0
            )
        )

        (
            ships_windowed
            | 'ShipsCSV' >> beam.Map(format_ships_csv)
            | 'WriteShipsCSV' >> beam.io.WriteToText(
                file_path_prefix='gs://ais-collision-detection-bucket/ais_data/ships/ships',
                file_name_suffix='.csv',
                shard_name_template='-SSSS-of-NNNN'
            )
        )

        # [Tu brak groupByKey w streaming]
        # Ewentualne kolizje można wykrywać inną metodą (Stateful DoFn),
        # lub w potoku batch offline z załadowanych plików.

if __name__=='__main__':
    run()