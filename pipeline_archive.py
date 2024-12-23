import os
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

def parse_message(record):
    """
    Prosta funkcja parsująca dane AIS (JSON).
    Zwraca None, jeśli kluczowe pola są nieobecne.
    """
    try:
        data = json.loads(record.decode('utf-8'))
        required = ['mmsi','latitude','longitude','cog','sog','timestamp']
        if not all(k in data for k in required):
            return None
        return data
    except:
        return None

def format_csv(r):
    def s(x): return '' if x is None else str(x)
    return (
        f"{s(r.get('mmsi'))},{s(r.get('latitude'))},"
        f"{s(r.get('longitude'))},{s(r.get('cog'))},"
        f"{s(r.get('sog'))},{s(r.get('timestamp'))},"
        f"{s(r.get('ship_name'))},{s(r.get('ship_length'))}"
    )

class MyPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_subscription', type=str, required=True)
        parser.add_argument('--max_records', type=int, default=50000,
                            help='Ile rekordów wczytać z Pub/Sub (batch)')

def run():
    """
    Ten pipeline działa w trybie BATCH (streaming=False).
    Wczytuje max_records z Pub/Sub i kończy się, zapisując CSV w GCS.
    """
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = False  # BATCH mode

    input_subscription = pipeline_options.view_as(MyPipelineOptions).input_subscription
    max_records = pipeline_options.view_as(MyPipelineOptions).max_records

    with beam.Pipeline(options=pipeline_options) as p:
        # Ustawiamy parametry ReadFromPubSub:
        #  - max_num_records = max_records => pipeline zakończy się po odczytaniu tylu rekordów.
        #  - with_attributes=False => standard.
        lines = (
            p
            | 'ReadPubSub' >> beam.io.ReadFromPubSub(
                  subscription=input_subscription,
                  with_attributes=False,
                  max_num_records=max_records
              )
        )

        # Parsuj JSON
        parsed = (
            lines
            | 'Parse' >> beam.Map(parse_message)
            | 'FilterNone' >> beam.Filter(lambda x: x is not None)
        )

        # Zapis do plików CSV w GCS (batch => WriteToText działa poprawnie)
        (
            parsed
            | 'ToCSV' >> beam.Map(format_csv)
            | 'WriteCSV' >> beam.io.WriteToText(
                  file_path_prefix='gs://ais-collision-detection-bucket/ais_data/raw/ais_raw',
                  file_name_suffix='.csv',
                  shard_name_template='-SSSS-of-NNNN'
              )
        )

if __name__ == '__main__':
    run()