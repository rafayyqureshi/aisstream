import os
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io import fileio
from apache_beam.transforms import window

def parse_message(record):
    """
    Parsuje AIS rekord z Pub/Sub (kodowany JSON).
    Zwraca None, jeśli brakuje pól mmsi, latitude, ...
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
    """
    Zwraca string w postaci: mmsi,lat,lon,cog,sog,timestamp,ship_name,ship_length
    """
    def s(x): 
        return '' if x is None else str(x)
    return (
        f"{s(r.get('mmsi'))},{s(r.get('latitude'))},"
        f"{s(r.get('longitude'))},{s(r.get('cog'))},"
        f"{s(r.get('sog'))},{s(r.get('timestamp'))},"
        f"{s(r.get('ship_name'))},{s(r.get('ship_length'))}\n"
    )

class MyPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_subscription', type=str, required=True)

def run():
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True

    input_subscription = pipeline_options.view_as(MyPipelineOptions).input_subscription

    with beam.Pipeline(options=pipeline_options) as p:
        # Odczyt z Pub/Sub
        lines = (
            p
            | 'ReadPubSub' >> beam.io.ReadFromPubSub(subscription=input_subscription)
        )

        # Parsowanie
        parsed = (
            lines
            | 'ParseJson' >> beam.Map(parse_message)
            | 'FilterNone' >> beam.Filter(lambda x: x is not None)
        )

        # Konwersja do CSV
        csv_lines = (
            parsed
            | 'FormatCSV' >> beam.Map(format_csv)
        )

        # Ustawiamy okno 5-minutowe
        windowed = (
            csv_lines
            | 'WindowInto5min' >> beam.WindowInto(window.FixedWindows(300))
        )

        # Zapis do GCS
        (
            windowed
            | 'FormatToCSV' >> beam.Map(lambda x: format_csv(x))
            | 'WriteRaw' >> beam.io.WriteToText(
                file_path_prefix='gs://BUCKET/ais_data/raw/ais_raw',
                file_name_suffix='.csv',
                # Ustawiamy liczbę shardów tak, żeby nie było ich 300 w 5 minut
                num_shards=3,  
                shard_name_template='-SS-of-NN'
            )
        )

if __name__ == '__main__':
    run()