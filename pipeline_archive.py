#!/usr/bin/env python3
import os
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.io import WriteToText

def parse_message(record):
    """
    Parsuje AIS (JSON) z Pub/Sub, zwraca dict (lub None, jeśli braki).
    """
    try:
        data = json.loads(record.decode('utf-8'))
        required = ['mmsi','latitude','longitude','cog','sog','timestamp']
        if not all(k in data for k in required):
            return None
        return data
    except:
        return None

def to_csv(row):
    """
    Zamiana na CSV:
    mmsi,latitude,longitude,cog,sog,timestamp,ship_name,ship_length
    """
    def s(v): return '' if v is None else str(v)
    return (
        f"{s(row.get('mmsi'))},"
        f"{s(row.get('latitude'))},"
        f"{s(row.get('longitude'))},"
        f"{s(row.get('cog'))},"
        f"{s(row.get('sog'))},"
        f"{s(row.get('timestamp'))},"
        f"{s(row.get('ship_name'))},"
        f"{s(row.get('ship_length'))}"
    )

def run():
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True

    # Subskrypcja do AIS (np. ta sama lub osobna)
    input_subscription = os.getenv('HISTORY_INPUT_SUB') or 'projects/ais-collision-detection/subscriptions/ais-data-sub'
    output_prefix = os.getenv('HISTORY_OUTPUT_PREFIX') or 'gs://ais-collision-detection-bucket/ais_data/raw/ais_raw'

    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | 'ReadPubSub' >> beam.io.ReadFromPubSub(subscription=input_subscription)

        parsed = (
            lines
            | 'Parse' >> beam.Map(parse_message)
            | 'FilterNone' >> beam.Filter(lambda x: x is not None)
        )

        # Okienkowanie np. 5-minutowe
        windowed = (
            parsed
            | 'FixedWindow5min' >> beam.WindowInto(FixedWindows(5*60))
        )

        # konwersja do CSV
        csv_lines = windowed | 'ToCSV' >> beam.Map(to_csv)

        # zapis do GCS
        (
            csv_lines
            | 'WriteCSV' >> WriteToText(
                file_path_prefix=output_prefix,
                file_name_suffix='.csv',
                num_shards=2,             # żeby nie tworzyć setek shardów
                shard_name_template='-SS-of-NN'
            )
        )

if __name__ == '__main__':
    run()