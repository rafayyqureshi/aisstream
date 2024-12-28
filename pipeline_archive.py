#!/usr/bin/env python3
import os
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterWatermark, AccumulationMode
from apache_beam import window
from apache_beam.io import fileio

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
        f"{s(row.get('ship_length'))}\n"
    )

class MyPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_subscription', type=str, required=True)

def run():
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True

    # Subskrypcja do AIS (osobna np. 'ais-data-sub-archive')
    input_subscription = pipeline_options.view_as(MyPipelineOptions).input_subscription
    # Ścieżka do GCS
    output_prefix = os.getenv('HISTORY_OUTPUT_PREFIX') or 'gs://ais-collision-detection-bucket/ais_data/raw/ais_raw'

    with beam.Pipeline(options=pipeline_options) as p:
        lines = (
            p
            | 'ReadPubSub' >> beam.io.ReadFromPubSub(subscription=input_subscription)
        )

        parsed = (
            lines
            | 'Parse' >> beam.Map(parse_message)
            | 'FilterNone' >> beam.Filter(lambda x: x is not None)
        )

        # Okienkowanie np. 5-minutowe, z prostym triggerem "AfterWatermark"
        # i odrzuceniem (DISCARDING) starych danych w oknie
        windowed = (
            parsed
            | 'WindowInto' >> beam.WindowInto(
                FixedWindows(5*60),
                trigger=beam.transforms.trigger.AfterWatermark(),
                accumulation_mode=AccumulationMode.DISCARDING,
                allowed_lateness=0
            )
        )

        # Konwersja do CSV (dodajemy znak końca linii w to_csv)
        csv_lines = (
            windowed
            | 'ToCSV' >> beam.Map(to_csv)
        )

        # Zapis do GCS za pomocą FileIO (WriteToFiles):
        # Tworzymy pliki windowowane, omijając wewnętrzny GroupByKey w streaming.
        (
            csv_lines
            | 'WriteToFiles' >> fileio.WriteToFiles(
                path=output_prefix,
                file_naming=fileio.file_naming.prefix_naming(
                    prefix=output_prefix,
                    suffix='.csv'
                ),
                # liczba jednoczesnych writerów, by nie generować setek plików
                max_writers_per_bundle=2
            )
        )

if __name__ == '__main__':
    run()