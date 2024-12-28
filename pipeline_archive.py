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
        required = ['mmsi', 'latitude', 'longitude', 'cog', 'sog', 'timestamp']
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
    def s(v): 
        return '' if v is None else str(v)
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

    # Subskrypcja AIS - np. "projects/.../subscriptions/ais-data-sub-archive"
    input_subscription = pipeline_options.view_as(MyPipelineOptions).input_subscription

    # Ścieżka do GCS, można nadpisać przez zmienną środowiskową
    output_prefix = os.getenv('HISTORY_OUTPUT_PREFIX') or 'gs://ais-collision-detection-bucket/ais_data/raw/ais_raw'

    with beam.Pipeline(options=pipeline_options) as p:
        # 1) Czytanie z Pub/Sub
        lines = (
            p
            | 'ReadPubSub' >> beam.io.ReadFromPubSub(subscription=input_subscription)
        )

        # 2) Parsowanie JSON → dict
        parsed = (
            lines
            | 'ParseJSON' >> beam.Map(parse_message)
            | 'FilterNone' >> beam.Filter(lambda x: x is not None)
        )

        # 3) Okienkowanie 5-minutowe + prosty trigger
        windowed = (
            parsed
            | 'WindowInto5min' >> beam.WindowInto(
                FixedWindows(5 * 60),
                trigger=AfterWatermark(),             # po Watermarku
                accumulation_mode=AccumulationMode.DISCARDING,
                allowed_lateness=0
            )
        )

        # 4) Konwersja do CSV
        csv_lines = (
            windowed
            | 'ToCSV' >> beam.Map(to_csv)
        )

        # 5) Zapis do GCS za pomocą fileio.WriteToFiles
        #    Używamy default_file_naming, by uniknąć problemów ze starszymi wersjami Beam
        (
            csv_lines
            | 'WriteToFiles' >> fileio.WriteToFiles(
                path=output_prefix,
                file_naming=fileio.default_file_naming('.csv'),
                # Ogranicza liczbę writerów (a co za tym idzie liczbę plików) 
                max_writers_per_bundle=2
            )
        )


if __name__ == '__main__':
    run()