import os
import json
import time
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.metrics import Metrics
from apache_beam.io import fileio
from apache_beam.transforms import window

def parse_message(record):
    parse_counter = Metrics.counter('ArchivePipeline', 'parse_calls')
    try:
        data = json.loads(record.decode('utf-8'))
        required = ['mmsi','latitude','longitude','cog','sog','timestamp']
        if not all(k in data for k in required):
            return None
        parse_counter.inc()
        return data
    except:
        logging.warning("Archive pipeline: Failed to parse JSON.")
        return None

def format_csv(r):
    format_counter = Metrics.counter('ArchivePipeline', 'format_csv_calls')
    format_counter.inc()

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
    logging.getLogger().setLevel(logging.INFO)

    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True

    input_subscription = pipeline_options.view_as(MyPipelineOptions).input_subscription

    with beam.Pipeline(options=pipeline_options) as p:
        lines = (
            p
            | 'ReadPubSub' >> beam.io.ReadFromPubSub(subscription=input_subscription)
        )

        parsed = (
            lines
            | 'ParseJson' >> beam.Map(parse_message)
            | 'FilterNone' >> beam.Filter(lambda x: x is not None)
        )

        csv_lines = (
            parsed
            | 'FormatCSV' >> beam.Map(format_csv)
        )

        # Dodajemy okno 5-minutowe, żeby co 5 min commitować pliki
        windowed = (
            csv_lines
            | 'Window5min' >> beam.WindowInto(window.FixedWindows(300))
        )

        (
            windowed
            | 'WriteFiles' >> fileio.WriteToFiles(
                path='gs://ais-collision-detection-bucket/ais_data/raw/',
                file_naming=fileio.default_file_naming(prefix='ais_raw-', suffix='.csv'),
                sink=lambda _: fileio.TextSink()  # Bez argumentów
            )
        )

if __name__ == '__main__':
    run()