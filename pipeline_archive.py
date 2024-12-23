import os
import json
import math
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io import fileio  # kluczowe
from apache_beam.io.filesystems import FileSystems

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
    """
    Zamienia dict AIS na pojedynczą linię CSV.
    """
    def s(x): return '' if x is None else str(x)
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
    """
    Ten pipeline działa w trybie STREAMING (streaming=True),
    ale do zapisu plików używa fileio.WriteToFiles (TextSink),
    co pozwala uniknąć wewnętrznego GroupByKey z WriteToText.
    """

    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True  # streaming!
    
    input_subscription = pipeline_options.view_as(MyPipelineOptions).input_subscription

    with beam.Pipeline(options=pipeline_options) as p:
        # 1) Odczyt z Pub/Sub w strumieniu
        lines = (
            p
            | 'ReadPubSub' >> beam.io.ReadFromPubSub(
                  subscription=input_subscription,
                  with_attributes=False
              )
        )

        # 2) Parsujemy JSON + filtrujemy None
        parsed = (
            lines
            | 'ParseJson' >> beam.Map(parse_message)
            | 'FilterNone' >> beam.Filter(lambda x: x is not None)
        )

        # 3) Format na CSV-linie
        csv_lines = (
            parsed
            | 'FormatCSV' >> beam.Map(format_csv)
        )

        # 4) Zapis do GCS za pomocą fileio.WriteToFiles
        #    Zamiast WriteToText (które w Python w streaming => błąd GroupByKey)
        (
            csv_lines
            | 'WriteToFiles' >> fileio.WriteToFiles(
                  path='gs://ais-collision-detection-bucket/ais_data/raw/',
                  file_naming=fileio.default_file_naming(prefix='ais_raw-', suffix='.csv'),
                  sink=lambda dest: fileio.TextSink(encoding='utf-8')
            )
        )

if __name__ == '__main__':
    run()