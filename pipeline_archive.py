import os
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import (
    AfterWatermark, 
    AccumulationMode
)
from apache_beam.utils.timestamp import Duration
from apache_beam.io import fileio
from apache_beam.io.fileio import TextSink

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
    Zwraca pojedynczy wiersz w formacie CSV:
      mmsi,latitude,longitude,cog,sog,timestamp,ship_name,ship_length
    """
    def s(x):
        return '' if x is None else str(x)
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

def run():
    # 1. Konfiguracja podstawowa
    pipeline_options = PipelineOptions()
    # Ustawiamy streaming, bo czytamy z Pub/Sub
    pipeline_options.view_as(StandardOptions).streaming = True

    input_subscription = pipeline_options.view_as(MyPipelineOptions).input_subscription

    with beam.Pipeline(options=pipeline_options) as p:

        # 2. Czytamy rekordy z Pub/Sub
        lines = (
            p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
                subscription=input_subscription
            )
        )

        # 3. Parsujemy JSON + odrzucamy None
        parsed = (
            lines
            | 'ParseJson' >> beam.Map(parse_message)
            | 'FilterInvalid' >> beam.Filter(lambda x: x is not None)
        )

        # 4. Format do CSV (string)
        csv_lines = (
            parsed
            | 'FormatCSV' >> beam.Map(format_csv)
        )

        # 5. Okienko 5-minutowe
        #    Po watermarku emitujemy dane, kasując stan (DISCARDING).
        windowed = (
            csv_lines
            | 'WindowInto5min' >> beam.WindowInto(
                FixedWindows(5*60),
                trigger=AfterWatermark(),
                accumulation_mode=AccumulationMode.DISCARDING,
                # jeżeli nie chcesz spóźnionych danych, to:
                # allowed_lateness=Duration(0)
            )
        )

        # 6. Aby plik nie był tworzony dla każdego rekordu,
        #    przypinamy sztuczny klucz (None), a potem groupujemy w obrębie okna.
        #    => Każde okno => 1 (lub kilka shardów).
        grouped = (
            windowed
            | 'KeyByNone' >> beam.Map(lambda row: (None, row))
            | 'GroupByNone' >> beam.GroupByKey()
        )

        # 7. Teraz zapisujemy do GCS. Tu używamy transformacji
        #    fileio.WriteToFiles, która obsługuje okna i jest
        #    poprawna dla streaming.
        #    Wewnątrz jednego klucza (None) = jednego okna
        #    generujemy 1-lub więcej shardów. 
        (
            grouped
            | 'WriteWindowedFiles'
              >> fileio.WriteToFiles(
                    path='gs://YOUR_BUCKET/ais_data/raw/',
                    file_naming=fileio.default_file_naming(
                        prefix='ais_raw-',
                        suffix='.csv'
                    ),
                    # Liczba równoległych writerów = liczba shardów. 
                    max_writers_per_bundle=1
                 )
              # W razie potrzeby format = text
              .with_format(TextSink())
        )

if __name__ == '__main__':
    run()