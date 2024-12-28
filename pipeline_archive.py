import os
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterWatermark, AccumulationMode
from apache_beam.utils.timestamp import Duration
from apache_beam.io import fileio
from apache_beam.io.fileio import TextSink

def parse_message(record):
    """
    Parsuje AIS rekord z Pub/Sub (kodowany JSON).
    Zwraca None, jeśli brakuje pól [mmsi, latitude, longitude, cog, sog, timestamp].
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
    # 1. Konfiguracja potoku
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True
    
    input_subscription = pipeline_options.view_as(MyPipelineOptions).input_subscription

    with beam.Pipeline(options=pipeline_options) as p:

        # 2. Czytamy rekordy z Pub/Sub
        lines = (
            p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription=input_subscription)
        )

        # 3. Parsujemy rekordy JSON i odrzucamy None
        parsed = (
            lines
            | 'ParseJson' >> beam.Map(parse_message)
            | 'FilterInvalid' >> beam.Filter(lambda x: x is not None)
        )

        # 4. Format do CSV (pojedyncza linijka)
        csv_lines = (
            parsed
            | 'FormatCSV' >> beam.Map(format_csv)
        )

        # 5. Okienko 5-minutowe + Watermark => DISCARDING
        windowed = (
            csv_lines
            | 'WindowInto5min' >> beam.WindowInto(
                FixedWindows(5 * 60),
                trigger=AfterWatermark(),
                accumulation_mode=AccumulationMode.DISCARDING,
                # allowed_lateness=Duration(0)  # jeśli nie chcesz spóźnionych danych
            )
        )

        # 6. GroupByKey, żeby zebrać rekordy z jednego okna w jedną grupę
        grouped = (
            windowed
            | 'KeyByNone' >> beam.Map(lambda row: (None, row))
            | 'GroupByNone' >> beam.GroupByKey()
        )
        # Teraz każdy element w grouped to: (None, [linie_csv_z_tego_okna])

        # 7. Sklejamy listę linii w jeden długi string z \n
        #    => każda grupa/okno => 1 "duży" string
        joined_lines = (
            grouped
            | 'JoinLines' >> beam.Map(lambda kv: "\n".join(kv[1]))
        )

        # 8. Zapis do GCS przy pomocy fileio.WriteToFiles
        #    Tworzymy pliki CSV. Każde okno => co najmniej jeden plik.
        #    Tu używamy sink=lambda dest: fileio.TextSink().
        (
            joined_lines
            | 'WriteWindowedFiles'
            >> fileio.WriteToFiles(
                path='gs://YOUR_BUCKET/ais_data/raw/',
                file_naming=fileio.default_file_naming(prefix='ais_raw-', suffix='.csv'),
                max_writers_per_bundle=1,  # lub zmniejsz/zwiększ w zależności od równoległości
                sink=lambda dest: TextSink()
            )
        )

if __name__ == '__main__':
    run()