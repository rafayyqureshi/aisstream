import os
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterWatermark, AccumulationMode

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
    # Podstawowe ustawienia potoku
    pipeline_options = PipelineOptions()
    # Uruchamiamy w trybie streaming (Pub/Sub)
    pipeline_options.view_as(StandardOptions).streaming = True

    # Pobranie parametru z linii poleceń: --input_subscription
    input_subscription = pipeline_options.view_as(MyPipelineOptions).input_subscription

    with beam.Pipeline(options=pipeline_options) as p:

        # 1. Odczyt danych z Pub/Sub
        lines = (
            p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
                subscription=input_subscription
            )
        )

        # 2. Parsowanie JSON + filtrowanie None
        parsed = (
            lines
            | 'ParseJson' >> beam.Map(parse_message)
            | 'FilterInvalid' >> beam.Filter(lambda x: x is not None)
        )

        # 3. Formatowanie do CSV (jeden wiersz na rekord)
        csv_lines = (
            parsed
            | 'FormatCSV' >> beam.Map(format_csv)
        )

        # 4. Okno 5 minut z wyzwalaczem "po watermarku",
        #    aby Dataflow wiedziało, kiedy zamknąć i zapisać plik.
        #    AccumulationMode.DISCARDING oznacza, że po emisji okna kasujemy stan (oszczędza zasoby).
        windowed = (
            csv_lines
            | 'WindowInto5min' >> beam.WindowInto(
                FixedWindows(5 * 60),      # co 5 minut
                trigger=AfterWatermark(),  # prosty wyzwalacz "po watermarku"
                accumulation_mode=AccumulationMode.DISCARDING
            )
        )

        # 5. Zapis do GCS w formacie .csv, z minimalną liczbą shardów (np. 3),
        #    aby uniknąć generowania setek malutkich plików.
        (
            windowed
            | 'WriteToGCS' >> beam.io.WriteToText(
                file_path_prefix='gs://BUCKET/ais_data/raw/ais_raw',
                file_name_suffix='.csv',
                num_shards=3,
                shard_name_template='-SS-of-NN'
            )
        )

if __name__ == '__main__':
    run()