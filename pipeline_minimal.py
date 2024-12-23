import os
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

def parse_message(record):
    """
    Minimalna funkcja parsująca strumień AIS z Pub/Sub.
    Zwraca None, jeśli dane niepełne lub błąd JSON.
    """
    try:
        data = json.loads(record.decode('utf-8'))
        required = ['mmsi','latitude','longitude','cog','sog','timestamp']
        if not all(k in data for k in required):
            return None
        return data
    except:
        return None

def format_ships_csv(r):
    """
    Zamienia dict AIS na pojedynczą linię CSV.
    """
    def s(x): 
        return '' if x is None else str(x)
    return (
        f"{s(r.get('mmsi'))},{s(r.get('latitude'))},{s(r.get('longitude'))},"
        f"{s(r.get('cog'))},{s(r.get('sog'))},{s(r.get('timestamp'))},"
        f"{s(r.get('ship_name'))},{s(r.get('ship_length'))}"
    )

class MyPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_subscription', type=str, required=True)

def run():
    # Sprawdzenie zmiennej środowiskowej z projektem GCP
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
    if not project_id:
        raise ValueError("GOOGLE_CLOUD_PROJECT not set")

    # Ustawienie opcji potoku – tryb streaming
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True

    # Subskrypcja Pub/Sub
    input_subscription = pipeline_options.view_as(MyPipelineOptions).input_subscription

    # Definiujemy pipeline bez żadnych okien i bez GroupByKey
    with beam.Pipeline(options=pipeline_options) as p:
        # 1. Odczyt z Pub/Sub (strumień)
        lines = (
            p
            | 'ReadPubSub' >> beam.io.ReadFromPubSub(
                subscription=input_subscription,
                with_attributes=False
                # Możesz usunąć 'timestamp_attribute' jeśli go nie używasz,
                # by uniknąć potencjalnych problemów.
            )
        )

        # 2. Parsowanie JSON, odfiltrowanie None
        parsed = (
            lines
            | 'ParseMsg' >> beam.Map(parse_message)
            | 'FilterNone' >> beam.Filter(lambda x: x is not None)
        )

        # 3. Formatowanie CSV i zapis do GCS
        (
            parsed
            | 'FormatShipsCSV' >> beam.Map(format_ships_csv)
            | 'WriteShipsCSV' >> beam.io.WriteToText(
                file_path_prefix='gs://ais-collision-detection-bucket/ais_data/ships/ships',
                file_name_suffix='.csv',
                shard_name_template='-SSSS-of-NNNN'
            )
        )

if __name__=='__main__':
    run()