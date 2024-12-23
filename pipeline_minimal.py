import os
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

def parse_message(record):
    try:
        data = json.loads(record.decode('utf-8'))
        required = ['mmsi','latitude','longitude','cog','sog','timestamp']
        if not all(k in data for k in required):
            return None
        return data
    except:
        return None

class MyPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_subscription', type=str, required=True)

def run():
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
    if not project_id:
        raise ValueError("GOOGLE_CLOUD_PROJECT not set")

    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True

    input_subscription = pipeline_options.view_as(MyPipelineOptions).input_subscription

    with beam.Pipeline(options=pipeline_options) as p:
        lines = (
            p
            | 'ReadPubSub' >> beam.io.ReadFromPubSub(
                subscription=input_subscription,
                with_attributes=False
            )
        )

        parsed = (
            lines
            | 'ParseJson' >> beam.Map(parse_message)
            | 'FilterNone' >> beam.Filter(lambda x: x is not None)
        )

        # Zamiast zapisu do plików: prosty Print lub cokolwiek innego
        _ = (
            parsed
            | 'PrintDebug' >> beam.Map(lambda x: print("AIS record:", x))
        )

if __name__ == '__main__':
    run()