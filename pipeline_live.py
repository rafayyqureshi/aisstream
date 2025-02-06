#!/usr/bin/env python3
import os
import json
import logging
import datetime
from dotenv import load_dotenv
from google.cloud import storage
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import window

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StaticDataManager:
    def __init__(self):
        self.static_data = {}

    def update_static_data(self, element):
        """Aktualizuje dane statyczne w pamięci."""
        mmsi = element["mmsi"]
        if any(element.get(dim) for dim in ["dim_a", "dim_b", "dim_c", "dim_d"]):
            if mmsi not in self.static_data:
                self.static_data[mmsi] = {
                    "ship_name": element.get("ship_name", "Unknown"),
                    "dim_a": element.get("dim_a"),
                    "dim_b": element.get("dim_b"),
                    "dim_c": element.get("dim_c"),
                    "dim_d": element.get("dim_d"),
                    "last_update": datetime.datetime.utcnow()
                }
                logging.info(f"Updated static data for MMSI: {mmsi}")
        return element

    def create_snapshot(self):
        """Tworzy JSON z aktualnym stanem danych statycznych."""
        return json.dumps(list(self.static_data.values()), indent=2)

class UploadSnapshotFn(beam.DoFn):
    def __init__(self, gcs_bucket, file_name):
        self.gcs_bucket = gcs_bucket
        self.file_name = file_name
        self.data_manager = StaticDataManager()

    def process(self, elements):
        """Przetwarza partię elementów i uploaduje snapshot."""
        # Aktualizuj dane statyczne dla wszystkich elementów w oknie
        for element in elements:
            self.data_manager.update_static_data(element)
        
        # Utwórz i uploaduj snapshot
        json_data = self.data_manager.create_snapshot()
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.gcs_bucket)
        blob = bucket.blob(self.file_name)
        blob.upload_from_string(json_data, content_type="application/json")
        logging.info(f"Uploaded snapshot to {self.gcs_bucket}/{self.file_name}")

def parse_ais(record_bytes):
    try:
        data = json.loads(record_bytes.decode("utf-8"))
        req = ["mmsi", "latitude", "longitude", "cog", "sog", "timestamp"]
        if not all(r in data for r in req):
            return None
        data["mmsi"] = int(data["mmsi"])
        data["latitude"] = float(data["latitude"])
        data["longitude"] = float(data["longitude"])
        data["cog"] = float(data["cog"])
        data["sog"] = float(data["sog"])
        data["heading"] = float(data["heading"]) if data.get("heading") else None
        return data
    except Exception as e:
        logging.error(f"Error in parse_ais: {e}")
        return None

def run():
    logging.getLogger().setLevel(logging.INFO)
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "ais-collision-detection")
    input_sub = os.getenv("INPUT_SUBSCRIPTION", "projects/ais-collision-detection/subscriptions/ais-data-sub")
    region = os.getenv("REGION", "us-east1")
    temp_loc = os.getenv("TEMP_LOCATION", "gs://ais-collision-detection-bucket/temp")
    staging_loc = os.getenv("STAGING_LOCATION", "gs://ais-collision-detection-bucket/staging")
    job_name = os.getenv("JOB_NAME", "ais-collision-detection-job")
    gcs_bucket = os.getenv("GCS_BUCKET", "ais-collision-detection-bucket")
    file_name = "ships_static_latest.json"

    pipeline_options = PipelineOptions(
        runner='DataflowRunner',
        project=project_id,
        region=region,
        temp_location=temp_loc,
        staging_location=staging_loc,
        job_name=job_name,
        streaming=True
    )

    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=input_sub)

        parsed = (
            lines
            | "ParseAIS" >> beam.Map(parse_ais)
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
        )

        # Przetwarzanie i okresowe snapshoty
        _ = (
            parsed
            | "WindowInto" >> beam.WindowInto(window.FixedWindows(300))  # 5 minut
            | "GroupAll" >> beam.CombineGlobally(beam.combiners.ToListCombineFn())
            | "UploadSnapshot" >> beam.ParDo(UploadSnapshotFn(gcs_bucket, file_name))
        )

if __name__ == "__main__":
    run()