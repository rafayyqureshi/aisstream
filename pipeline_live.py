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
    def __init__(self, gcs_bucket):
        self.static_data = {}
        self.gcs_bucket = gcs_bucket
        self._load_initial_data()

    def _load_initial_data(self):
        """Wczytaj ostatni stan z GCS przy inicjalizacji"""
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(self.gcs_bucket)
            blob = bucket.blob("ships_static_latest.json")
            
            if blob.exists():
                json_data = blob.download_as_text()
                for item in json.loads(json_data):
                    self.static_data[item["mmsi"]] = item
                logger.info(f"Loaded {len(self.static_data)} ships from GCS")
                
        except Exception as e:
            logger.error(f"Error loading initial data: {str(e)}")

    def update_static_data(self, element):
        """Aktualizuj dane statyczne z nadpisaniem"""
        mmsi = element["mmsi"]
        self.static_data[mmsi] = {
            "mmsi": mmsi,
            "ship_name": element["ship_name"],
            "dim_a": element["dim_a"],
            "dim_b": element["dim_b"],
            "dim_c": element["dim_c"],
            "dim_d": element["dim_d"]
        }
        return element

    def get_snapshot(self):
        """Generuj JSON z aktualnymi danymi"""
        return json.dumps(list(self.static_data.values()), indent=2)

class UploadSnapshotFn(beam.DoFn):
    def __init__(self, gcs_bucket):
        self.gcs_bucket = gcs_bucket
        self.data_manager = None

    def setup(self):
        self.data_manager = StaticDataManager(self.gcs_bucket)

    def process(self, elements):
        """Przetwarzaj partię elementów i zapisz snapshot"""
        if not elements:
            return

        try:
            # Aktualizuj dane
            for element in elements:
                self.data_manager.update_static_data(element)
            
            # Zapisz do GCS
            json_data = self.data_manager.get_snapshot()
            storage_client = storage.Client()
            bucket = storage_client.bucket(self.gcs_bucket)
            blob = bucket.blob("ships_static_latest.json")
            blob.upload_from_string(json_data, content_type="application/json")
            logger.info(f"Snapshot updated: {len(self.data_manager.static_data)} ships")
            
        except Exception as e:
            logger.error(f"Upload failed: {str(e)}")

def parse_ais(record_bytes):
    try:
        data = json.loads(record_bytes.decode("utf-8"))
        
        # Wymagane pola: mmsi, ship_name, dim_a, dim_b, dim_c, dim_d
        required = ["mmsi", "ship_name", "dim_a", "dim_b", "dim_c", "dim_d"]
        for key in required:
            if key not in data or data[key] is None:
                return None

        # Odrzuć wiadomość, jeśli nazwa statku to "Unknown" (bez względu na wielkość liter)
        if str(data["ship_name"]).strip().lower() == "unknown":
            return None

        return {
            "mmsi": int(data["mmsi"]),
            "ship_name": data["ship_name"],
            "dim_a": float(data["dim_a"]),
            "dim_b": float(data["dim_b"]),
            "dim_c": float(data["dim_c"]),
            "dim_d": float(data["dim_d"])
        }
        
    except Exception as e:
        logger.error(f"Parse error: {str(e)}")
        return None

def run():
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "ais-collision-detection")
    input_sub = os.getenv("INPUT_SUBSCRIPTION", "projects/ais-collision-detection/subscriptions/ais-data-sub")
    region = os.getenv("REGION", "us-east1")
    temp_loc = os.getenv("TEMP_LOCATION", "gs://ais-collision-detection-bucket/temp")
    staging_loc = os.getenv("STAGING_LOCATION", "gs://ais-collision-detection-bucket/staging")
    job_name = os.getenv("JOB_NAME", "ais-collision-detection-job")
    gcs_bucket = os.getenv("GCS_BUCKET", "ais-collision-detection-bucket")

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
        (
            p 
            | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=input_sub)
            | "ParseAIS" >> beam.Map(parse_ais)
            | "FilterInvalid" >> beam.Filter(lambda x: x is not None)
            | "WindowInto" >> beam.WindowInto(window.FixedWindows(600))  # 10 minutowe okno
            | "CombineGlobally" >> beam.CombineGlobally(beam.combiners.ToListCombineFn()).without_defaults()
            | "UploadSnapshot" >> beam.ParDo(UploadSnapshotFn(gcs_bucket))
        )

if __name__ == "__main__":
    run()