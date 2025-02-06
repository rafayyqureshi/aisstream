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
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.transforms.trigger import AfterWatermark, AccumulationMode

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StaticDataManager:
    def __init__(self, gcs_bucket):
        self.static_data = {}
        self.gcs_bucket = gcs_bucket
        self._load_initial_data()

    def _load_initial_data(self):
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(self.gcs_bucket)
            blob = bucket.blob("ships_static_latest.json")
            
            if blob.exists():
                json_data = blob.download_as_text()
                data_list = json.loads(json_data)
                for item in data_list:
                    self.static_data[item["mmsi"]] = item
                logger.info(f"Loaded {len(self.static_data)} static records from GCS")
                
        except Exception as e:
            logger.error(f"Error loading initial data: {str(e)}")

    def update_static_data(self, element):
        mmsi = element["mmsi"]
        self.static_data[mmsi] = {
            "mmsi": mmsi,
            "ship_name": element["ship_name"],
            "dim_a": element.get("dim_a"),
            "dim_b": element.get("dim_b"),
            "dim_c": element.get("dim_c"),
            "dim_d": element.get("dim_d")
        }
        return element

    def get_snapshot(self):
        return json.dumps(list(self.static_data.values()), indent=2)

    def find_ship_name(self, mmsi):
        return self.static_data.get(mmsi, {}).get("ship_name")

class UploadSnapshotFn(beam.DoFn):
    def __init__(self, gcs_bucket):
        self.gcs_bucket = gcs_bucket

    def setup(self):
        self.data_manager = StaticDataManager(self.gcs_bucket)

    def process(self, elements):
        if not elements:
            return

        try:
            for element in elements:
                self.data_manager.update_static_data(element)
            
            json_data = self.data_manager.get_snapshot()
            storage_client = storage.Client()
            bucket = storage_client.bucket(self.gcs_bucket)
            blob = bucket.blob("ships_static_latest.json")
            blob.upload_from_string(json_data)
            logger.info(f"Snapshot saved with {len(self.data_manager.static_data)} ships")
            
        except Exception as e:
            logger.error(f"Snapshot error: {str(e)}")

def parse_ais_for_static(record_bytes):
    """
    Odczytuje wiadomości, wyłuskuje wartości statyczne statku.
    Zwraca obiekt {mmsi, ship_name, dim_a, dim_b, dim_c, dim_d}, albo None.
    Odrzuca (None) jeśli brak wymaganych pól lub ship_name to "Unknown".
    """
    try:
        data = json.loads(record_bytes.decode("utf-8"))
        required = ["mmsi", "ship_name", "dim_a", "dim_b", "dim_c", "dim_d"]
        for key in required:
            if key not in data or data[key] is None:
                return None
        # Jeżeli nazwa to "Unknown" => odrzucamy
        if str(data["ship_name"]).strip().lower() == "unknown":
            return None

        return {
            "mmsi": int(data["mmsi"]),
            "ship_name": data["ship_name"],
            "dim_a": float(data["dim_a"]),
            "dim_b": float(data["dim_b"]),
            "dim_c": float(data["dim_c"]),
            "dim_d": float(data["dim_d"]),
        }
    except Exception as e:
        logger.error(f"[parse_ais_for_static] Parse error: {str(e)}")
        return None

class EnrichPositionFn(beam.DoFn):
    def __init__(self, gcs_bucket):
        self.gcs_bucket = gcs_bucket
        self.data_manager = None

    def setup(self):
        self.data_manager = StaticDataManager(self.gcs_bucket)

    def process(self, element):
        if not element:
            return
            
        try:
            if element.get("ship_name", "Unknown").strip().lower() == "unknown":
                name = self.data_manager.find_ship_name(element["mmsi"])
                if name:
                    element["ship_name"] = name
            yield element
        except KeyError as e:
            logger.error(f"Enrichment error: {str(e)}")

def parse_positions(record_bytes):
    try:
        data = json.loads(record_bytes.decode("utf-8"))
        
        req = ["mmsi", "latitude", "longitude", "cog", "sog", "timestamp"]
        if not all(r in data for r in req):
            return None

        try:
            ts = datetime.datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))
        except:
            ts = datetime.datetime.utcnow()

        return {
            "mmsi": int(data["mmsi"]),
            "latitude": float(data["latitude"]),
            "longitude": float(data["longitude"]),
            "cog": float(data["cog"]),
            "sog": float(data["sog"]),
            "heading": float(data["heading"]) if data.get("heading") else None,
            "timestamp": ts.isoformat(),
            "ship_name": data.get("ship_name", "Unknown")
        }
    except Exception as e:
        logger.error(f"Position parse error: {str(e)}")
        return None

def run():
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    input_sub = os.getenv("INPUT_SUBSCRIPTION")
    region = os.getenv("REGION")
    temp_loc = os.getenv("TEMP_LOCATION")
    staging_loc = os.getenv("STAGING_LOCATION")
    job_name = os.getenv("JOB_NAME")
    gcs_bucket = os.getenv("GCS_BUCKET")
    table_positions = f"{project_id}.ais_dataset_us.ships_positions"

    pipeline_options = PipelineOptions(
        runner='DataflowRunner',
        project=project_id,
        region=region,
        temp_location=temp_loc,
        staging_location=staging_loc,
        job_name=job_name,
        streaming=True,
        save_main_session=True
    )

    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=input_sub)

        # Static data pipeline
        (
            lines
            | "ParseStatic" >> beam.Map(parse_ais_for_static)
            | "FilterStatic" >> beam.Filter(lambda x: x is not None)
            | "WindowStatic" >> beam.WindowInto(
                window.FixedWindows(600),  # 10 minut
                trigger=AfterWatermark(),
                accumulation_mode=AccumulationMode.DISCARDING
            )
            | "CombineStatic" >> beam.CombineGlobally(beam.combiners.ToListCombineFn()).without_defaults()
            | "UploadStatic" >> beam.ParDo(UploadSnapshotFn(gcs_bucket))
        )

        # Positions pipeline
        (
            lines
            | "ParsePositions" >> beam.Map(parse_positions)
            | "FilterPositions" >> beam.Filter(lambda x: x is not None)
            | "WindowPositions" >> beam.WindowInto(window.FixedWindows(15))
            | "EnrichPositions" >> beam.ParDo(EnrichPositionFn(gcs_bucket))
            | "WriteToBQ" >> WriteToBigQuery(
                table=table_positions,
                schema={
                    "fields": [
                        {"name": "mmsi", "type": "INTEGER"},
                        {"name": "ship_name", "type": "STRING"},
                        {"name": "latitude", "type": "FLOAT"},
                        {"name": "longitude", "type": "FLOAT"},
                        {"name": "cog", "type": "FLOAT"},
                        {"name": "sog", "type": "FLOAT"},
                        {"name": "heading", "type": "FLOAT"},
                        {"name": "timestamp", "type": "TIMESTAMP"}
                    ]
                },
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == "__main__":
    run()