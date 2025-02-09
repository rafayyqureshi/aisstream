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
from apache_beam.transforms.trigger import AfterWatermark, AccumulationMode
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

# Load environment variables and configure logging
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GCS_BUCKET = os.getenv("GCS_BUCKET")
if not GCS_BUCKET:
    raise ValueError("Missing GCS_BUCKET environment variable.")

###############################################################################
# Static data manager classes and functions
###############################################################################
class StaticDataManager:
    def __init__(self, gcs_bucket: str):
        self.gcs_bucket = gcs_bucket
        self.static_data = {}
        self._load_initial_data()

    def _load_initial_data(self):
        """Load the snapshot file (ships_static_latest.json) from GCS (if it exists)."""
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(self.gcs_bucket)
            blob = bucket.blob("ships_static_latest.json")
            if blob.exists():
                content = blob.download_as_text()
                records = json.loads(content)
                for item in records:
                    self.static_data[item["mmsi"]] = item
                logger.info(f"[StaticDataManager] Loaded {len(self.static_data)} static records from GCS.")
            else:
                logger.info("[StaticDataManager] No ships_static_latest.json found (starting empty).")
        except Exception as e:
            logger.warning(f"[StaticDataManager] Could not load initial data: {e}")

    def update_static(self, record: dict):
        """Update static data by storing the record under its MMSI."""
        mmsi = record["mmsi"]
        self.static_data[mmsi] = record
        return record

    def get_snapshot(self):
        """Return a JSON snapshot of the current static data."""
        return json.dumps(list(self.static_data.values()), indent=2)

    def find_ship_name(self, mmsi: int):
        return self.static_data.get(mmsi, {}).get("ship_name")

class UploadSnapshotFn(beam.DoFn):
    """DoFn that batches static data records from a window and writes a snapshot to GCS."""
    def __init__(self, gcs_bucket: str):
        self.gcs_bucket = gcs_bucket

    def setup(self):
        self.manager = StaticDataManager(self.gcs_bucket)

    def process(self, batch):
        if not batch:
            return
        try:
            for record in batch:
                self.manager.update_static(record)
            json_data = self.manager.get_snapshot()
            storage_client = storage.Client()
            bucket = storage_client.bucket(self.gcs_bucket)
            blob = bucket.blob("ships_static_latest.json")
            blob.upload_from_string(json_data, content_type="application/json")
            logger.info(f"[UploadSnapshotFn] Snapshot updated with {len(self.manager.static_data)} static records.")
        except Exception as e:
            logger.error(f"[UploadSnapshotFn] Error during snapshot upload: {e}")

###############################################################################
# Parsing function for AIS messages (unified for static and position data)
###############################################################################
def parse_ais(record_bytes: bytes):
    try:
        raw = json.loads(record_bytes.decode("utf-8"))
    except Exception:
        return []
    
    outputs = []
    
    # Check for static data (required fields: mmsi, ship_name, dim_a, dim_b, dim_c, dim_d)
    static_fields = ["mmsi", "ship_name", "dim_a", "dim_b", "dim_c", "dim_d"]
    if all(field in raw and raw[field] is not None for field in static_fields):
        if str(raw["ship_name"]).strip().lower() != "unknown":
            outputs.append({
                "type": "static",
                "mmsi": int(raw["mmsi"]),
                "ship_name": str(raw["ship_name"]),
                "dim_a": float(raw["dim_a"]),
                "dim_b": float(raw["dim_b"]),
                "dim_c": float(raw["dim_c"]),
                "dim_d": float(raw["dim_d"]),
            })
    
    # Check for position data (required: mmsi, latitude, longitude, cog, sog, timestamp)
    pos_fields = ["mmsi", "latitude", "longitude", "cog", "sog", "timestamp"]
    if all(field in raw and raw[field] is not None for field in pos_fields):
        try:
            dt = datetime.datetime.fromisoformat(str(raw["timestamp"]).replace("Z", "+00:00"))
        except:
            # Reject record with invalid timestamp
            return []
        heading = None
        if "heading" in raw and raw["heading"] is not None:
            try:
                heading = float(raw["heading"])
            except:
                heading = None
        outputs.append({
            "type": "position",
            "mmsi": int(raw["mmsi"]),
            "latitude": float(raw["latitude"]),
            "longitude": float(raw["longitude"]),
            "cog": float(raw["cog"]),
            "sog": float(raw["sog"]),
            "heading": heading,
            "timestamp": dt.isoformat() + "Z",
            "ship_name": str(raw.get("ship_name", "Unknown"))
        })
    
    return outputs

###############################################################################
# DoFn that enriches position data by replacing "Unknown" ship_name using static data
###############################################################################
class EnrichPositionFn(beam.DoFn):
    def __init__(self, gcs_bucket: str):
        self.gcs_bucket = gcs_bucket

    def setup(self):
        self.manager = StaticDataManager(self.gcs_bucket)

    def process(self, element):
        if element.get("ship_name", "Unknown").strip().lower() == "unknown":
            known = self.manager.find_ship_name(element["mmsi"])
            if known:
                element["ship_name"] = known
                logger.info(f"[EnrichPositionFn] Enriched MMSI {element['mmsi']} with ship_name {known}.")
        yield element

###############################################################################
# Main pipeline
###############################################################################
def run():
    # Environment variables
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "ais-collision-detection")
    input_sub = os.getenv("INPUT_SUBSCRIPTION", "projects/ais-collision-detection/subscriptions/ais-data-sub")
    region = os.getenv("REGION", "us-east1")
    temp_loc = os.getenv("TEMP_LOCATION", "gs://ais-collision-detection-bucket/temp")
    staging_loc = os.getenv("STAGING_LOCATION", "gs://ais-collision-detection-bucket/staging")
    job_name = os.getenv("JOB_NAME", "ais-collision-detection-job")
    table_positions = f"{project_id}.ais_dataset_us.ships_positions"

    # Schema for positions table in BigQuery
    positions_schema = {
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
    }

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

        # Unified parser: produces 0..2 dictionaries (static and/or position)
        parsed = lines | "ParseAISUnified" >> beam.FlatMap(parse_ais)

        # 1) STATIC pipeline: 10-minute window to batch static records and write snapshot to GCS
        _ = (
            parsed
            | "FilterStatic" >> beam.Filter(lambda x: x["type"] == "static")
            | "WindowStatic" >> beam.WindowInto(
                  window.FixedWindows(600),  # 10-minute window
                  trigger=AfterWatermark(),
                  accumulation_mode=AccumulationMode.DISCARDING)
            | "CombineStatic" >> beam.CombineGlobally(beam.combiners.ToListCombineFn()).without_defaults()
            | "UploadStaticSnapshot" >> beam.ParDo(UploadSnapshotFn(GCS_BUCKET))
        )

        # 2) POSITIONS pipeline: 60-second window for batching position records
        positions = (
            parsed
            | "FilterPositions" >> beam.Filter(lambda x: x["type"] == "position")
            | "WindowPositions" >> beam.WindowInto(
                  window.FixedWindows(60),  # 60-second window
                  trigger=AfterWatermark(),
                  accumulation_mode=AccumulationMode.DISCARDING)
            | "EnrichPositions" >> beam.ParDo(EnrichPositionFn(GCS_BUCKET))
        )

        # Write positions to BigQuery using FILE_LOADS with triggering frequency set to 60 seconds.
        _ = (
            positions
            | "WriteToBQ" >> WriteToBigQuery(
                  table=table_positions,
                  schema=positions_schema,
                  write_disposition=BigQueryDisposition.WRITE_APPEND,
                  create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                  method="FILE_LOADS",
                  triggering_frequency=60  # Batch load every 60 seconds
            )
        )

if __name__ == "__main__":
    run()