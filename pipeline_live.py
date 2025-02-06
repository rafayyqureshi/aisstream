#!/usr/bin/env python3
import os
import json
import logging
import datetime
import time
from dotenv import load_dotenv
from google.cloud import storage
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import window

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# In-memory dictionary to store static ship data (MMSI as key)
static_data = {}

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
        data.pop("ship_length", None)
        hdg = data.get("heading")
        if hdg is not None:
            try:
                data["heading"] = float(hdg)
            except:
                data["heading"] = None
        else:
            data["heading"] = None
        for d in ["dim_a", "dim_b", "dim_c", "dim_d"]:
            v = data.get(d)
            data[d] = float(v) if v is not None else None
        data["ship_name"] = data.get("ship_name", "Unknown")
        data["geohash"] = data.get("geohash", "none")
        return data
    except Exception as e:
        logging.error(f"Error in parse_ais: {e}")
        return None

def process_static_data(row):
    """Process static data from Pub/Sub."""
    mmsi = row["mmsi"]
    if mmsi not in static_data:
        static_data[mmsi] = {
            "ship_name": row["ship_name"],
            "dim_a": row["dim_a"],
            "dim_b": row["dim_b"],
            "dim_c": row["dim_c"],
            "dim_d": row["dim_d"]
        }
        logging.info(f"New static ship data added: {mmsi}")
    return row

def create_json_snapshot():
    """Create a snapshot of all static data and convert it to JSON."""
    snapshot = []
    for mmsi, info in static_data.items():
        obj = {"mmsi": mmsi}
        obj.update(info)
        snapshot.append(obj)
    json_str = json.dumps(snapshot, indent=2)
    return json_str

def upload_to_gcs(json_data, bucket_name, file_name):
    """Upload JSON data to Google Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(json_data, content_type="application/json")
    logging.info(f"File {file_name} uploaded to {bucket_name}.")

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
        num_workers=1,
        max_num_workers=10,
        autoscaling_algorithm='THROUGHPUT_BASED',
        save_main_session=True,
        streaming=True
    )

    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=input_sub)

        parsed = (
            lines
            | "ParseAIS" >> beam.Map(parse_ais)
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
        )

        # Process static data
        processed_static_data = parsed | "ProcessStaticData" >> beam.Map(process_static_data)

        # Timer for periodic snapshot creation and uploading to GCS (every 5 minutes for testing)
        def create_and_upload_snapshot(window, elements):
            json_data = create_json_snapshot()
            upload_to_gcs(json_data, gcs_bucket, file_name)

        # Apply the snapshot timer function every 5 minutes
        processed_static_data | "CreateSnapshot" >> beam.WindowInto(window.FixedWindows(300)) \
            | "UploadSnapshot" >> beam.ParDo(create_and_upload_snapshot)

if __name__ == "__main__":
    run()