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


###############################################################################
# Ustawienia logowania i zmiennych środowiskowych
###############################################################################
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GCS_BUCKET = os.getenv("GCS_BUCKET")
if not GCS_BUCKET:
    raise ValueError("Missing GCS_BUCKET environment variable.")


###############################################################################
# Klasy do zarządzania danymi statycznymi
###############################################################################
class StaticDataManager:
    def __init__(self, gcs_bucket: str):
        self.gcs_bucket = gcs_bucket
        self.static_data = {}
        self._load_initial_data()

    def _load_initial_data(self):
        """Wczytuje plik ships_static_latest.json z GCS (jeśli istnieje)."""
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(self.gcs_bucket)
            blob = bucket.blob("ships_static_latest.json")
            if blob.exists():
                content = blob.download_as_text()
                records = json.loads(content)
                for item in records:
                    self.static_data[item["mmsi"]] = item
                logger.info(f"[StaticDataManager] Loaded {len(self.static_data)} records from GCS.")
            else:
                logger.info("[StaticDataManager] No ships_static_latest.json found (starting empty).")
        except Exception as e:
            logger.warning(f"[StaticDataManager] Could not load initial data: {e}")

    def update_static(self, record: dict):
        """Aktualizuje dane statyczne (mmsi->nazwa, wymiary)."""
        mmsi = record["mmsi"]
        self.static_data[mmsi] = record  # np. {'mmsi':..., 'ship_name':..., 'dim_a':...}
        return record

    def get_snapshot(self):
        return json.dumps(list(self.static_data.values()), indent=2)

    def find_ship_name(self, mmsi: int):
        return self.static_data.get(mmsi, {}).get("ship_name")


class UploadSnapshotFn(beam.DoFn):
    """DoFn, który w oknie zbiera listę rekordów i dokonuje snapshotu do GCS."""
    def __init__(self, gcs_bucket: str):
        self.gcs_bucket = gcs_bucket

    def setup(self):
        self.manager = StaticDataManager(self.gcs_bucket)

    def process(self, batch):
        if not batch:
            return
        try:
            for r in batch:
                self.manager.update_static(r)
            json_data = self.manager.get_snapshot()
            storage_client = storage.Client()
            bucket = storage_client.bucket(self.gcs_bucket)
            blob = bucket.blob("ships_static_latest.json")
            blob.upload_from_string(json_data, content_type="application/json")
            logger.info(f"[UploadSnapshotFn] Wrote snapshot with {len(self.manager.static_data)} ships.")
        except Exception as e:
            logger.error(f"[UploadSnapshotFn] Error: {e}")


###############################################################################
# Funkcja parse_ais - obsługuje zarówno dane statyczne, jak i pozycyjne
###############################################################################
def parse_ais(record_bytes: bytes):
    """
    Rozpoznaje i parsuje JSON AIS — może zawierać dane statyczne (dim_a..d) i/lub pozycyjne (lat,lon,...).
    Zwraca listę 0..N słowników, gdzie każdy ma klucz 'type' = 'static'/'position'.
    """
    try:
        raw = json.loads(record_bytes.decode("utf-8"))
    except Exception:
        # Minimalne logi, by nie przepełniać
        return []

    outputs = []

    # 1) Czy to może być "static"?
    # Wymagane: mmsi, ship_name != 'Unknown', dim_a..d
    can_be_static = True
    static_fields = ["mmsi", "ship_name", "dim_a", "dim_b", "dim_c", "dim_d"]
    for fld in static_fields:
        if fld not in raw or raw[fld] is None:
            can_be_static = False
            break
    # Dodatkowo odrzucamy if ship_name='Unknown'
    if can_be_static and str(raw["ship_name"]).strip().lower() != "unknown":
        # Tworzymy słownik
        sdict = {
            "type": "static",
            "mmsi": int(raw["mmsi"]),
            "ship_name": str(raw["ship_name"]),
            "dim_a": float(raw["dim_a"]),
            "dim_b": float(raw["dim_b"]),
            "dim_c": float(raw["dim_c"]),
            "dim_d": float(raw["dim_d"]),
        }
        outputs.append(sdict)

    # 2) Czy to może być "position"?
    # Wymagane: mmsi, latitude, longitude, cog, sog, timestamp
    pos_fields = ["mmsi", "latitude", "longitude", "cog", "sog", "timestamp"]
    can_be_position = all(fld in raw and raw[fld] is not None for fld in pos_fields)
    if can_be_position:
        # Parsujemy heading, timestamp
        heading = None
        if "heading" in raw and raw["heading"] is not None:
            try:
                heading = float(raw["heading"])
            except:
                pass

        try:
            dt = datetime.datetime.fromisoformat(str(raw["timestamp"]).replace("Z", "+00:00"))
        except:
            dt = datetime.datetime.utcnow()

        # ship_name może być cokolwiek, w tym 'Unknown'
        pdict = {
            "type": "position",
            "mmsi": int(raw["mmsi"]),
            "latitude": float(raw["latitude"]),
            "longitude": float(raw["longitude"]),
            "cog": float(raw["cog"]),
            "sog": float(raw["sog"]),
            "heading": heading,
            "timestamp": dt.isoformat() + "Z",
            "ship_name": str(raw.get("ship_name", "Unknown"))
        }
        outputs.append(pdict)

    return outputs


###############################################################################
# Wzbogacanie pozycji, jeżeli ship_name = Unknown
###############################################################################
class EnrichPositionFn(beam.DoFn):
    def __init__(self, gcs_bucket: str):
        self.gcs_bucket = gcs_bucket

    def setup(self):
        self.manager = StaticDataManager(self.gcs_bucket)

    def process(self, element):
        # Zmieniamy 'Unknown' na to, co jest w managerze
        if element.get("ship_name", "Unknown").strip().lower() == "unknown":
            known = self.manager.find_ship_name(element["mmsi"])
            if known:
                element["ship_name"] = known
        yield element


###############################################################################
# Główny potok
###############################################################################
def run():
    # Zmienne środowiskowe
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "ais-collision-detection")
    input_sub = os.getenv("INPUT_SUBSCRIPTION", "projects/ais-collision-detection/subscriptions/ais-data-sub")
    region = os.getenv("REGION", "us-east1")
    temp_loc = os.getenv("TEMP_LOCATION", "gs://ais-collision-detection-bucket/temp")
    staging_loc = os.getenv("STAGING_LOCATION", "gs://ais-collision-detection-bucket/staging")
    job_name = os.getenv("JOB_NAME", "ais-collision-detection-job")

    # Tabela w BigQuery
    table_positions = f"{project_id}.ais_dataset_us.ships_positions"
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

        # Jedna funkcja parse_ais -> generuje 0..2 obiektów per rekord (static / position)
        parsed = (
            lines
            | "ParseAISUnified" >> beam.FlatMap(parse_ais)
        )

        # 1) STATIC pipeline
        static_data = (
            parsed
            | "FilterStatic" >> beam.Filter(lambda x: x["type"] == "static")
            | "WindowStatic" >> beam.WindowInto(
                window.FixedWindows(600),
                trigger=AfterWatermark(),
                accumulation_mode=AccumulationMode.DISCARDING
            )
            | "CombineStatic" >> beam.CombineGlobally(beam.combiners.ToListCombineFn()).without_defaults()
            | "UploadStaticSnapshot" >> beam.ParDo(UploadSnapshotFn(GCS_BUCKET))
        )

        # 2) POSITIONS pipeline
        # Okno 10s, batch do BQ
        positions = (
            parsed
            | "FilterPositions" >> beam.Filter(lambda x: x["type"] == "position")
            | "WindowPositions" >> beam.WindowInto(
                window.FixedWindows(10),
                trigger=AfterWatermark(),
                accumulation_mode=AccumulationMode.DISCARDING
            )
            | "CombinePositions" >> beam.CombineGlobally(beam.combiners.ToListCombineFn()).without_defaults()
            | "EnrichPositions" >> beam.ParDo(EnrichPositionFn(GCS_BUCKET))
        )

        # Zapis do BQ
        _ = (
            positions
            | "WriteToBQ" >> WriteToBigQuery(
                table=table_positions,
                schema=positions_schema,
                write_disposition=WriteToBigQuery.WRITE_APPEND,
                create_disposition=WriteToBigQuery.CREATE_IF_NEEDED,
                method="STREAMING_INSERTS"
            )

        )

if __name__ == "__main__":
    run()