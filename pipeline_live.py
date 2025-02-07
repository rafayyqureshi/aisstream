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

# Ładowanie zmiennych środowiskowych i konfiguracja logowania
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GCS_BUCKET = os.getenv("GCS_BUCKET")
if not GCS_BUCKET:
    raise ValueError("Missing GCS_BUCKET environment variable.")

###############################################################################
# Klasy do zarządzania danymi statycznymi (StaticDataManager) oraz ich migawką
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
                logger.info(f"[StaticDataManager] Loaded {len(self.static_data)} static records from GCS.")
            else:
                logger.info("[StaticDataManager] No ships_static_latest.json found (starting empty).")
        except Exception as e:
            logger.warning(f"[StaticDataManager] Could not load initial data: {e}")

    def update_static(self, record: dict):
        """Aktualizuje dane statyczne; zapisuje rekord pod kluczem mmsi."""
        mmsi = record["mmsi"]
        self.static_data[mmsi] = record
        return record

    def get_snapshot(self):
        """Zwraca migawkę danych jako JSON."""
        return json.dumps(list(self.static_data.values()), indent=2)

    def find_ship_name(self, mmsi: int):
        return self.static_data.get(mmsi, {}).get("ship_name")

###############################################################################
# DoFn zapisujący migawkę danych statycznych do Cloud Storage
###############################################################################
class UploadSnapshotFn(beam.DoFn):
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
            logger.info(f"[UploadSnapshotFn] Snapshot updated with {len(self.manager.static_data)} static records.")
        except Exception as e:
            logger.error(f"[UploadSnapshotFn] Error during snapshot upload: {e}")

###############################################################################
# Funkcja parse_ais – parsuje wiadomość i zwraca listę słowników typu "static" oraz "position"
###############################################################################
def parse_ais(record_bytes: bytes):
    try:
        raw = json.loads(record_bytes.decode("utf-8"))
    except Exception:
        return []

    outputs = []

    # Sprawdzamy, czy wiadomość zawiera dane statyczne
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

    # Sprawdzamy dane pozycyjne
    pos_fields = ["mmsi", "latitude", "longitude", "cog", "sog", "timestamp"]
    if all(field in raw and raw[field] is not None for field in pos_fields):
        # Odrzucanie nieprawidłowych timestampów
        try:
            dt = datetime.datetime.fromisoformat(str(raw["timestamp"]).replace("Z", "+00:00"))
        except:
            return []  # Odrzucamy dany rekord całkowicie

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
# DoFn wzbogacająca dane pozycyjne – uzupełnia ship_name z danych statycznych, jeśli "Unknown"
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
        yield element

###############################################################################
# Główny potok
###############################################################################
def run():
    # Pobieranie zmiennych środowiskowych
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "ais-collision-detection")
    input_sub = os.getenv("INPUT_SUBSCRIPTION", "projects/ais-collision-detection/subscriptions/ais-data-sub")
    region = os.getenv("REGION", "us-east1")
    temp_loc = os.getenv("TEMP_LOCATION", "gs://ais-collision-detection-bucket/temp")
    staging_loc = os.getenv("STAGING_LOCATION", "gs://ais-collision-detection-bucket/staging")
    job_name = os.getenv("JOB_NAME", "ais-collision-detection-job")
    table_positions = f"{project_id}.ais_dataset_us.ships_positions"

    # Schemat
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

        # 1) Parsowanie unified
        parsed = lines | "ParseAISUnified" >> beam.FlatMap(parse_ais)

       # 2) Przetwarzanie danych statycznych
        static_data = (
            parsed
        | "FilterStatic" >> beam.Filter(lambda x: x["type"] == "static")
        | "WindowStatic" >> beam.WindowInto(
            window.FixedWindows(600),  # okno 10 minut
            trigger=AfterWatermark(),
            accumulation_mode=AccumulationMode.DISCARDING
        )
        # Deduplikacja w oknie: klucz -> x["mmsi"]
        | "AddStaticKeys" >> beam.WithKeys(lambda x: x["mmsi"])  # Dodaj klucz dla deduplikacji
        | "DeduplicateStatic" >> beam.Distinct()  # Deduplikacja po kluczu
        | "RemoveStaticKeys" >> beam.Values()  # Usuń klucz i przywróć oryginalną strukturę
        #   Zbieranie do listy w ramach okna, potem migawka
        | "CombineStatic" >> beam.CombineGlobally(beam.combiners.ToListCombineFn()).without_defaults()
        | "UploadStaticSnapshot" >> beam.ParDo(UploadSnapshotFn(GCS_BUCKET))
    )

        # 3) Przetwarzanie danych pozycyjnych
        positions = (
            parsed
        | "FilterPositions" >> beam.Filter(lambda x: x["type"] == "position")
        | "WindowPositions" >> beam.WindowInto(
            window.FixedWindows(10),  # okno 10 sekund
            trigger=AfterWatermark(),
            accumulation_mode=AccumulationMode.DISCARDING
        )
    # Deduplikacja (mmsi, timestamp), by uniknąć wielokrotnych identycznych wpisów
    | "AddPositionKeys" >> beam.WithKeys(lambda x: (x["mmsi"], x["timestamp"]))  # Dodaj klucz dla deduplikacji
    | "DeduplicatePositions" >> beam.Distinct()  # Deduplikacja po kluczu
    | "RemovePositionKeys" >> beam.Values()  # Usuń klucz i przywróć oryginalną strukturę
    | "EnrichPositions" >> beam.ParDo(EnrichPositionFn(GCS_BUCKET))
)

        # 4) Zapis do BQ z FILE_LOADS i trigger co 10 s
        _ = (
            positions
            | "WriteToBQ" >> WriteToBigQuery(
                table=table_positions,
                schema=positions_schema,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                method="FILE_LOADS",
                triggering_frequency=10  # co 10 sekund spływ danych do BQ
            )
        )

if __name__ == "__main__":
    run()