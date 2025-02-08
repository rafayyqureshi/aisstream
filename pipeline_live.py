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
            logger.info("[StaticDataManager] Starting with empty static data (no file found or read error).")

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
            logger.info(
                f"[UploadSnapshotFn] File ships_static_latest.json updated in GCS "
                f"with {len(self.manager.static_data)} ships."
            )
        except Exception as e:
            logger.error(f"[UploadSnapshotFn] Error during snapshot upload: {e}")

###############################################################################
# Funkcja parse_ais – parsuje wiadomość i zwraca listę słowników typu "static" oraz "position"
###############################################################################
def parse_ais(record_bytes: bytes):
    try:
        raw = json.loads(record_bytes.decode("utf-8"))
    except Exception as e:
        logger.warning(f"[parse_ais] Could not parse JSON: {e}, skipping record.")
        return []

    outputs = []

    # Próbujemy zbudować rekord typu "static" tylko jeśli dim_* nie są None:
    static_fields = ["mmsi", "ship_name", "dim_a", "dim_b", "dim_c", "dim_d"]
    if all(field in raw and raw[field] is not None for field in static_fields):
        if str(raw["ship_name"]).strip().lower() != "unknown":
            try:
                # Rzutowania do float
                dim_a = float(raw["dim_a"])
                dim_b = float(raw["dim_b"])
                dim_c = float(raw["dim_c"])
                dim_d = float(raw["dim_d"])
                mmsi_static = int(raw["mmsi"])
            except Exception as e:
                logger.warning(f"[parse_ais] Could not parse static dims: {e}, skipping static record.")
            else:
                outputs.append({
                    "type": "static",
                    "mmsi": mmsi_static,
                    "ship_name": str(raw["ship_name"]).strip(),
                    "dim_a": dim_a,
                    "dim_b": dim_b,
                    "dim_c": dim_c,
                    "dim_d": dim_d,
                })

    # Budujemy rekord pozycyjny, jeżeli wszystkie niezbędne pola występują i nie są None:
    pos_fields = ["mmsi", "latitude", "longitude", "cog", "sog", "timestamp"]
    if all(field in raw and raw[field] is not None for field in pos_fields):
        try:
            dt = datetime.datetime.fromisoformat(str(raw["timestamp"]).replace("Z", "+00:00"))
            # Opcjonalny check rozsądnego zakresu lat, np. 2000 - 2100:
            if dt.year < 2000 or dt.year > 2100:
                logger.warning(f"[parse_ais] Out-of-range timestamp ({dt}) - record dropped.")
                return []
        except Exception:
            logger.warning("[parse_ais] Invalid timestamp found - record dropped.")
            return []

        # Rzutowania
        try:
            mmsi_pos = int(raw["mmsi"])
            lat = float(raw["latitude"])
            lon = float(raw["longitude"])
            cog = float(raw["cog"])
            sog = float(raw["sog"])
        except Exception as e:
            logger.warning(f"[parse_ais] Could not parse position fields: {e}, skipping record.")
            return []

        heading = None
        if "heading" in raw and raw["heading"] is not None:
            try:
                heading = float(raw["heading"])
            except Exception:
                heading = None

        ship_name = str(raw.get("ship_name", "Unknown")).strip()

        outputs.append({
            "type": "position",
            "mmsi": mmsi_pos,
            "latitude": lat,
            "longitude": lon,
            "cog": cog,
            "sog": sog,
            "heading": heading,
            "timestamp": dt.isoformat() + "Z",
            "ship_name": ship_name
        })

    return outputs

###############################################################################
# DoFn wzbogacająca dane pozycyjne – uzupełnia ship_name z danych statycznych
###############################################################################
class EnrichPositionFn(beam.DoFn):
    def __init__(self, gcs_bucket: str):
        self.gcs_bucket = gcs_bucket

    def setup(self):
        self.manager = StaticDataManager(self.gcs_bucket)

    def process(self, element):
        if element.get("ship_name", "Unknown").lower() == "unknown":
            known = self.manager.find_ship_name(element["mmsi"])
            if known:
                element["ship_name"] = known
                logger.info(
                    f"[EnrichPositionFn] Found unknown ship name for MMSI {element['mmsi']} "
                    f"- enriched with '{known}'."
                )
        yield element

###############################################################################
# Funkcja pomocnicza – usuwanie pól niewymienionych w schemacie BQ
###############################################################################
def filter_fields_for_bq(record):
    """
    Zwraca nowy słownik zawierający tylko pola zdefiniowane w schemacie BQ.
    Usunięcie pola 'type' zapobiega błędom wczytywania (nie ma go w schemacie).
    """
    return {
        "mmsi": record["mmsi"],
        "ship_name": record["ship_name"],
        "latitude": record["latitude"],
        "longitude": record["longitude"],
        "cog": record["cog"],
        "sog": record["sog"],
        "heading": record["heading"],
        "timestamp": record["timestamp"],
    }

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
        # 1) Odczyt z PubSub
        lines = p | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=input_sub)

        # 2) Parsowanie wiadomości AIS
        parsed = lines | "ParseAISUnified" >> beam.FlatMap(parse_ais)

        # 3) Przetwarzanie danych statycznych (tylko jeśli wszystkie wymiary != None)
        static_data = (
            parsed
            | "FilterStatic" >> beam.Filter(lambda x: x["type"] == "static")
            | "WindowStatic" >> beam.WindowInto(
                window.FixedWindows(600),  # okno 10 minut
                trigger=AfterWatermark(),
                accumulation_mode=AccumulationMode.DISCARDING
            )
            # Deduplikacja w oknie: klucz -> x["mmsi"]
            | "AddStaticKeys" >> beam.WithKeys(lambda x: x["mmsi"])
            | "DeduplicateStatic" >> beam.Distinct()
            | "RemoveStaticKeys" >> beam.Values()
            # Zbieranie do listy w ramach okna, potem migawka
            | "CombineStatic" >> beam.CombineGlobally(beam.combiners.ToListCombineFn()).without_defaults()
            | "UploadStaticSnapshot" >> beam.ParDo(UploadSnapshotFn(GCS_BUCKET))
        )

        # 4) Przetwarzanie danych pozycyjnych
        positions = (
            parsed
            | "FilterPositions" >> beam.Filter(lambda x: x["type"] == "position")
            | "WindowPositions" >> beam.WindowInto(
                window.FixedWindows(10),  # okno 10 sekund
                trigger=AfterWatermark(),
                accumulation_mode=AccumulationMode.DISCARDING
            )
            # Deduplikacja (mmsi, timestamp), by uniknąć wielokrotnych identycznych wpisów
            | "AddPositionKeys" >> beam.WithKeys(lambda x: (x["mmsi"], x["timestamp"]))
            | "DeduplicatePositions" >> beam.Distinct()
            | "RemovePositionKeys" >> beam.Values()
            | "EnrichPositions" >> beam.ParDo(EnrichPositionFn(GCS_BUCKET))
            # Usunięcie pola 'type'
            | "FilterFieldsForBQ" >> beam.Map(filter_fields_for_bq)
        )

        # 5) Zapis do BQ z metodą FILE_LOADS i wyzwalaniem co 10 s
        _ = (
            positions
            | "WriteToBQ" >> WriteToBigQuery(
                table=table_positions,
                schema=positions_schema,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                method="FILE_LOADS",
                triggering_frequency=10
                # Możesz dodać, jeśli chcesz pominąć sporadycznie błędne rekordy:
                # additional_bq_parameters={"maxBadRecords": 5, "ignoreUnknownValues": True}
            )
        )

if __name__ == "__main__":
    run()