#!/usr/bin/env python3
import os
import json
import logging
import datetime
import time
import tempfile
from dotenv import load_dotenv

from google.cloud import storage
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import window
from apache_beam.transforms.trigger import AfterWatermark, AccumulationMode
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.transforms.util import WithKeys
from apache_beam import pvalue

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
# DoFn zapisujący migawkę danych statycznych do Cloud Storage z prostym retry
###############################################################################
class UploadSnapshotFn(beam.DoFn):
    def __init__(self, gcs_bucket: str, max_retries=3, retry_delay=2):
        self.gcs_bucket = gcs_bucket
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def setup(self):
        self.manager = StaticDataManager(self.gcs_bucket)

    def _safe_upload(self, data):
        """Próbuje wysłać plik do GCS z prostą logiką retry."""
        for attempt in range(1, self.max_retries + 1):
            try:
                storage_client = storage.Client()
                bucket = storage_client.bucket(self.gcs_bucket)
                blob = bucket.blob("ships_static_latest.json")
                blob.upload_from_string(data, content_type="application/json")
                logger.info(
                    f"[UploadSnapshotFn] File ships_static_latest.json updated in GCS "
                    f"with {len(self.manager.static_data)} ships."
                )
                return True
            except Exception as e:
                logger.error(f"[UploadSnapshotFn] GCS upload attempt {attempt} failed: {e}")
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay)
        return False

    def process(self, batch):
        if not batch:
            return
        try:
            for r in batch:
                self.manager.update_static(r)
            json_data = self.manager.get_snapshot()

            success = self._safe_upload(json_data)
            if not success:
                # Jeśli nie uda się wysłać do GCS, zapisujemy do pliku tymczasowego lokalnie (log)
                with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmpfile:
                    tmpfile.write(json_data.encode("utf-8"))
                logger.error("[UploadSnapshotFn] All retries failed. Snapshot written to temp file.")
        except Exception as e:
            logger.error(f"[UploadSnapshotFn] Unexpected error: {e}")

###############################################################################
# Funkcja parse_ais – parsuje wiadomość i zwraca rekordy główne (static/position)
# przez yield, a błędy przez yield pvalue.TaggedOutput('error', ...)
###############################################################################
def parse_ais(record_bytes):
    try:
        raw_str = record_bytes.decode("utf-8")
        raw = json.loads(raw_str)
    except Exception as e:
        logger.warning(f"[parse_ais] Could not parse JSON: {e}, skipping record.")
        yield pvalue.TaggedOutput('error', {"error": str(e), "raw": raw_str})
        return

    outputs = []

    # --- DANE STATYCZNE ---
    static_fields = ["mmsi", "ship_name", "dim_a", "dim_b", "dim_c", "dim_d"]
    if all(field in raw and raw[field] is not None for field in static_fields):
        if str(raw["ship_name"]).strip().lower() != "unknown":
            try:
                dim_a = float(raw["dim_a"])
                dim_b = float(raw["dim_b"])
                dim_c = float(raw["dim_c"])
                dim_d = float(raw["dim_d"])
                mmsi_static = int(raw["mmsi"])
                outputs.append({
                    "type": "static",
                    "mmsi": mmsi_static,
                    "ship_name": str(raw["ship_name"]).strip(),
                    "dim_a": dim_a,
                    "dim_b": dim_b,
                    "dim_c": dim_c,
                    "dim_d": dim_d,
                })
            except Exception as e:
                logger.warning(f"[parse_ais] Could not parse static dims: {e}, skipping static record.")
                yield pvalue.TaggedOutput('error', {"error": str(e), "raw": raw})
                return

    # --- DANE POZYCYJNE ---
    pos_fields = ["mmsi", "latitude", "longitude", "cog", "sog", "timestamp"]
    if all(field in raw and raw[field] is not None for field in pos_fields):
        try:
            dt = datetime.datetime.fromisoformat(str(raw["timestamp"]).replace("Z", "+00:00"))
            if dt.year < 2000 or dt.year > 2100:
                logger.warning(f"[parse_ais] Out-of-range timestamp ({dt}) - record dropped.")
                yield pvalue.TaggedOutput('error', {"error": "Out-of-range timestamp", "raw": raw})
                return
        except Exception as e:
            logger.warning(f"[parse_ais] Invalid timestamp found - record dropped. {e}")
            yield pvalue.TaggedOutput('error', {"error": str(e), "raw": raw})
            return

        try:
            mmsi_pos = int(raw["mmsi"])
            lat = float(raw["latitude"])
            lon = float(raw["longitude"])
            cog = float(raw["cog"])
            sog = float(raw["sog"])
            heading = None
            if "heading" in raw and raw["heading"] is not None:
                heading = float(raw["heading"])
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
        except Exception as e:
            logger.warning(f"[parse_ais] Could not parse position fields: {e}, skipping record.")
            yield pvalue.TaggedOutput('error', {"error": str(e), "raw": raw})
            return

    # Jeśli nie ma nic w outputs, to też błąd
    if not outputs:
        yield pvalue.TaggedOutput('error', {"error": "No valid fields found", "raw": raw})
        return

    # Emitujemy poprawne rekordy do głównego wyjścia
    for out in outputs:
        yield out

###############################################################################
# DoFn wzbogacająca dane pozycyjne – uzupełnia ship_name z danych statycznych
# + okresowe odświeżanie cache co 5 minut
###############################################################################
class EnrichPositionFn(beam.DoFn):
    def __init__(self, gcs_bucket: str):
        self.gcs_bucket = gcs_bucket
        self.last_refresh = datetime.datetime.min

    def setup(self):
        self.manager = StaticDataManager(self.gcs_bucket)

    def process(self, element):
        # Odśwież cache co 5 minut
        now = datetime.datetime.utcnow()
        if (now - self.last_refresh).total_seconds() > 300:
            logger.info("[EnrichPositionFn] Refreshing static cache from GCS...")
            self.manager._load_initial_data()
            self.last_refresh = now

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
# Funkcja pomocnicza – usuwanie niepotrzebnych pól i weryfikacja required
###############################################################################
def filter_fields_for_bq(record):
    """Zwraca nowy słownik z polami pasującymi do schematu i sprawdza required."""
    required_fields = ["mmsi", "latitude", "longitude", "cog", "sog", "timestamp"]
    for rf in required_fields:
        if record.get(rf) is None:
            logger.warning(f"[filter_fields_for_bq] Missing or None in required field '{rf}', skipping record.")
            return None

    return {
        "mmsi": record["mmsi"],
        "ship_name": record.get("ship_name", None),
        "latitude": record["latitude"],
        "longitude": record["longitude"],
        "cog": record["cog"],
        "sog": record["sog"],
        "heading": record.get("heading", None),
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
            {"name": "mmsi", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "ship_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "latitude", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "longitude", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "cog", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "sog", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "heading", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"}
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
        # Odczyt z PubSub
        lines = p | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=input_sub)

        # Parse z dead-letter queue
        parsed_with_errors = (
            lines
            | "ParseAISUnified" >> beam.FlatMap(parse_ais).with_outputs("error", main="main")
        )
        parsed = parsed_with_errors["main"]
        errors = parsed_with_errors["error"]

        # 1) Dead-letter queue – zapis błędnych rekordów do GCS
        _ = (
            errors
            | "ErrorsToJSON" >> beam.Map(json.dumps)
            | "WriteErrorsToGCS" >> beam.io.WriteToText(
                file_path_prefix="gs://ais-collision-detection-bucket/errors/error",
                file_name_suffix=".json"
            )
        )

        # 2) Przetwarzanie danych statycznych
        static_data = (
            parsed
            | "FilterStatic" >> beam.Filter(lambda x: x.get("type") == "static")
            | "WindowStatic" >> beam.WindowInto(
                window.FixedWindows(600),  # okno 10 minut
                trigger=AfterWatermark(),
                accumulation_mode=AccumulationMode.DISCARDING
            )
            | "KeyByMMSIStatic" >> beam.WithKeys(lambda x: x["mmsi"])
            | "DistinctStatic" >> beam.Distinct()
            | "DropKeyStatic" >> beam.Values()
            | "GroupStatic" >> beam.CombineGlobally(beam.combiners.ToListCombineFn()).without_defaults()
            | "UploadStaticSnapshot" >> beam.ParDo(UploadSnapshotFn(GCS_BUCKET))
        )

        # 3) Przetwarzanie danych pozycyjnych
        positions = (
            parsed
            | "FilterPositions" >> beam.Filter(lambda x: x.get("type") == "position")
            | "WindowPositions" >> beam.WindowInto(
                window.FixedWindows(10),  # okno 10 sekund
                trigger=AfterWatermark(),
                accumulation_mode=AccumulationMode.DISCARDING
            )
            | "KeyByMMSITime" >> beam.WithKeys(lambda x: (x["mmsi"], x["timestamp"]))
            | "DistinctPos" >> beam.Distinct()
            | "DropKeyPos" >> beam.Values()
            | "EnrichPositions" >> beam.ParDo(EnrichPositionFn(GCS_BUCKET))
            | "FilterFieldsForBQ" >> beam.Map(filter_fields_for_bq)
            | "DropInvalidRecords" >> beam.Filter(lambda x: x is not None)
        )

        # 4) Zapis do BigQuery z obsługą błędów
        _ = (
            positions
            | "WriteToBQ" >> WriteToBigQuery(
                table=table_positions,
                schema=positions_schema,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                method="FILE_LOADS",
                triggering_frequency=10,
                additional_bq_parameters={
                    "maxBadRecords": 5,
                    "ignoreUnknownValues": True
                }
            )
        )

if __name__ == "__main__":
    run()