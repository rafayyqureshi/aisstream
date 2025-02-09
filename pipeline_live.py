#!/usr/bin/env python3
import os
import json
import logging
import datetime
from datetime import timedelta
from dotenv import load_dotenv

from google.cloud import storage
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import window
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

##########################################################################
# 1. Klasa do zarządzania danymi statycznymi w pamięci + GCS (snapshot)
##########################################################################
class StaticDataManager:
    """
    Trzyma w pamięci dane statyczne w słowniku self.static_data {mmsi -> {...}}.
    Przy starcie wczytuje 'ships_static_latest.json' z GCS (o ile istnieje).
    Co pewien czas potok wywołuje 'update_static_data' i 'get_snapshot'
    w celu zapisania do GCS.
    """
    def __init__(self, gcs_bucket):
        self.static_data = {}
        self.gcs_bucket = gcs_bucket
        self._load_initial_data()

    def _load_initial_data(self):
        """Wczytaj ostatni stan z GCS przy inicjalizacji."""
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(self.gcs_bucket)
            blob = bucket.blob("ships_static_latest.json")

            if blob.exists():
                json_data = blob.download_as_text()
                data_list = json.loads(json_data)
                for item in data_list:
                    mmsi_ = item["mmsi"]
                    self.static_data[mmsi_] = item
                logger.info(f"[StaticDataManager] Loaded {len(self.static_data)} ships from GCS.")
        except Exception as e:
            logger.error(f"[StaticDataManager] Error loading initial data: {str(e)}")

    def update_static_data(self, element):
        """
        Aktualizuje dane statyczne w pamięci na podstawie elementu,
        który zawiera (mmsi, ship_name, dim_a, dim_b, dim_c, dim_d).
        """
        mmsi = element["mmsi"]
        self.static_data[mmsi] = {
            "mmsi": mmsi,
            "ship_name": element["ship_name"],
            "dim_a": element["dim_a"],
            "dim_b": element["dim_b"],
            "dim_c": element["dim_c"],
            "dim_d": element["dim_d"],
        }
        return element

    def get_snapshot(self):
        """Generuj JSON z aktualnymi danymi statycznymi."""
        # Konwersja values() do listy. Każdy obiekt to np. {mmsi, ship_name, ...}
        return json.dumps(list(self.static_data.values()), indent=2)

    def find_ship_name(self, mmsi):
        """
        Zwraca nazwe statku z pamięci, jeśli istnieje w self.static_data,
        w przeciwnym razie None.
        """
        entry = self.static_data.get(mmsi)
        if entry and "ship_name" in entry:
            return entry["ship_name"]
        return None

##########################################################################
# 2. DoFn do zapisywania snapshotu do GCS (co X minut)
##########################################################################
class UploadSnapshotFn(beam.DoFn):
    def __init__(self, gcs_bucket):
        self.gcs_bucket = gcs_bucket
        self.data_manager = None

    def setup(self):
        # Tworzymy / wczytujemy DataManager
        self.data_manager = StaticDataManager(self.gcs_bucket)

    def process(self, elements):
        """
        Przetwarzamy batch (np. z okna), aktualizujemy self.data_manager
        i zapisujemy migawkę do GCS.
        """
        if not elements:
            return

        try:
            # Aktualizuj dane w pamięci
            for element in elements:
                self.data_manager.update_static_data(element)

            # Po zaktualizowaniu – zapis do GCS
            json_data = self.data_manager.get_snapshot()
            storage_client = storage.Client()
            bucket = storage_client.bucket(self.gcs_bucket)
            blob = bucket.blob("ships_static_latest.json")
            blob.upload_from_string(json_data, content_type="application/json")

            logger.info(f"[UploadSnapshotFn] Snapshot updated, total ships in memory: "
                        f"{len(self.data_manager.static_data)}")
        except Exception as e:
            logger.error(f"[UploadSnapshotFn] Upload failed: {str(e)}")

    def teardown(self):
        """Opcjonalnie – np. zapisać finalną wersję przy zakończeniu potoku."""
        pass

##########################################################################
# 3. Funkcja parse_ais do odbioru DANYCH STATYCZNYCH (mmsi, dims)
##########################################################################
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

##########################################################################
# 4. Funkcja parse_positions do odbioru DANYCH POZYCYJNYCH (mmsi, lat, lon, sog, cog, hdg, timestamp)
#    + ewentualne uzupełnianie nazwy statku, jeśli jest Unknown
##########################################################################
class EnrichPositionFn(beam.DoFn):
    """
    DoFn do wzbogacania rekordów pozycyjnych o ewentualną nazwę statku z pamięci (static_data).
    - setup() ładuje DataManager
    - process() sprawdza, czy ship_name == "Unknown"; jeśli tak, próbuje odczytać z memory.
    """
    def __init__(self, gcs_bucket):
        super().__init__()
        self.gcs_bucket = gcs_bucket
        self.data_manager = None

    def setup(self):
        # Załaduj static data manager (z pliku w GCS)
        self.data_manager = StaticDataManager(self.gcs_bucket)

    def process(self, element):
        """
        element: {'mmsi', 'latitude', 'longitude', 'cog', 'sog', 'heading', 'timestamp', 'ship_name'}
        """
        # Jeśli nazwa to "Unknown", spróbujmy pobrać z memory
        if not element.get("ship_name") or element["ship_name"].strip().lower() == "unknown":
            name_from_mem = self.data_manager.find_ship_name(element["mmsi"])
            if name_from_mem:
                element["ship_name"] = name_from_mem

        yield element

def parse_positions(record_bytes):
    """
    Odczytuje wiadomości z Pub/Sub dotyczące pozycji statku.
    Wymagane pola: mmsi, latitude, longitude, cog, sog, heading, timestamp.
    ship_name może być (lub nie być) => wypełnimy w EnrichPositionFn, jeśli Unknown.
    """
    try:
        data = json.loads(record_bytes.decode("utf-8"))

        req = ["mmsi", "latitude", "longitude", "cog", "sog", "timestamp"]
        if not all(r in data for r in req):
            return None

        # Wypełnienie domyślnych wartości:
        mmsi_ = int(data["mmsi"])
        lat_ = float(data["latitude"])
        lon_ = float(data["longitude"])
        cog_ = float(data["cog"])
        sog_ = float(data["sog"])
        # heading może być None / float
        hdg = data.get("heading")
        if hdg is not None:
            try:
                hdg = float(hdg)
            except:
                hdg = None

        # Nazwa statku – może być unknown lub brak
        ship_name_ = data.get("ship_name", "Unknown")

        # Tworzymy obiekt do dalszego przetwarzania
        return {
            "mmsi": mmsi_,
            "latitude": lat_,
            "longitude": lon_,
            "cog": cog_,
            "sog": sog_,
            "heading": hdg,
            "timestamp": data["timestamp"],  # Zakładamy, że jest poprawnym stringiem
            "ship_name": ship_name_
        }

    except Exception as e:
        logger.error(f"[parse_positions] Parse error: {e}")
        return None

##########################################################################
# 5. Główny run() – pipeline, w którym:
#    a) zbieramy dane statyczne => JSON w GCS (co 10min)
#    b) przetwarzamy dane pozycyjne => BQ
##########################################################################
def run():
    from apache_beam.io.gcp.bigquery import BigQueryDisposition
    from apache_beam.utils.timestamp import Duration

    # Załadowanie ENV
    project_id  = os.getenv("GOOGLE_CLOUD_PROJECT", "ais-collision-detection")
    input_sub   = os.getenv("INPUT_SUBSCRIPTION", "projects/ais-collision-detection/subscriptions/ais-data-sub")
    region      = os.getenv("REGION", "us-east1")
    temp_loc    = os.getenv("TEMP_LOCATION", "gs://ais-collision-detection-bucket/temp")
    staging_loc = os.getenv("STAGING_LOCATION", "gs://ais-collision-detection-bucket/staging")
    job_name    = os.getenv("JOB_NAME", "ais-collision-detection-job")
    gcs_bucket  = os.getenv("GCS_BUCKET", "ais-collision-detection-bucket")

    # Tabela do zapisu pozycji
    table_positions = f"{project_id}.ais_dataset_us.ships_positions"

    # Konfiguracja partycjonowania i klastrowania (retencja 1 godz.)
    positions_schema = {
        "fields": [
            {"name": "mmsi",       "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "ship_name",  "type": "STRING",  "mode": "NULLABLE"},
            {"name": "latitude",   "type": "FLOAT",   "mode": "REQUIRED"},
            {"name": "longitude",  "type": "FLOAT",   "mode": "REQUIRED"},
            {"name": "cog",        "type": "FLOAT",   "mode": "REQUIRED"},
            {"name": "sog",        "type": "FLOAT",   "mode": "REQUIRED"},
            {"name": "heading",    "type": "FLOAT",   "mode": "NULLABLE"},
            {"name": "timestamp",  "type": "TIMESTAMP","mode": "REQUIRED"}
        ]
    }

    pipeline_options = PipelineOptions(
        runner='DataflowRunner',
        project=project_id,
        region=region,
        temp_location=temp_loc,
        staging_location=staging_loc,
        job_name=job_name,
        streaming=True,  # Zakładamy stream
        save_main_session=True
    )

    with beam.Pipeline(options=pipeline_options) as p:
        # Odczyt z PubSub – wspólne
        lines = p | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=input_sub)

        ###################################################################
        # A) Obsługa danych statycznych: parse_ais_for_static
        #    -> okno 10min -> snapshot do GCS
        ###################################################################
        static_data = (
            lines
            | "ParseForStatic" >> beam.Map(parse_ais_for_static)
            | "FilterStaticValid" >> beam.Filter(lambda x: x is not None)
            | "WinStatic" >> beam.WindowInto(window.FixedWindows(600))  # co 10min
            | "GroupToListStatic" >> beam.CombineGlobally(beam.combiners.ToListCombineFn()).without_defaults()
            | "UploadSnapshot" >> beam.ParDo(UploadSnapshotFn(gcs_bucket))
        )

        ###################################################################
        # B) Obsługa danych pozycyjnych: parse_positions
        #    -> wzbogacanie nazwy z memory, zapisy do BQ (1h retencja)
        ###################################################################
        positions = (
            lines
            | "ParsePositions" >> beam.Map(parse_positions)
            | "FilterPosValid" >> beam.Filter(lambda x: x is not None)
            # Okno np. 30s – można ustawić dowolne; tu np. 15s
            | "WinPositions" >> beam.WindowInto(window.FixedWindows(15))
            # Wzbogacamy dane, jeśli ship_name = 'Unknown'
            | "EnrichPositions" >> beam.ParDo(EnrichPositionFn(gcs_bucket))
        )

        # Zapis do BigQuery: retencja 1h => w definicji tabeli
        # (partitioning, clustering)
        # Ustawiamy parametry: expiration_ms=3600000 (60*60*1000=1h),
        # partition po timestamp, cluster = [mmsi].
        positions | "WritePositions" >> WriteToBigQuery(
            table=table_positions,
            schema=positions_schema,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            method="STREAMING_INSERTS",
            additional_bq_parameters={
                "timePartitioning": {
                    "type": "DAY",
                    "field": "timestamp",
                    "expirationMs": 3600000  # 1h
                },
                "clustering": {
                    "fields": ["mmsi"]
                }
            }
        )

if __name__ == "__main__":
    run()