#!/usr/bin/env python3
import os
import json
import math
import time
import logging
import datetime
from dotenv import load_dotenv

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from google.cloud import bigquery
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.transforms.userstate import BagStateSpec
import apache_beam.coders

from apache_beam import window

# Załaduj zmienne środowiskowe z .env
load_dotenv()

# Konfiguracja loggera
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

###############################################################################
# Parametry kolizji i state
###############################################################################
CPA_THRESHOLD        = 0.5   # mile morskie
TCPA_THRESHOLD       = 10.0  # minuty
STATE_RETENTION_SEC  = 120   # 2 min

###############################################################################
# 1) compute_cpa_tcpa – obliczanie CPA/TCPA
###############################################################################
def compute_cpa_tcpa(shipA, shipB):
    """
    Oblicza CPA (Closest Point of Approach) i TCPA (Time to CPA) w milach morskich i minutach.
    
    Parametry:
    - shipA: dict zawierający 'latitude', 'longitude', 'cog', 'sog'
    - shipB: dict zawierający 'latitude', 'longitude', 'cog', 'sog'
    
    Zwraca:
    - (cpa_nm, tcpa_min)
    """
    # Sprawdzenie wymaganych pól
    required = ['latitude', 'longitude', 'cog', 'sog']
    for ship in [shipA, shipB]:
        if not all(field in ship and ship[field] is not None for field in required):
            return (9999, -1)
    
    # Konwersja COG na radiany
    cogA_rad = math.radians(shipA['cog'])
    cogB_rad = math.radians(shipB['cog'])
    
    # Prędkości w kn (mile morskie na godzinę)
    sogA = shipA['sog']
    sogB = shipB['sog']
    
    # Prędkości w nm/min
    speedA = sogA / 60.0
    speedB = sogB / 60.0
    
    # Prędkości w x (east) i y (north)
    vxA = speedA * math.sin(cogA_rad)
    vyA = speedA * math.cos(cogA_rad)
    vxB = speedB * math.sin(cogB_rad)
    vyB = speedB * math.cos(cogB_rad)
    
    # Różnice pozycji
    dx = shipA['longitude'] - shipB['longitude']
    dy = shipA['latitude'] - shipB['latitude']
    
    # Różnice prędkości
    dvx = vxA - vxB
    dvy = vyA - vyB
    
    # Obliczenia CPA i TCPA
    dv2 = dvx**2 + dvy**2
    if dv2 == 0.0:
        return (9999, -1)
    
    pv  = dx*dvx + dy*dvy
    tcpa = -pv/dv2
    if tcpa < 0.0:
        return (9999, -1)
    
    # Obliczenie CPA
    closest_x = dx + dvx*tcpa
    closest_y = dy + dvy*tcpa
    cpa_deg = math.sqrt(closest_x**2 + closest_y**2)
    cpa_nm  = cpa_deg * 60.0  # 1° ~ 60 nm
    
    return (cpa_nm, tcpa)

###############################################################################
# 2) parse_ais
###############################################################################
def parse_ais(record_bytes):
    """
    Parsuje rekord JSON z Pub/Sub (AIS).
    Wymaga: [mmsi, latitude, longitude, cog, sog, timestamp].
    Dodatkowo obsługuje 'heading' (TrueHeading) i dim_a..d, ship_name, itp.
    """
    try:
        data = json.loads(record_bytes.decode("utf-8"))
        
        required = ["mmsi", "latitude", "longitude", "cog", "sog", "timestamp"]
        if not all(r in data for r in required):
            return None
        
        # Rzutowania podstawowych pól
        data["mmsi"]     = int(data["mmsi"])
        data["latitude"] = float(data["latitude"])
        data["longitude"]= float(data["longitude"])
        data["cog"]      = float(data["cog"])
        data["sog"]      = float(data["sog"])

        # Wyrzucamy ewentualny stary ship_length
        data.pop("ship_length", None)

        # heading (TrueHeading) – float lub None
        hdg = data.get("heading")  # klucz 'heading' w Pub/Sub
        if hdg is not None:
            try:
                data["heading"] = float(hdg)
            except:
                data["heading"] = None
        else:
            data["heading"] = None

        # Dimensje
        for dim in ["dim_a", "dim_b", "dim_c", "dim_d"]:
            val = data.get(dim)
            if val is not None:
                try:    
                    data[dim] = float(val)
                except:
                    data[dim] = None
            else:
                data[dim] = None

        # ship_name (fallback)
        data["ship_name"] = data.get("ship_name", "Unknown")

        # geohash – ewentualnie, do collision detection
        data["geohash"] = data.get("geohash", "none")

        return data
    except Exception as e:
        logger.error(f"Error parsing AIS record: {e}")
        return None

###############################################################################
# 3) CollisionDoFn – stateful detection
###############################################################################
class CollisionDoFn(beam.DoFn):
    """
    Stateful DoFn do wykrywania kolizji.
    """
    RECORDS_STATE = BagStateSpec("records_state", beam.coders.TupleCoder((
        beam.coders.FastPrimitivesCoder(),  # ship dict
        beam.coders.FastPrimitivesCoder()   # timestamp
    )))

    def process(self, element, records_state=beam.DoFn.StateParam(RECORDS_STATE)):
        gh, ship = element
        now_sec = time.time()

        # Dodaj nowy statek do stanu
        records_state.add((ship, now_sec))

        # Odczytaj stan i odfiltruj stare wpisy
        old_list = list(records_state.read())
        fresh = []
        for (s, t) in old_list:
            if (now_sec - t) <= STATE_RETENTION_SEC:
                fresh.append((s, t))

        # Aktualizacja stanu
        records_state.clear()
        for (s, t) in fresh:
            records_state.add((s, t))

        # Wykryj kolizje
        for (old_ship, _) in fresh:
            if old_ship["mmsi"] == ship["mmsi"]:
                continue
            cpa, tcpa = compute_cpa_tcpa(old_ship, ship)
            if cpa < CPA_THRESHOLD and 0 <= tcpa < TCPA_THRESHOLD:
                logger.info(f"Collision detected between MMSI {old_ship['mmsi']} and MMSI {ship['mmsi']} - CPA: {cpa} nm, TCPA: {tcpa} min")
                yield {
                    "mmsi_a": old_ship["mmsi"],
                    "mmsi_b": ship["mmsi"],
                    "timestamp": ship["timestamp"],  # z nowszego
                    "cpa": cpa,
                    "tcpa": tcpa,
                    "latitude_a": old_ship["latitude"],
                    "longitude_a": old_ship["longitude"],
                    "latitude_b": ship["latitude"],
                    "longitude_b": ship["longitude"]
                }

###############################################################################
# 4) Tabele BQ – logika
###############################################################################
def remove_geohash_and_dims(row):
    """
    Dla ships_positions – usuwamy geohash i dim_* 
    (bo w tabeli dynamicznej nie mamy tych kolumn).
    """
    new_row = dict(row)
    new_row.pop("geohash", None)
    for dim in ["dim_a", "dim_b", "dim_c", "dim_d"]:
        new_row.pop(dim, None)
    return new_row

class DeduplicateStaticDoFn(beam.DoFn):
    """
    Stateful DoFn do deduplikacji `ships_static` przy użyciu cache w pamięci workerów.
    Zakłada jeden worker, aby cache działało poprawnie.
    """
    def __init__(self):
        self.seen_mmsi = set()

    def process(self, row):
        mmsi = row["mmsi"]
        if mmsi not in self.seen_mmsi:
            self.seen_mmsi.add(mmsi)
            yield row
        else:
            logger.warning(f"Duplicate static data for MMSI {mmsi} detected.")

def keep_static_fields(row):
    """
    Zostawiamy tylko statyczne atrybuty: mmsi, ship_name, dim_a..d oraz update_time.
    """
    return {
        "mmsi": row["mmsi"],
        "ship_name": row["ship_name"],
        "dim_a": row["dim_a"],
        "dim_b": row["dim_b"],
        "dim_c": row["dim_c"],
        "dim_d": row["dim_d"],
        "update_time": datetime.datetime.utcnow().isoformat() + "Z"
    }

###############################################################################
# 5) Tworzenie Tabel BigQuery
###############################################################################
class CreateBQTableDoFn(beam.DoFn):
    """
    DoFn do tworzenia tabel BigQuery z odpowiednimi ustawieniami partycjonowania i klasteryzacji.
    """
    def process(self, table_ref):
        """
        table_ref: dict zawierający:
            - 'table_id': str (pełna nazwa tabeli)
            - 'schema': dict (schemat JSON)
            - 'time_partitioning': dict (opcjonalnie)
            - 'clustering_fields': list (opcjonalnie)
        """
        table_id = table_ref['table_id']
        schema = table_ref['schema']
        time_partitioning = table_ref.get('time_partitioning')
        clustering_fields = table_ref.get('clustering_fields')

        table = bigquery.Table(table_id, schema=schema)
        
        if time_partitioning:
            table.time_partitioning = bigquery.TimePartitioning(**time_partitioning)
        
        if clustering_fields:
            table.clustering_fields = clustering_fields
        
        try:
            client.get_table(table_id)
            logger.info(f"Tabela {table_id} już istnieje.")
        except bigquery.NotFound:
            client.create_table(table)
            logger.info(f"Tabela {table_id} została utworzona.")
        except Exception as e:
            logger.error(f"Error creating table {table_id}: {e}")

###############################################################################
# 6) Główny potok
###############################################################################
def run():
    # Ustawienia PipelineOptions z wykorzystaniem zmiennych środowiskowych
    options = PipelineOptions(
        runner='DataflowRunner',
        project=os.getenv("GOOGLE_CLOUD_PROJECT", "ais-collision-detection"),
        temp_location=os.getenv("TEMP_LOCATION", "gs://ais-collision-detection-bucket/temp"),
        staging_location=os.getenv("STAGING_LOCATION", "gs://ais-collision-detection-bucket/staging"),
        region=os.getenv("REGION", "us-east1"),
        num_workers=1,                      # Startuj z jednym workerem
        max_num_workers=10,                 # Maksymalna liczba workerów (autoscaling)
        autoscaling_algorithm='THROUGHPUT_BASED'  # Domyślna metoda autoscalingu
    )

    # Pipeline Options
    project_id  = os.getenv("GOOGLE_CLOUD_PROJECT", "ais-collision-detection")
    dataset     = os.getenv("LIVE_DATASET", "ais_dataset_us")
    input_sub   = os.getenv("INPUT_SUBSCRIPTION", "projects/ais-collision-detection/subscriptions/ais-data-sub")

    # Tabele BigQuery
    table_positions  = f"{project_id}:{dataset}.ships_positions"
    table_collisions = f"{project_id}:{dataset}.collisions"
    table_static     = f"{project_id}:{dataset}.ships_static"

    # Definicje tabel z partycjonowaniem i klasteryzacją
    tables_to_create = [
        {
            'table_id': table_positions,
            'schema': {
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
            },
            'time_partitioning': {
                "type": "DAY",
                "field": "timestamp",
                "expiration_ms": None  # Brak automatycznej retencji
            },
            'clustering_fields': ["mmsi"]
        },
        {
            'table_id': table_collisions,
            'schema': {
                "fields": [
                    {"name": "mmsi_a", "type": "INTEGER", "mode": "REQUIRED"},
                    {"name": "mmsi_b", "type": "INTEGER", "mode": "REQUIRED"},
                    {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
                    {"name": "cpa", "type": "FLOAT", "mode": "REQUIRED"},
                    {"name": "tcpa", "type": "FLOAT", "mode": "REQUIRED"},
                    {"name": "latitude_a", "type": "FLOAT", "mode": "REQUIRED"},
                    {"name": "longitude_a", "type": "FLOAT", "mode": "REQUIRED"},
                    {"name": "latitude_b", "type": "FLOAT", "mode": "REQUIRED"},
                    {"name": "longitude_b", "type": "FLOAT", "mode": "REQUIRED"}
                ]
            },
            'time_partitioning': {
                "type": "DAY",
                "field": "timestamp",
                "expiration_ms": None
            },
            'clustering_fields': ["mmsi_a", "mmsi_b"]
        },
        {
            'table_id': table_static,
            'schema': {
                "fields": [
                    {"name": "mmsi", "type": "INTEGER", "mode": "REQUIRED"},
                    {"name": "ship_name", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "dim_a", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "dim_b", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "dim_c", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "dim_d", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "update_time", "type": "TIMESTAMP", "mode": "REQUIRED"}
                ]
            },
            'time_partitioning': None,  # Brak partycjonowania
            'clustering_fields': ["mmsi"]
        }
    ]

    with beam.Pipeline(options=options) as p:
        ######################################################################
        # A) Tworzenie tabel BigQuery
        ######################################################################
        create_tables = (
            tables_to_create
            | "CreateTables" >> beam.ParDo(CreateBQTableDoFn())
        )

        ######################################################################
        # B) Odczyt i parsowanie danych z Pub/Sub
        ######################################################################
        lines = p | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=input_sub)
        parsed = (
            lines
            | "ParseAIS"   >> beam.Map(parse_ais)
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
        )

        ######################################################################
        # C) ships_positions – zapis w oknach 10s
        ######################################################################
        windowed_positions = (
            parsed
            | "WinPositions" >> beam.WindowInto(window.FixedWindows(10))
        )
        grouped_positions = (
            windowed_positions
            | "KeyPos"   >> beam.Map(lambda r: (None, r))
            | "GroupPos" >> beam.GroupByKey()
        )
        flatten_positions = (
            grouped_positions
            | "FlattenPos" >> beam.FlatMap(lambda kv: kv[1])
        )
        final_positions = flatten_positions | "RemoveDims" >> beam.Map(remove_geohash_and_dims)

        final_positions | "WritePositions" >> WriteToBigQuery(
            table=table_positions,
            schema="""
              mmsi:INTEGER,
              ship_name:STRING,
              latitude:FLOAT,
              longitude:FLOAT,
              cog:FLOAT,
              sog:FLOAT,
              heading:FLOAT,
              timestamp:TIMESTAMP
            """,
            create_disposition=BigQueryDisposition.CREATE_NEVER,  # Zakładamy, że tabele są tworzone wcześniej
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            method="STREAMING_INSERTS",
        )

        ######################################################################
        # D) ships_static – deduplikacja i zapis z memory-based cache
        ######################################################################
        with_dims = (
            parsed
            | "FilterDims" >> beam.Filter(
                lambda r: any(r.get(dim) is not None for dim in ["dim_a", "dim_b", "dim_c", "dim_d"])
            )
        )
        # Deduplicate using in-memory cache (assuming single worker)
        deduped_static = with_dims | "DedupStatic" >> beam.ParDo(DeduplicateStaticDoFn())
        prepared_static = deduped_static | "KeepStaticFields" >> beam.Map(keep_static_fields)

        prepared_static | "WriteStatic" >> WriteToBigQuery(
            table=table_static,
            schema="""
              mmsi:INTEGER,
              ship_name:STRING,
              dim_a:FLOAT,
              dim_b:FLOAT,
              dim_c:FLOAT,
              dim_d:FLOAT,
              update_time:TIMESTAMP
            """,
            create_disposition=BigQueryDisposition.CREATE_NEVER,  # Zakładamy, że tabele są tworzone wcześniej
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            method="STREAMING_INSERTS",
        )

        ######################################################################
        # E) collisions – stateful detection i zapis w oknach 10s
        ######################################################################
        keyed = parsed | "KeyByGeohash" >> beam.Map(lambda r: (r["geohash"], r))
        collisions_raw = keyed | "DetectCollisions" >> beam.ParDo(CollisionDoFn())

        windowed_collisions = (
            collisions_raw
            | "WinColl"   >> beam.WindowInto(window.FixedWindows(10))
            | "KeyColl"   >> beam.Map(lambda c: (None, c))
            | "GroupColl" >> beam.GroupByKey()
            | "FlattenColl" >> beam.FlatMap(lambda kv: kv[1])
        )

        windowed_collisions | "WriteCollisions" >> WriteToBigQuery(
            table=table_collisions,
            schema="""
              mmsi_a:INTEGER,
              mmsi_b:INTEGER,
              timestamp:TIMESTAMP,
              cpa:FLOAT,
              tcpa:FLOAT,
              latitude_a:FLOAT,
              longitude_a:FLOAT,
              latitude_b:FLOAT,
              longitude_b:FLOAT
            """,
            create_disposition=BigQueryDisposition.CREATE_NEVER,  # Zakładamy, że tabele są tworzone wcześniej
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            method="STREAMING_INSERTS",
        )

###############################################################################
# Tworzenie Tabel BigQuery
###############################################################################
class CreateBQTableDoFn(beam.DoFn):
    """
    DoFn do tworzenia tabel BigQuery z odpowiednimi ustawieniami partycjonowania i klasteryzacji.
    """
    def process(self, table_ref):
        """
        table_ref: dict zawierający:
            - 'table_id': str (pełna nazwa tabeli)
            - 'schema': dict (schemat JSON)
            - 'time_partitioning': dict (opcjonalnie)
            - 'clustering_fields': list (opcjonalnie)
        """
        table_id = table_ref['table_id']
        schema = table_ref['schema']
        time_partitioning = table_ref.get('time_partitioning')
        clustering_fields = table_ref.get('clustering_fields')

        table = bigquery.Table(table_id, schema=schema)
        
        if time_partitioning:
            table.time_partitioning = bigquery.TimePartitioning(**time_partitioning)
        
        if clustering_fields:
            table.clustering_fields = clustering_fields
        
        try:
            client.get_table(table_id)
            logger.info(f"Tabela {table_id} już istnieje.")
        except bigquery.NotFound:
            client.create_table(table)
            logger.info(f"Tabela {table_id} została utworzona.")
        except Exception as e:
            logger.error(f"Error creating table {table_id}: {e}")

###############################################################################
# Uruchomienie potoku
###############################################################################
if __name__ == "__main__":
    run()