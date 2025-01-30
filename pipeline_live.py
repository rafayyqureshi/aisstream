#!/usr/bin/env python3
import os
import json
import time
import logging
import datetime
from dotenv import load_dotenv

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.transforms.userstate import BagStateSpec
import apache_beam.coders

from apache_beam import window

# Import funkcji z cpa_utils.py
from cpa_utils import (
    compute_cpa_tcpa,
    local_distance_nm,
    is_approaching
)

# Załaduj zmienne z .env
load_dotenv()

# Konfiguracja loggera
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Parametry kolizji i retencji
CPA_THRESHOLD        = 0.5   # mile morskie
TCPA_THRESHOLD       = 10.0  # minuty
STATE_RETENTION_SEC  = 120   # 2 minuty

# Parametry wstępnego filtra
DISTANCE_THRESHOLD_NM = 5.0  # Dystans powyżej którego nie liczymy CPA

def parse_ais(record_bytes):
    """
    Parsuje JSON AIS z Pub/Sub.
    """
    try:
        data = json.loads(record_bytes.decode("utf-8"))
        req = ["mmsi","latitude","longitude","cog","sog","timestamp"]
        if not all(r in data for r in req):
            return None

        data["mmsi"]      = int(data["mmsi"])
        data["latitude"]  = float(data["latitude"])
        data["longitude"] = float(data["longitude"])
        data["cog"]       = float(data["cog"])
        data["sog"]       = float(data["sog"])
        data.pop("ship_length", None)  # usuwamy stare pole

        # heading
        hdg = data.get("heading")
        if hdg is not None:
            try:
                data["heading"] = float(hdg)
            except:
                data["heading"] = None
        else:
            data["heading"] = None

        # wymiary
        for d in ["dim_a","dim_b","dim_c","dim_d"]:
            v = data.get(d)
            data[d] = float(v) if v is not None else None

        # nazwa statku
        data["ship_name"] = data.get("ship_name","Unknown")

        # geohash (zakładamy, że jest nadawany w rozdzielczości ~5-7 Nm)
        data["geohash"] = data.get("geohash","none")

        return data
    except:
        return None

def is_ship_long_enough(ship):
    """
    Zwraca True, jeśli statek ma dim_a i dim_b (nie None),
    a ich suma > 50m. W przeciwnym razie False.
    """
    dim_a = ship.get("dim_a")
    dim_b = ship.get("dim_b")
    if dim_a is None or dim_b is None:
        return False
    return (dim_a + dim_b) > 50

class CollisionDoFn(beam.DoFn):
    """
    Stateful DoFn do wykrywania kolizji w ramach geohash.
    Wstępne filtrowanie:
      1) lokalny dystans < 5 nm,
      2) statki się zbliżają (dot product < 0).
    Dopiero potem obliczamy CPA/TCPA i sprawdzamy progi cpa/tcpa.
    """
    RECORDS_STATE = BagStateSpec(
        "records_state",
        beam.coders.TupleCoder((beam.coders.FastPrimitivesCoder(),
                                beam.coders.FastPrimitivesCoder()))
    )

    def process(self, element, records_state=beam.DoFn.StateParam(RECORDS_STATE)):
        gh, ship = element
        now_sec = time.time()

        # Zapis do stanu
        records_state.add((ship, now_sec))

        # Czytamy dotychczasowe dane i czyścimy stare
        old_list = list(records_state.read())
        fresh = [(s, t) for (s, t) in old_list if (now_sec - t) <= STATE_RETENTION_SEC]

        records_state.clear()
        for (s, t) in fresh:
            records_state.add((s, t))

        # Sprawdzamy ewentualne kolizje dla par (old_ship, ship)
        for (old_ship, _) in fresh:
            if old_ship["mmsi"] == ship["mmsi"]:
                continue

            # --- WSTĘPNE FILTROWANIE ---
            dist_nm = local_distance_nm(old_ship, ship)
            if dist_nm > DISTANCE_THRESHOLD_NM:
                # Statki są zbyt daleko (np. >5 nm), pomijamy
                continue

            if not is_approaching(old_ship, ship):
                # Statki się nie zbliżają - pomijamy
                continue

            # --- Jeśli pozytywnie przeszły filtry, liczymy CPA/TCPA ---
            cpa, tcpa = compute_cpa_tcpa(old_ship, ship)

            # Filtr końcowy: cpa < 0.5 i 0 <= tcpa < 10
            if cpa < CPA_THRESHOLD and 0 <= tcpa < TCPA_THRESHOLD:
                logging.info(
                    f"Collision detected between {old_ship['mmsi']} & {ship['mmsi']} "
                    f"cpa={cpa}, tcpa={tcpa}"
                )
                yield {
                    "mmsi_a": old_ship["mmsi"],
                    "mmsi_b": ship["mmsi"],
                    "timestamp": ship["timestamp"],
                    "cpa": cpa,
                    "tcpa": tcpa,
                    "latitude_a": old_ship["latitude"],
                    "longitude_a": old_ship["longitude"],
                    "latitude_b": ship["latitude"],
                    "longitude_b": ship["longitude"]
                }

def remove_geohash_and_dims(row):
    """
    Usunięcie geohash i dim_* z docelowego wiersza do ships_positions.
    """
    new_row = dict(row)
    new_row.pop("geohash", None)
    for d in ["dim_a","dim_b","dim_c","dim_d"]:
        new_row.pop(d, None)
    return new_row

class DeduplicateStaticDoFn(beam.DoFn):
    """
    Naiwna deduplikacja w pamięci (cache w worker).
    Jeśli autoscaling > 1, to może nieco przepuszczać duplikaty.
    """
    def __init__(self):
        self.seen = set()

    def process(self, row):
        mmsi = row["mmsi"]
        if mmsi not in self.seen:
            self.seen.add(mmsi)
            yield row

def keep_static_fields(row):
    """
    Zostawiamy tylko statyczne kolumny + update_time.
    """
    return {
        "mmsi": row["mmsi"],
        "ship_name": row["ship_name"],
        "dim_a": row["dim_a"],
        "dim_b": row["dim_b"],
        "dim_c": row["dim_c"],
        "dim_d": row["dim_d"],
        "update_time": datetime.datetime.utcnow().isoformat()+"Z"
    }

class CreateBQTableDoFn(beam.DoFn):
    """
    Tworzy tabele w BigQuery w formacie:
      project.dataset.table
    (zamiast "project:dataset.table").
    """
    def process(self, table_ref):
        client_local = bigquery.Client()

        table_id = table_ref['table_id']
        schema    = table_ref['schema']['fields']
        time_part = table_ref.get('time_partitioning')
        cluster   = table_ref.get('clustering_fields')

        table = bigquery.Table(table_id, schema=schema)

        if time_part:
            # Zastępujemy 'type' na 'type_' aby uniknąć kolizji słów kluczowych
            time_part_corrected = {
                k if k != 'type' else 'type_': v for k, v in time_part.items()
            }
            table.time_partitioning = bigquery.TimePartitioning(**time_part_corrected)

        if cluster:
            table.clustering_fields = cluster

        try:
            client_local.get_table(table_id)
            logging.info(f"Tabela {table_id} już istnieje.")
        except NotFound:
            client_local.create_table(table)
            logging.info(f"Utworzono nową tabelę: {table_id}")
        except Exception as e:
            logging.error(f"Nie można utworzyć tabeli {table_id}: {e}")

def run():
    logging.getLogger().setLevel(logging.INFO)

    # Konfiguracja z .env
    project_id  = os.getenv("GOOGLE_CLOUD_PROJECT","ais-collision-detection")
    dataset     = os.getenv("LIVE_DATASET","ais_dataset_us")
    input_sub   = os.getenv("INPUT_SUBSCRIPTION","projects/ais-collision-detection/subscriptions/ais-data-sub")
    region      = os.getenv("REGION","us-east1")
    temp_loc    = os.getenv("TEMP_LOCATION","gs://ais-collision-detection-bucket/temp")
    staging_loc = os.getenv("STAGING_LOCATION","gs://ais-collision-detection-bucket/staging")
    job_name    = os.getenv("JOB_NAME","ais-collision-job")

    # ZAMIANA ':' NA '.' W NAZWACH TABEL
    table_positions  = f"{project_id}.{dataset}.ships_positions"
    table_collisions = f"{project_id}.{dataset}.collisions"
    table_static     = f"{project_id}.{dataset}.ships_static"

    # Retencja 24h
    tables_to_create = [
        {
            'table_id': table_positions,
            'schema': {
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
            },
            'time_partitioning': {
                "type_": "DAY",
                "field": "timestamp",
                "expiration_ms": 86400000
            },
            'clustering_fields': ["mmsi"]
        },
        {
            'table_id': table_collisions,
            'schema': {
                "fields": [
                    {"name": "mmsi_a",      "type": "INTEGER", "mode": "REQUIRED"},
                    {"name": "mmsi_b",      "type": "INTEGER", "mode": "REQUIRED"},
                    {"name": "timestamp",   "type": "TIMESTAMP","mode": "REQUIRED"},
                    {"name": "cpa",         "type": "FLOAT",   "mode": "REQUIRED"},
                    {"name": "tcpa",        "type": "FLOAT",   "mode": "REQUIRED"},
                    {"name": "latitude_a",  "type": "FLOAT",   "mode": "REQUIRED"},
                    {"name": "longitude_a", "type": "FLOAT",   "mode": "REQUIRED"},
                    {"name": "latitude_b",  "type": "FLOAT",   "mode": "REQUIRED"},
                    {"name": "longitude_b", "type": "FLOAT",   "mode": "REQUIRED"}
                ]
            },
            'time_partitioning': {
                "type_": "DAY",
                "field": "timestamp",
                "expiration_ms": 86400000
            },
            'clustering_fields': ["mmsi_a","mmsi_b"]
        },
        {
            'table_id': table_static,
            'schema': {
                "fields": [
                    {"name": "mmsi",        "type": "INTEGER","mode": "REQUIRED"},
                    {"name": "ship_name",   "type": "STRING", "mode": "NULLABLE"},
                    {"name": "dim_a",       "type": "FLOAT",  "mode": "NULLABLE"},
                    {"name": "dim_b",       "type": "FLOAT",  "mode": "NULLABLE"},
                    {"name": "dim_c",       "type": "FLOAT",  "mode": "NULLABLE"},
                    {"name": "dim_d",       "type": "FLOAT",  "mode": "NULLABLE"},
                    {"name": "update_time", "type": "TIMESTAMP","mode": "REQUIRED"}
                ]
            },
            'time_partitioning': None,
            'clustering_fields': ["mmsi"]
        }
    ]

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
        # 1) Tworzymy tabele (o ile nie istnieją)
        _ = (
            tables_to_create
            | "CreateTables" >> beam.ParDo(CreateBQTableDoFn())
        )

        # 2) Odczyt z PubSub -> parse
        lines = p | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=input_sub)
        parsed = (
            lines
            | "ParseAIS"   >> beam.Map(parse_ais)
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
        )

        # 3) ships_positions (co 10s)
        w_pos = (
            parsed
            | "WinPositions"   >> beam.WindowInto(window.FixedWindows(10))
            | "KeyPositions"   >> beam.Map(lambda r: (None, r))
            | "GroupPositions" >> beam.GroupByKey()
            | "FlatPositions"  >> beam.FlatMap(lambda kv: kv[1])
            | "RmGeohashDims"  >> beam.Map(remove_geohash_and_dims)
        )
        w_pos | "WritePositions" >> WriteToBigQuery(
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
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            method="STREAMING_INSERTS",
        )

        # 4) ships_static (deduplicate in-memory), okna 5min
        ships_static = (
            parsed
            | "FilterDims" >> beam.Filter(
                lambda r: any(r.get(dim) for dim in ["dim_a","dim_b","dim_c","dim_d"])
            )
            | "WindowStatic" >> beam.WindowInto(window.FixedWindows(300))
            | "KeyStatic"    >> beam.Map(lambda r: (r["mmsi"], r))
            | "GroupStaticByMMSI" >> beam.GroupByKey()
            | "LatestStaticPerMMSI" >> beam.Map(
                lambda kv: max(kv[1], key=lambda x: x["timestamp"])
            )
            | "DeduplicateStatic" >> beam.ParDo(DeduplicateStaticDoFn())
            | "PrepStatic"        >> beam.Map(keep_static_fields)
        )
        ships_static | "WriteStatic" >> WriteToBigQuery(
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
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            method="STREAMING_INSERTS",
        )

        # 5) collisions
        filtered_for_collisions = (
            parsed
            | "FilterShortShips" >> beam.Filter(is_ship_long_enough)
        )
        keyed = filtered_for_collisions | "KeyGeohash" >> beam.Map(lambda r: (r["geohash"], r))
        collisions_raw = keyed | "DetectCollisions" >> beam.ParDo(CollisionDoFn())

        w_coll = (
            collisions_raw
            | "WinColl"   >> beam.WindowInto(window.FixedWindows(10))
            | "KeyColl"   >> beam.Map(lambda c: (None, c))
            | "GroupColl" >> beam.GroupByKey()
            | "FlatColl"  >> beam.FlatMap(lambda kv: kv[1])
        )
        w_coll | "WriteCollisions" >> WriteToBigQuery(
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
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            method="STREAMING_INSERTS",
        )

if __name__ == "__main__":
    run()