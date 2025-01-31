#!/usr/bin/env python3
import os
import json
import time
import logging
import datetime
from dotenv import load_dotenv

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.transforms.userstate import BagStateSpec
import apache_beam.coders

from apache_beam import window
from apache_beam.utils.timestamp import Duration

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
    Następnie obliczanie CPA/TCPA.
    Dodatkowo:
      - side input z ships_static => pobieramy ship_name,
      - obliczamy distance (aktualny),
      - is_active => true, jeśli aktualny distance <= minimalny distance dotąd; 
        w momencie, gdy distance zacznie rosnąć, is_active=false.
    """

    RECORDS_STATE = BagStateSpec(
        "records_state",
        beam.coders.TupleCoder((beam.coders.FastPrimitivesCoder(),
                                beam.coders.FastPrimitivesCoder()))
    )
    # Dodatkowy stan na minimalny distance "per pair"
    # Będziemy przechowywać krotki ( (mmsi_a,mmsi_b), min_distance )
    PAIRS_STATE = BagStateSpec(
        "pairs_state",
        beam.coders.TupleCoder((
            beam.coders.FastPrimitivesCoder(),
            beam.coders.FastPrimitivesCoder()
        ))
    )

    def __init__(self, static_side):
        super().__init__()
        self.static_side = static_side  # side input (dict)

    def process(self, element,
                records_state=beam.DoFn.StateParam(RECORDS_STATE),
                pairs_state=beam.DoFn.StateParam(PAIRS_STATE)):
        gh, ship = element
        now_sec = time.time()

        # Zapis do stanu (lista statków w tym geohash)
        records_state.add((ship, now_sec))

        # Czytamy dotychczasowe dane i czyścimy stare
        old_list = list(records_state.read())
        fresh = [(s, t) for (s, t) in old_list if (now_sec - t) <= STATE_RETENTION_SEC]

        records_state.clear()
        for (s, t) in fresh:
            records_state.add((s, t))

        # side input => dict {mmsi: {ship_name, dim_a, dim_b, ...}}
        static_dict = self.static_side

        # PairsState => [( (mmsi_a,mmsi_b), min_dist ), ... ]
        pairs_list = list(pairs_state.read())
        # Zamieniamy to na dict w pamięci, by łatwiej modyfikować
        pairs_dict = {}
        for pair_key, dist_val in pairs_list:
            pairs_dict[pair_key] = dist_val
        # Wyczyść pairs_state w tym momencie (odtworzymy potem)
        pairs_state.clear()

        # Sprawdzamy ewentualne kolizje
        for (old_ship, _) in fresh:
            if old_ship["mmsi"] == ship["mmsi"]:
                continue

            dist_nm = local_distance_nm(old_ship, ship)
            if dist_nm > DISTANCE_THRESHOLD_NM:
                continue
            if not is_approaching(old_ship, ship):
                continue

            cpa, tcpa = compute_cpa_tcpa(old_ship, ship)
            if cpa < 0.5 and 0 <= tcpa < 10:
                # Mamy kolizję
                mA = old_ship["mmsi"]
                mB = ship["mmsi"]
                # Upewnijmy się, że klucz jest w porządku (mniejszy,mwiększy)
                pair_key = tuple(sorted([mA, mB]))

                # Odczytaj poprzedni minimalny dystans (jeśli istnieje)
                prev_min_dist = pairs_dict.get(pair_key, None)
                if prev_min_dist is None:
                    prev_min_dist = dist_nm

                # Czy aktualny dystans jest <= minimalnego?
                is_active = True
                if dist_nm > prev_min_dist:
                    # zaczęło rosnąć => is_active = False
                    is_active = False

                # Aktualizuj minimalny distance
                new_min_dist = min(prev_min_dist, dist_nm)
                pairs_dict[pair_key] = new_min_dist

                # Zbieramy nazwy
                infoA = static_dict.get(mA, {})
                infoB = static_dict.get(mB, {})
                nameA = infoA.get("ship_name", "Unknown")
                nameB = infoB.get("ship_name", "Unknown")

                yield {
                    "mmsi_a": mA,
                    "ship_name_a": nameA,
                    "mmsi_b": mB,
                    "ship_name_b": nameB,
                    "timestamp": ship["timestamp"],
                    "cpa": cpa,
                    "tcpa": tcpa,
                    "distance": dist_nm,
                    "is_active": is_active,
                    "latitude_a": old_ship["latitude"],
                    "longitude_a": old_ship["longitude"],
                    "latitude_b": ship["latitude"],
                    "longitude_b": ship["longitude"]
                }

        # Zapisz pairs_dict z powrotem do pairs_state
        for pk, dval in pairs_dict.items():
            pairs_state.add((pk, dval))


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

    # Dodajemy kolumny: ship_name_a, ship_name_b, distance, is_active
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
                    {"name": "mmsi_a",       "type": "INTEGER", "mode": "REQUIRED"},
                    {"name": "ship_name_a",  "type": "STRING",  "mode": "NULLABLE"},
                    {"name": "mmsi_b",       "type": "INTEGER", "mode": "REQUIRED"},
                    {"name": "ship_name_b",  "type": "STRING",  "mode": "NULLABLE"},
                    {"name": "timestamp",    "type": "TIMESTAMP","mode": "REQUIRED"},
                    {"name": "cpa",          "type": "FLOAT",   "mode": "REQUIRED"},
                    {"name": "tcpa",         "type": "FLOAT",   "mode": "REQUIRED"},
                    {"name": "distance",     "type": "FLOAT",   "mode": "REQUIRED"},
                    {"name": "is_active",    "type": "BOOL",    "mode": "REQUIRED"},
                    {"name": "latitude_a",   "type": "FLOAT",   "mode": "REQUIRED"},
                    {"name": "longitude_a",  "type": "FLOAT",   "mode": "REQUIRED"},
                    {"name": "latitude_b",   "type": "FLOAT",   "mode": "REQUIRED"},
                    {"name": "longitude_b",  "type": "FLOAT",   "mode": "REQUIRED"}
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

        # Tworzymy side input z ships_static (mmsi->(ship_name, dim_a,b,c,d))
        static_dict = (
            ships_static
            | "ShipStaticKey" >> beam.Map(lambda row: (row["mmsi"], row))
            | "GroupByMmsi"   >> beam.GroupByKey()
            | "PickOne"       >> beam.Map(lambda kv: (kv[0], list(kv[1])[0]))
        )
        side_static = beam.pvalue.AsDict(static_dict)

        # 5) collisions
        filtered_for_collisions = (
            parsed
            | "FilterShortShips" >> beam.Filter(is_ship_long_enough)
        )
        keyed = filtered_for_collisions | "KeyGeohash" >> beam.Map(lambda r: (r["geohash"], r))

        # Wykrywanie kolizji z logiką is_active
        collisions_raw = (
            keyed
            | "DetectCollisions" >> beam.ParDo(CollisionDoFn(side_static), side_static)
        )

        # Zapis do collisions (5 sek okno, allowed_lateness=0)
        (
            collisions_raw
            | "WinColl" >> beam.WindowInto(
                window.FixedWindows(5),
                allowed_lateness=Duration(0)
            )
            | "WriteCollisions" >> WriteToBigQuery(
                table=table_collisions,
                schema="""
                  mmsi_a:INTEGER,
                  ship_name_a:STRING,
                  mmsi_b:INTEGER,
                  ship_name_b:STRING,
                  timestamp:TIMESTAMP,
                  cpa:FLOAT,
                  tcpa:FLOAT,
                  distance:FLOAT,
                  is_active:BOOL,
                  latitude_a:FLOAT,
                  longitude_a:FLOAT,
                  latitude_b:FLOAT,
                  longitude_b:FLOAT
                """,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                method="STREAMING_INSERTS",
            )
        )

if __name__ == "__main__":
    run()