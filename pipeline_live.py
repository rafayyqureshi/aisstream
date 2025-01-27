#!/usr/bin/env python3
import os
import json
import math
import time
import logging
import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from google.cloud import bigquery
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.transforms.userstate import BagStateSpec
import apache_beam.coders

from apache_beam import window

CPA_THRESHOLD = 0.5        # mile morskie
TCPA_THRESHOLD = 10.0      # minuty
STATE_RETENTION_SEC = 120  # 2 min (stateful collision detection)

def parse_ais(record: bytes):
    """
    Parsuje rekord JSON z Pub/Sub (AIS).
    Wymaga: [mmsi, latitude, longitude, cog, sog, timestamp].
    Dodatkowo obsługuje 'heading' (TrueHeading) i dim_a..d, ship_name, itp.
    """
    try:
        data = json.loads(record.decode("utf-8"))
        
        required = ["mmsi", "latitude", "longitude", "cog", "sog", "timestamp"]
        if not all(r in data for r in required):
            return None
        
        # Rzutowania podstawowych pól
        data["mmsi"] = int(data["mmsi"])
        data["latitude"] = float(data["latitude"])
        data["longitude"] = float(data["longitude"])
        data["cog"] = float(data["cog"])
        data["sog"] = float(data["sog"])

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
    except:
        return None

def compute_cpa_tcpa(a, b):
    """
    Przykładowa funkcja do liczenia (cpa, tcpa).
    W tym projekcie możesz mieć inną implementację. 
    """
    # np. pomijamy statki < 50m (kiedyś?), albo implementacja uproszczona...
    return (9999, -1)

class CollisionDoFn(beam.DoFn):
    """
    Stateful DoFn do wykrywania kolizji – roboczy, 
    o ile faktycznie używasz stateful detection w geohash.
    """
    RECORDS_STATE = BagStateSpec("records_state", beam.coders.FastPrimitivesCoder())

    def process(self, element, records_state=beam.DoFn.StateParam(RECORDS_STATE)):
        gh, ship = element
        now_sec = time.time()

        # Dodaj nowy statek do stanu
        records_state.add((ship, now_sec))

        # Odczyt stanu i odfiltrowanie starych
        old_list = list(records_state.read())
        fresh = []
        for (s, t) in old_list:
            if (now_sec - t) <= STATE_RETENTION_SEC:
                fresh.append((s,t))

        records_state.clear()
        for (s,t) in fresh:
            records_state.add((s,t))

        # Wykryj kolizje – uproszczone
        for (old_ship, _) in fresh:
            if old_ship["mmsi"] == ship["mmsi"]:
                continue
            cpa, tcpa = compute_cpa_tcpa(old_ship, ship)
            if cpa < CPA_THRESHOLD and 0 <= tcpa < TCPA_THRESHOLD:
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

def remove_geohash_and_dims(row):
    """
    Dla ships_positions – usuwamy geohash i dim_* 
    (bo w tabeli dynamicznej nie mamy tych kolumn).
    """
    new_row = dict(row)
    new_row.pop("geohash", None)
    for dim in ["dim_a","dim_b","dim_c","dim_d"]:
        new_row.pop(dim, None)
    return new_row

def keep_static_fields_only(row):
    """
    Dla ships_static – zachowujemy: mmsi, ship_name, dim_a,b,c,d, plus update_time.
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

def run():
    logging.getLogger().setLevel(logging.INFO)
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True

    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "ais-collision-detection")
    dataset = os.getenv("LIVE_DATASET", "ais_dataset_us")
    input_sub = os.getenv("INPUT_SUBSCRIPTION",
                          "projects/ais-collision-detection/subscriptions/ais-data-sub")

    table_positions = f"{project_id}:{dataset}.ships_positions"
    table_collisions = f"{project_id}:{dataset}.collisions"
    table_static = f"{project_id}:{dataset}.ships_static"

    with beam.Pipeline(options=pipeline_options) as p:
        # 1) Odczyt z Pub/Sub
        lines = p | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=input_sub)

        # 2) Parsowanie
        parsed = (
            lines
            | "ParseAIS" >> beam.Map(parse_ais)
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
        )

        # 3A) ships_static
        static_data = (
            parsed
            | "FilterWithDims" >> beam.Filter(
                lambda r: any(r.get(dim) is not None for dim in ["dim_a","dim_b","dim_c","dim_d"])
            )
        )
        windowed_static = static_data | "WinStatic" >> beam.WindowInto(window.FixedWindows(60))
        grouped_static = (
            windowed_static
            | "KeyStatic" >> beam.Map(lambda r: (None, r))
            | "GroupStatic" >> beam.GroupByKey()
        )
        flattened_static = grouped_static | "ExtractStatic" >> beam.FlatMap(lambda kv: kv[1])
        prepped_static = flattened_static | "KeepStatic" >> beam.Map(keep_static_fields_only)

        prepped_static | "WriteStatic" >> WriteToBigQuery(
            table=table_static,
            schema=(
                "mmsi:INTEGER,"
                "ship_name:STRING,"
                "dim_a:FLOAT,"
                "dim_b:FLOAT,"
                "dim_c:FLOAT,"
                "dim_d:FLOAT,"
                "update_time:TIMESTAMP"
            ),
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
        )

        # 3B) ships_positions (dynamic)
        #   Dodajemy heading do schematu
        cleaned_for_positions = parsed | "RemoveHashDims" >> beam.Map(remove_geohash_and_dims)

        windowed_positions = cleaned_for_positions | "WinPositions" >> beam.WindowInto(window.FixedWindows(10))
        grouped_positions = (
            windowed_positions
            | "KeyPositions" >> beam.Map(lambda r: (None, r))
            | "GroupPositions" >> beam.GroupByKey()
        )
        flattened_positions = grouped_positions | "ExtractPositions" >> beam.FlatMap(lambda kv: kv[1])

        flattened_positions | "WritePositions" >> WriteToBigQuery(
            table=table_positions,
            schema=(
                "mmsi:INTEGER,"
                "ship_name:STRING,"
                "latitude:FLOAT,"
                "longitude:FLOAT,"
                "cog:FLOAT,"
                "sog:FLOAT,"
                "heading:FLOAT,"  # <--- NOWE
                "timestamp:TIMESTAMP"
            ),
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
        )

        # 3C) collisions
        keyed = parsed | "KeyByGeohash" >> beam.Map(lambda r: (r["geohash"], r))
        collisions_raw = keyed | "DetectCollisions" >> beam.ParDo(CollisionDoFn())
        windowed_collisions = collisions_raw | "WinColl" >> beam.WindowInto(window.FixedWindows(10))
        grouped_coll = (
            windowed_collisions
            | "KeyColl" >> beam.Map(lambda c: (None, c))
            | "GroupColl" >> beam.GroupByKey()
        )
        flattened_coll = grouped_coll | "ExtractColl" >> beam.FlatMap(lambda kv: kv[1])

        flattened_coll | "WriteCollisions" >> WriteToBigQuery(
            table=table_collisions,
            schema=(
                "mmsi_a:INTEGER,"
                "mmsi_b:INTEGER,"
                "timestamp:TIMESTAMP,"
                "cpa:FLOAT,"
                "tcpa:FLOAT,"
                "latitude_a:FLOAT,"
                "longitude_a:FLOAT,"
                "latitude_b:FLOAT,"
                "longitude_b:FLOAT"
            ),
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
        )

if __name__ == "__main__":
    run()