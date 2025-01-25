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

# Dodatkowe importy do okien
from apache_beam import window

CPA_THRESHOLD = 0.5        # mile morskie
TCPA_THRESHOLD = 10.0      # minuty
STATE_RETENTION_SEC = 120  # (2 min) do wykrywania kolizji

def parse_ais(record: bytes):
    """
    Parsuje rekord JSON z Pub/Sub (AIS).
    Zwraca None, jeśli brakuje wymaganych pól dynamicznych
    (mmsi, lat, lon, cog, sog, timestamp).
    """
    try:
        data = json.loads(record.decode("utf-8"))
        # Pola dynamiczne, minimalne do zapisu w ships_positions
        required = ["mmsi", "latitude", "longitude", "cog", "sog", "timestamp"]
        if not all(r in data for r in required):
            return None

        # Rzutowania
        data["mmsi"] = int(data["mmsi"])
        data["latitude"] = float(data["latitude"])
        data["longitude"] = float(data["longitude"])
        data["cog"] = float(data["cog"])
        data["sog"] = float(data["sog"])

        # Pola statyczne (mogą być None / nieobecne)
        # dim_a, dim_b, dim_c, dim_d
        for dim in ["dim_a", "dim_b", "dim_c", "dim_d"]:
            if dim in data:
                try:
                    data[dim] = float(data[dim])
                except:
                    data[dim] = None
            else:
                data[dim] = None

        # ship_name
        if "ship_name" not in data:
            data["ship_name"] = "Unknown"

        # geohash (opcjonalne – do CollisionDoFn)
        data["geohash"] = data.get("geohash", "none")

        return data
    except:
        return None

def compute_cpa_tcpa(a, b):
    """
    Oblicza (cpa, tcpa) na podstawie punktu anteny (lat,lon).
    Zwraca (9999, -1) jeśli tcpa < 0.
    """
    latRef = (a["latitude"] + b["latitude"]) / 2.0
    scaleLat = 111000.0
    scaleLon = 111000.0 * math.cos(math.radians(latRef))

    def to_xy(lat, lon):
        return (lon * scaleLon, lat * scaleLat)

    xA, yA = to_xy(a["latitude"], a["longitude"])
    xB, yB = to_xy(b["latitude"], b["longitude"])

    sogA = float(a["sog"] or 0)
    sogB = float(b["sog"] or 0)

    def cog_to_vec(cog_deg, sog_nm):
        r = math.radians(cog_deg)
        return (sog_nm * math.sin(r), sog_nm * math.cos(r))

    vxA, vyA = cog_to_vec(a["cog"], sogA)
    vxB, vyB = cog_to_vec(b["cog"], sogB)

    dx = xA - xB
    dy = yA - yB

    speed_scale = 1852.0 / 60.0  # nm/h => m/min
    dvx_mpm = (vxA - vxB) * speed_scale
    dvy_mpm = (vyA - vyB) * speed_scale

    VV = dvx_mpm**2 + dvy_mpm**2
    PV = dx * dvx_mpm + dy * dvy_mpm

    if VV == 0:
        tcpa = 0.0
    else:
        tcpa = -PV / VV

    if tcpa < 0:
        return (9999, -1)

    # Pozycje przy CPA
    vxA_mpm = vxA * speed_scale
    vyA_mpm = vyA * speed_scale
    vxB_mpm = vxB * speed_scale
    vyB_mpm = vyB * speed_scale

    xA2 = xA + vxA_mpm * tcpa
    yA2 = yA + vyA_mpm * tcpa
    xB2 = xB + vxB_mpm * tcpa
    yB2 = yB + vyB_mpm * tcpa

    dist_m = math.sqrt((xA2 - xB2)**2 + (yA2 - yB2)**2)
    dist_nm = dist_m / 1852.0
    return (dist_nm, tcpa)

class CollisionDoFn(beam.DoFn):
    """
    Stateful DoFn do wykrywania kolizji w obrębie tej samej komórki geohash.
    """
    RECORDS_STATE = BagStateSpec("records_state", beam.coders.FastPrimitivesCoder())

    def process(self, element, records_state=beam.DoFn.StateParam(RECORDS_STATE)):
        gh, ship = element
        now_sec = time.time()

        # Dodaj nowy statek do stanu
        records_state.add((ship, now_sec))

        # Odczyt stanu
        old_list = list(records_state.read())
        fresh = []
        for (s, t) in old_list:
            if (now_sec - t) <= STATE_RETENTION_SEC:
                fresh.append((s, t))

        # Oczyszczamy stan i zapisujemy tylko świeże
        records_state.clear()
        for (s, t) in fresh:
            records_state.add((s, t))

        # Oblicz kolizje
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
                    "longitude_b": ship["longitude"],
                    # Ewentualnie można dodać dim_a, itp., ale tu pominę
                }

def remove_geohash(row):
    """
    Usuwamy geohash, bo nie chcemy go zapisywać w ships_positions.
    """
    new_row = dict(row)
    new_row.pop("geohash", None)
    return new_row

def is_static_data(row):
    """
    Zwraca True, jeśli w wierszu jest cokolwiek z dim_a/dim_b/dim_c/dim_d != None
    """
    return any(row.get(dim) is not None for dim in ["dim_a","dim_b","dim_c","dim_d"])

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
        # 1) Odczyt AIS z Pub/Sub
        lines = p | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=input_sub)

        # 2) Parsowanie AIS
        parsed = (
            lines
            | "ParseAIS" >> beam.Map(parse_ais)
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
        )

        # --------------------------
        # A) Zapis do ships_static
        # --------------------------
        # Filtrujemy te rekordy, które mają `dim_a` lub b,c,d != None
        static_data = parsed | "FilterStatic" >> beam.Filter(is_static_data)

        # Możesz też okienkować statyczne (np. co 60 s, bo rzadziej się zmienia)
        # Tu dla prostoty dajmy co 60 s
        windowed_static = static_data | "WindowStatic" >> beam.WindowInto(window.FixedWindows(60))
        grouped_static = (
            windowed_static
            | "KeyStatic" >> beam.Map(lambda row: (None, row))
            | "GroupStatic" >> beam.GroupByKey()
        )
        flattened_static = (
            grouped_static
            | "ExtractStatic" >> beam.FlatMap(lambda kv: kv[1])
        )

        flattened_static | "WriteStatic" >> WriteToBigQuery(
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
            additional_bq_parameters={"location": "us-east1"},
            # Możesz dać funkcję do generowania schematu dynamicznie, ale tu statycznie
        )

        # Ulepszamy: dopisz update_time = current_time, np. w Map
        def add_update_time(row):
            row2 = dict(row)
            row2["update_time"] = datetime.datetime.utcnow().isoformat() + "Z"
            return row2

        updated_static = flattened_static | "AddUpdateTime" >> beam.Map(add_update_time)
        updated_static | "WriteStatic2" >> WriteToBigQuery(
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
            additional_bq_parameters={"location": "us-east1"},
        )
        # (Mozesz spiąć w jedną transform, ale pokazuję koncepcyjnie)

        # --------------------------
        # B) Zapis do ships_positions (dane dynamiczne)
        # --------------------------
        # Usuwamy geohash, okienkujemy co 10s
        parsed_for_bq = parsed | "RemoveGeoHash" >> beam.Map(remove_geohash)

        windowed_positions = (
            parsed_for_bq
            | "WindowPositions" >> beam.WindowInto(window.FixedWindows(10))  # co 10s
        )
        grouped_positions = (
            windowed_positions
            | "KeyPositions" >> beam.Map(lambda row: (None, row))
            | "GroupPositions" >> beam.GroupByKey()
        )
        flattened_positions = (
            grouped_positions
            | "ExtractPositions" >> beam.FlatMap(lambda kv: kv[1])
        )

        # 3) Zapis do BQ
        flattened_positions | "WritePositions" >> WriteToBigQuery(
            table=table_positions,
            schema=(
                "mmsi:INTEGER,"
                "ship_name:STRING,"
                "latitude:FLOAT,"
                "longitude:FLOAT,"
                "cog:FLOAT,"
                "sog:FLOAT,"
                "timestamp:TIMESTAMP"
            ),
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            additional_bq_parameters={"location": "us-east1"},
        )

        # --------------------------
        # C) Wykrywanie kolizji + okienkowanie (10s) do collisions
        # --------------------------
        keyed = parsed | "KeyByGeohash" >> beam.Map(lambda r: (r["geohash"], r))
        collisions_raw = keyed | "DetectCollisions" >> beam.ParDo(CollisionDoFn())

        windowed_collisions = (
            collisions_raw
            | "WindowCollisions" >> beam.WindowInto(window.FixedWindows(10))
        )
        grouped_collisions = (
            windowed_collisions
            | "KeyCollisions" >> beam.Map(lambda row: (None, row))
            | "GroupCollisions" >> beam.GroupByKey()
        )
        flattened_collisions = (
            grouped_collisions
            | "ExtractCollisions" >> beam.FlatMap(lambda kv: kv[1])
        )

        flattened_collisions | "WriteCollisions" >> WriteToBigQuery(
            table=f"{table_collisions}",
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
            create_disposition=BigQueryDisposition.CREATE_NEVER,  # utwórz wcześniej
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            additional_bq_parameters={"location": "us-east1"},
        )

if __name__ == "__main__":
    run()