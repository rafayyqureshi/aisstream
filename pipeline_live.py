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
    Zwraca None, jeśli brakuje required dynamic fields
    (mmsi, lat, lon, cog, sog, timestamp).
    
    Dodatkowo: 
    - usuwa "ship_length" jeśli istnieje 
    - konwertuje dim_a..d na float
    """
    try:
        data = json.loads(record.decode("utf-8"))
        
        # Pola potrzebne do dynamicznego zapisu
        required = ["mmsi", "latitude", "longitude", "cog", "sog", "timestamp"]
        if not all(r in data for r in required):
            return None

        # Rzutowania
        data["mmsi"] = int(data["mmsi"])
        data["latitude"] = float(data["latitude"])
        data["longitude"] = float(data["longitude"])
        data["cog"] = float(data["cog"])
        data["sog"] = float(data["sog"])

        # Wyrzucamy pole ship_length, bo nie ma go w nowym schemacie
        if "ship_length" in data:
            data.pop("ship_length", None)

        # Dim_a..d
        for dim in ["dim_a","dim_b","dim_c","dim_d"]:
            if dim in data:
                try:
                    data[dim] = float(data[dim])
                except:
                    data[dim] = None
            else:
                data[dim] = None

        # ship_name
        data["ship_name"] = data.get("ship_name", "Unknown")

        # geohash – do stateful detection
        data["geohash"] = data.get("geohash", "none")

        return data
    except:
        return None

def filter_old_messages_that_have_only_ship_length(data):
    """
    Jeżeli to stary format, który miał TYLKO ship_length i brak dim_x,
    to ignorujemy. 
    (Możesz też przepuszczać, ale i tak w BQ będzie błąd).
    """
    # Jesli dim_a..d są None i występuje "ship_length" => to stara wiadomość
    # Zwracamy False => odrzuć
    if data["dim_a"] is None and data["dim_b"] is None and data["dim_c"] is None and data["dim_d"] is None:
        # To oznacza brak nowych wymiarów
        # (Jeśli chcesz je przepuszczać do ships_positions, usuń warunek)
        # W tym przykładzie odrzucamy
        return False
    return True

def compute_cpa_tcpa(a, b):
    """
    Oblicza (cpa, tcpa) na podstawie punktów anteny (lat,lon).
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
                    "longitude_b": ship["longitude"]
                }

def remove_geohash(row):
    """
    Usuwamy geohash, bo nie chcemy go zapisywać w ships_positions.
    Usuwamy też dim_a..d przy zapisie do positions, 
    bo tamte kolumny nie istnieją.
    """
    new_row = dict(row)
    new_row.pop("geohash", None)
    for dim in ["dim_a","dim_b","dim_c","dim_d"]:
        if dim in new_row:
            new_row.pop(dim, None)
    return new_row

def keep_static_fields_only(row):
    """
    Do ships_static -> zatrzymujemy TYLKO mmsi, ship_name, dim_a,b,c,d + update_time
    """
    return {
        "mmsi": row["mmsi"],
        "ship_name": row["ship_name"],
        "dim_a": row["dim_a"],
        "dim_b": row["dim_b"],
        "dim_c": row["dim_c"],
        "dim_d": row["dim_d"],
        # Dołączmy currentTime 
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
        # 1) Odczyt AIS z Pub/Sub
        lines = p | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=input_sub)

        # 2) Parsowanie AIS
        parsed = (
            lines
            | "ParseAIS" >> beam.Map(parse_ais)
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
        )

        # 2b) (Opcjonalnie) odfiltrować stare wiadomości z ship_length (bo i tak nie pasują do nowego schematu).
        #    Gdy chcesz całkiem je odrzucić:
        # parsed_filtered = parsed | "FilterStare" >> beam.Filter(
        #     lambda row: "ship_length" not in row or row["ship_length"] is None
        # )

        # ----------------------------------------------------------------
        # A) ships_static -> te rekordy, które mają cokolwiek w dim_a..d
        # ----------------------------------------------------------------
        static_data = parsed | "FilterStatic" >> beam.Filter(
            lambda row: any(row.get(dim) is not None for dim in ["dim_a","dim_b","dim_c","dim_d"])
        )

        # Okno co 60 s
        windowed_static = static_data | "WindowStatic" >> beam.WindowInto(window.FixedWindows(60))
        grouped_static = (
            windowed_static
            | "KeyStatic" >> beam.Map(lambda r: (None, r))
            | "GroupStatic" >> beam.GroupByKey()
        )
        flattened_static = (
            grouped_static
            | "ExtractStatic" >> beam.FlatMap(lambda kv: kv[1])
        )
        # Ucinamy tylko potrzebne pola (mmsi, ship_name, dim_a..d, update_time)
        prepped_static = flattened_static | "KeepStaticFields" >> beam.Map(keep_static_fields_only)

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
            additional_bq_parameters={"location": "us-east1"},
        )

        # ----------------------------------------------------------------
        # B) ships_positions -> dane dynamiczne
        # ----------------------------------------------------------------
        # Usuwamy geohash i dim_a..d, bo w tej tabeli nie ma takich kolumn
        cleaned_for_positions = parsed | "RemoveGeohashAndDims" >> beam.Map(remove_geohash)

        # Okienkowanie co 10 s
        windowed_positions = (
            cleaned_for_positions
            | "WindowPositions" >> beam.WindowInto(window.FixedWindows(10))
        )
        grouped_positions = (
            windowed_positions
            | "KeyPositions" >> beam.Map(lambda r: (None, r))
            | "GroupPositions" >> beam.GroupByKey()
        )
        flattened_positions = (
            grouped_positions
            | "ExtractPositions" >> beam.FlatMap(lambda kv: kv[1])
        )

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

        # ----------------------------------------------------------------
        # C) collisions -> analogicznie
        # ----------------------------------------------------------------
        # Wykrywanie kolizji
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
            additional_bq_parameters={"location": "us-east1"},
        )

if __name__ == "__main__":
    run()