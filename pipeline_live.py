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
from apache_beam.transforms import trigger
from apache_beam.transforms.window import FixedWindows

CPA_THRESHOLD = 0.5         # mile morskie
TCPA_THRESHOLD = 10.0       # minuty
STATE_RETENTION_SEC = 120   # (2 min) do wykrywania kolizji

def parse_ais(record: bytes):
    """
    Parsuje rekord JSON z Pub/Sub (AIS).
    Zwraca None, jeśli brakuje wymaganych pól.
    """
    try:
        data = json.loads(record.decode("utf-8"))
        required = ["mmsi", "latitude", "longitude", "cog", "sog", "timestamp"]
        if not all(r in data for r in required):
            return None

        data["mmsi"] = int(data["mmsi"])
        data["latitude"] = float(data["latitude"])
        data["longitude"] = float(data["longitude"])
        data["cog"] = float(data["cog"])
        data["sog"] = float(data["sog"])

        # Opcjonalne pole ship_length
        ship_len = None
        if "ship_length" in data:
            try:
                ship_len = float(data["ship_length"])
            except:
                pass
        data["ship_length"] = ship_len

        # geohash – do stateful detection
        data["geohash"] = data.get("geohash", "none")

        return data
    except:
        return None

def compute_cpa_tcpa(a, b):
    """
    Oblicza (cpa, tcpa).
    Zwraca (9999, -1) jeśli statki <50m lub tcpa < 0.
    """
    try:
        la = float(a.get("ship_length") or 0)
        lb = float(b.get("ship_length") or 0)
    except:
        return (9999, -1)

    # Pomijamy statki <50m
    if la < 50 or lb < 50:
        return (9999, -1)

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
                    "ship_length_a": old_ship.get("ship_length"),
                    "ship_length_b": ship.get("ship_length")
                }

def remove_geohash(row):
    new_row = dict(row)
    new_row.pop("geohash", None)
    return new_row

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

    with beam.Pipeline(options=pipeline_options) as p:
        # 1) Odczyt AIS z Pub/Sub
        lines = p | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=input_sub)

        # 2) Parsowanie AIS
        parsed = (
            lines
            | "ParseAIS" >> beam.Map(parse_ais)
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
        )

        # ---- A) Okienkowanie (10s) danych do ships_positions ----
        # 3) Usuwamy geohash i okienkujemy co 10s
        parsed_for_bq = parsed | "RemoveGeoHash" >> beam.Map(remove_geohash)

        windowed_positions = (
            parsed_for_bq
            | "WindowPositions" >> beam.WindowInto(window.FixedWindows(10))  # co 10s
        )
        # 3a) GroupByKey, aby zebrać rekordy w każdej "partycji" (użyjemy sztucznego klucza = None)
        grouped_positions = (
            windowed_positions
            | "KeyPositions" >> beam.Map(lambda row: (None, row))
            | "GroupPositions" >> beam.GroupByKey()
        )
        # 3b) Flatten zgrupowane wartości
        flattened_positions = (
            grouped_positions
            | "ExtractPositions" >> beam.FlatMap(lambda kv: kv[1])  # kv[1] = list of rows
        )

        # 3c) Zapis do BQ (CREATE_NEVER -> musimy utworzyć tabelę 'ships_positions' wcześniej)
        flattened_positions | "WritePositions" >> WriteToBigQuery(
            table=table_positions,
            schema=(
                "mmsi:INTEGER,"
                "latitude:FLOAT,"
                "longitude:FLOAT,"
                "cog:FLOAT,"
                "sog:FLOAT,"
                "timestamp:TIMESTAMP,"
                "ship_name:STRING,"
                "ship_length:FLOAT"
            ),
            create_disposition=BigQueryDisposition.CREATE_NEVER,  # <-- zmiana
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            additional_bq_parameters={"location": "us-east1"},
        )

        # ---- B) Wykrywanie kolizji + okienkowanie (10s) do collisions ----
        keyed = parsed | "KeyByGeohash" >> beam.Map(lambda r: (r["geohash"], r))
        collisions_raw = keyed | "DetectCollisions" >> beam.ParDo(CollisionDoFn())

        # 4) Okienko 10s + group
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

        # 5) Zapis kolizji do BQ (też 10s okno)
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
                "longitude_b:FLOAT,"
                "ship_length_a:FLOAT,"
                "ship_length_b:FLOAT"
            ),
            create_disposition=BigQueryDisposition.CREATE_NEVER,  # lepiej też utworzyć wcześniej
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            additional_bq_parameters={"location": "us-east1"},
        )

        # (opcjonalnie) Publikacja do PubSub collisions-topic ...
        # ...

if __name__ == "__main__":
    run()