#!/usr/bin/env python3
import os
import json
import math
import time
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.transforms.userstate import BagStateSpec
import apache_beam.coders

CPA_THRESHOLD = 0.5
TCPA_THRESHOLD = 10.0
STATE_RETENTION_SEC = 30

def parse_ais(record: bytes):
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
        if "ship_length" in data:
            try:
                data["ship_length"] = float(data["ship_length"])
            except:
                data["ship_length"] = None
        else:
            data["ship_length"] = None

        if "geohash" not in data:
            data["geohash"] = "none"

        return data
    except:
        return None

def compute_cpa_tcpa(a, b):
    try:
        la = a.get("ship_length") or 0
        lb = b.get("ship_length") or 0
        la, lb = float(la), float(lb)
    except:
        return (9999, -1)
    if la < 50 or lb < 50:
        return (9999, -1)

    latRef = (a["latitude"] + b["latitude"]) / 2
    scaleLat = 111000
    scaleLon = 111000 * math.cos(math.radians(latRef))

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
    speed_scale = 1852 / 60
    dvx_mpm = (vxA - vxB) * speed_scale
    dvy_mpm = (vyA - vyB) * speed_scale

    VV = dvx_mpm**2 + dvy_mpm**2
    PV = dx*dvx_mpm + dy*dvy_mpm
    if VV == 0:
        tcpa = 0.0
    else:
        tcpa = - PV / VV

    if tcpa < 0:
        return (9999, -1)

    vxA_mpm = vxA * speed_scale
    vyA_mpm = vyA * speed_scale
    vxB_mpm = vxB * speed_scale
    vyB_mpm = vyB * speed_scale

    xA2 = xA + vxA_mpm * tcpa
    yA2 = yA + vyA_mpm * tcpa
    xB2 = xB + vxB_mpm * tcpa
    yB2 = yB + vyB_mpm * tcpa

    dist_m = math.sqrt((xA2 - xB2)**2 + (yA2 - yB2)**2)
    dist_nm = dist_m / 1852
    return (dist_nm, tcpa)

class CollisionDoFn(beam.DoFn):
    RECORDS_STATE = BagStateSpec("records_state", beam.coders.FastPrimitivesCoder())
    def process(self, element, records_state=beam.DoFn.StateParam(RECORDS_STATE)):
        gh, ship = element
        now_sec = time.time()

        # dopisz
        records_state.add((ship, now_sec))

        # odczyt
        old = list(records_state.read())
        fresh = []
        for (s, t) in old:
            if (now_sec - t) <= STATE_RETENTION_SEC:
                fresh.append((s, t))

        # wyczysc i zapisz fresh
        records_state.clear()
        for (s, t) in fresh:
            records_state.add((s, t))

        # oblicz kolizje
        for (old_ship, _) in fresh:
            if old_ship["mmsi"] == ship["mmsi"]:
                continue
            cpa, tcpa = compute_cpa_tcpa(old_ship, ship)
            if cpa < CPA_THRESHOLD and 0 <= tcpa < TCPA_THRESHOLD:
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

def run():
    logging.getLogger().setLevel(logging.INFO)

    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True

    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "ais-collision-detection")
    dataset = os.getenv("LIVE_DATASET", "ais_dataset_us")
    input_sub = os.getenv("INPUT_SUBSCRIPTION", "projects/ais-collision-detection/subscriptions/ais-data-sub")

    table_positions = f"{project_id}:{dataset}.ships_positions"
    table_collisions = f"{project_id}:{dataset}.collisions"

    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=input_sub)

        # sprawdz czy w logach cokolwiek przychodzi
        _ = (
            lines
            | "LOG_lines" >> beam.Map(lambda row: logging.info(f"Raw PubSub: {row}"))
        )

        parsed = (
            lines
            | "ParseAIS" >> beam.Map(parse_ais)
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
        )
        # sprawdz co jest w parsed
        _ = (
            parsed
            | "LOG_parsed" >> beam.Map(lambda row: logging.info(f"Parsed: {row}"))
        )

        parsed | "WritePositions" >> WriteToBigQuery(
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
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            additional_bq_parameters={"location": "us-east1"},
            custom_gcs_temp_location="gs://ais-collision-detection-bucket/temp_bq"
        )

        keyed = parsed | "KeyByGH" >> beam.Map(lambda r: (r["geohash"], r))
        collisions = keyed | "Collisions" >> beam.ParDo(CollisionDoFn())

        _ = (
            collisions
            | "LOG_collisions" >> beam.Map(lambda c: logging.info(f"Collision: {c}"))
        )

        collisions | "WriteCollisions" >> WriteToBigQuery(
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
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            additional_bq_parameters={"location": "us-east1"},
            custom_gcs_temp_location="gs://ais-collision-detection-bucket/temp_bq"
        )

if __name__ == "__main__":
    run()