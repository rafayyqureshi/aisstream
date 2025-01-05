#!/usr/bin/env python3
import os
import json
import math
import time

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.transforms.userstate import BagStateSpec
import apache_beam.coders

# Domyślne progi kolizji
CPA_THRESHOLD = 0.5
TCPA_THRESHOLD = 10.0
STATE_RETENTION_SEC = 30

def parse_ais(record: bytes):
    try:
        data = json.loads(record.decode("utf-8"))
        required = ["mmsi", "latitude", "longitude", "cog", "sog", "timestamp"]
        if not all(k in data for k in required):
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

def compute_cpa_tcpa(ship_a, ship_b):
    try:
        la = float(ship_a.get("ship_length") or 0)
        lb = float(ship_b.get("ship_length") or 0)
    except:
        return (9999, -1)

    if la < 50 or lb < 50:
        return (9999, -1)

    latRef = (ship_a["latitude"] + ship_b["latitude"]) / 2.0
    scaleLat = 111000.0
    scaleLon = 111000.0 * math.cos(math.radians(latRef))

    def to_xy(lat, lon):
        return (lon * scaleLon, lat * scaleLat)

    xA, yA = to_xy(ship_a["latitude"], ship_a["longitude"])
    xB, yB = to_xy(ship_b["latitude"], ship_b["longitude"])

    sogA = float(ship_a["sog"] or 0)
    sogB = float(ship_b["sog"] or 0)

    def cog_to_vec(cog_deg, sog_nm_h):
        r = math.radians(cog_deg)
        return (
            sog_nm_h * math.sin(r),
            sog_nm_h * math.cos(r)
        )

    vxA_nm, vyA_nm = cog_to_vec(ship_a["cog"], sogA)
    vxB_nm, vyB_nm = cog_to_vec(ship_b["cog"], sogB)

    dx = xA - xB
    dy = yA - yB

    speed_scale = 1852.0 / 60.0
    dvx_mpm = (vxA_nm - vxB_nm) * speed_scale
    dvy_mpm = (vyA_nm - vyB_nm) * speed_scale

    VV_m = dvx_mpm**2 + dvy_mpm**2
    PV_m = dx * dvx_mpm + dy * dvy_mpm
    if VV_m == 0:
        tcpa = 0.0
    else:
        tcpa = - PV_m / VV_m

    if tcpa < 0:
        return (9999, -1)

    vxA_mpm = vxA_nm * speed_scale
    vyA_mpm = vyA_nm * speed_scale
    vxB_mpm = vxB_nm * speed_scale
    vyB_mpm = vyB_nm * speed_scale

    xA2 = xA + vxA_mpm * tcpa
    yA2 = yA + vyA_mpm * tcpa
    xB2 = xB + vxB_mpm * tcpa
    yB2 = yB + vyB_mpm * tcpa

    dist_m = math.sqrt((xA2 - xB2)**2 + (yA2 - yB2)**2)
    dist_nm = dist_m / 1852.0
    return (dist_nm, tcpa)

class CollisionDoFn(beam.DoFn):
    RECORDS_STATE = BagStateSpec("records_state", beam.coders.FastPrimitivesCoder())

    def process(self, element, records_state=beam.DoFn.StateParam(RECORDS_STATE)):
        geohash, ship = element
        now_sec = time.time()

        records_state.add((ship, now_sec))

        old_records = list(records_state.read())
        fresh = []
        for (s, t) in old_records:
            if (now_sec - t) <= STATE_RETENTION_SEC:
                fresh.append((s, t))

        records_state.clear()
        for (s, t) in fresh:
            records_state.add((s, t))

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
                    "longitude_b": ship["longitude"],
                }

def run():
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True

    # Z .env / Makefile
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "ais-collision-detection")
    dataset = os.getenv("LIVE_DATASET", "ais_dataset_us")
    input_subscription = os.getenv("INPUT_SUBSCRIPTION", "projects/ais-collision-detection/subscriptions/ais-data-sub")
    # collisions_topic = os.getenv("COLLISIONS_TOPIC", "projects/ais-collision-detection/topics/collisions-topic")

    table_positions = f"{project_id}:{dataset}.ships_positions"
    table_collisions = f"{project_id}:{dataset}.collisions"

    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=input_subscription)

        parsed = (
            lines
            | "ParseAIS" >> beam.Map(parse_ais)
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
        )

        # Zapis do BQ
        _ = (
            parsed
            | "WritePositions" >> WriteToBigQuery(
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
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,

                # DODATKOWE PARAMETRY:
                # Zapewniamy location = "us-east1"
                additional_bq_parameters={"location": "us-east1"},
            )
        )

        keyed = parsed | "KeyByGeohash" >> beam.Map(lambda r: (r["geohash"], r))
        collisions = keyed | "DetectCollisions" >> beam.ParDo(CollisionDoFn())

        _ = (
            collisions
            | "WriteCollisions" >> WriteToBigQuery(
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
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,

                # Kluczowa linijka:
                additional_bq_parameters={"location": "us-east1"},
            )
        )

        # Ewentualnie publikacja do Pub/Sub collisions_topic
        # collisions_str = collisions | beam.Map(lambda x: json.dumps(x).encode("utf-8"))
        # collisions_str | beam.io.WriteToPubSub(topic=collisions_topic)

if __name__ == "__main__":
    run()