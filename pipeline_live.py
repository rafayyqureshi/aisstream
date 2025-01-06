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

# ----------------------------
# Parametry kolizji
# ----------------------------
CPA_THRESHOLD = 0.5       # mile morskie
TCPA_THRESHOLD = 10.0     # minuty
STATE_RETENTION_SEC = 30  # ile sekund pamiętamy statki w danym geohash

def parse_ais(record: bytes):
    """
    Parsuje rekord JSON z Pub/Sub (AIS).
    Zwraca None, jeśli brakuje wymaganych pól (mmsi, lat, lon, sog, cog, timestamp).
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

        # Opcjonalna długość:
        if "ship_length" in data:
            try:
                data["ship_length"] = float(data["ship_length"])
            except:
                data["ship_length"] = None
        else:
            data["ship_length"] = None

        # geohash – tylko do stateful detection
        data["geohash"] = data.get("geohash", "none")

        # (opcjonalnie) nazwa statku – data.get("ship_name")
        # nic nie zmieniamy, jeśli brak
        return data
    except:
        return None

def compute_cpa_tcpa(ship_a, ship_b):
    """
    Oblicza (cpa, tcpa) bazując na sog/cog i prostym przybliżeniu geograficznym.
    Zwraca (9999, -1), jeśli statki <50m lub tcpa<0.
    """
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
        vx = sog_nm_h * math.sin(r)
        vy = sog_nm_h * math.cos(r)
        return (vx, vy)

    vxA, vyA = cog_to_vec(ship_a["cog"], sogA)
    vxB, vyB = cog_to_vec(ship_b["cog"], sogB)

    dx = xA - xB
    dy = yA - yB

    speed_scale = 1852.0 / 60.0  # nm/h => m/min
    dvx_mpm = (vxA - vxB) * speed_scale
    dvy_mpm = (vyA - vyB) * speed_scale

    VV = dvx_mpm**2 + dvy_mpm**2
    PV = dx*dvx_mpm + dy*dvy_mpm

    if VV == 0:
        tcpa = 0.0
    else:
        tcpa = -PV / VV

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
    dist_nm = dist_m / 1852.0
    return (dist_nm, tcpa)

class CollisionDoFn(beam.DoFn):
    """
    Stateful DoFn, wykrywanie kolizji w obrębie tego samego geohasha.
    """
    RECORDS_STATE = BagStateSpec("records_state", beam.coders.FastPrimitivesCoder())

    def process(self, element, records_state=beam.DoFn.StateParam(RECORDS_STATE)):
        geohash, ship = element
        now_sec = time.time()

        # Zapisz do stanu
        records_state.add((ship, now_sec))

        # Wczytaj dotychczasowy stan
        old_list = list(records_state.read())
        fresh = []
        for (s, t) in old_list:
            if (now_sec - t) <= STATE_RETENTION_SEC:
                fresh.append((s, t))

        # Czyścimy i zapisujemy z powrotem jedynie fresh
        records_state.clear()
        for (s, t) in fresh:
            records_state.add((s, t))

        # Weryfikacja kolizji
        for (old_ship, _) in fresh:
            if old_ship["mmsi"] == ship["mmsi"]:
                continue

            cpa, tcpa = compute_cpa_tcpa(old_ship, ship)
            if cpa < CPA_THRESHOLD and 0 <= tcpa < TCPA_THRESHOLD:
                # Dodajemy ship_length_a i ship_length_b do collisions:
                yield {
                    "mmsi_a": old_ship["mmsi"],
                    "mmsi_b": ship["mmsi"],
                    "timestamp": ship["timestamp"],  # "nowszy" timestamp
                    "cpa": cpa,
                    "tcpa": tcpa,
                    "latitude_a": old_ship["latitude"],
                    "longitude_a": old_ship["longitude"],
                    "latitude_b": ship["latitude"],
                    "longitude_b": ship["longitude"],
                    "ship_length_a": old_ship.get("ship_length"),
                    "ship_length_b": ship.get("ship_length")
                    # ewentualnie: "ship_name_a": old_ship.get("ship_name"),
                    #              "ship_name_b": ship.get("ship_name"),
                }

def remove_geohash(row):
    # Pomocnicza funkcja usuwająca geohash przed zapisem do ships_positions
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
        # 1) Odczyt z PubSub
        lines = p | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=input_sub)

        # 2) Parsowanie JSON → dict
        parsed = (
            lines
            | "ParseAIS" >> beam.Map(parse_ais)
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
        )

        # 3) Zapis statków do BQ (bez geohash)
        parsed_for_bq = parsed | "RemoveGeoHash" >> beam.Map(remove_geohash)
        parsed_for_bq | "WritePositions" >> WriteToBigQuery(
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

        # 4) Detekcja kolizji
        keyed = parsed | "KeyByGeohash" >> beam.Map(lambda r: (r["geohash"], r))
        collisions = keyed | "DetectCollisions" >> beam.ParDo(CollisionDoFn())

        # 5) Zapis kolizji do BQ wraz z ship_length_a, ship_length_b
        collisions | "WriteCollisions" >> WriteToBigQuery(
            table=table_collisions,
            # Dodajemy kolumny ship_length_a i ship_length_b
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
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            additional_bq_parameters={"location": "us-east1"},
            custom_gcs_temp_location="gs://ais-collision-detection-bucket/temp_bq"
        )

        # (opcjonalnie) Publikacja do PubSub
        # collisions_str = collisions | "CollisionsToJson" >> beam.Map(lambda c: json.dumps(c).encode("utf-8"))
        # collisions_str | "PubCollisions" >> beam.io.WriteToPubSub(topic=...)
        # ...

if __name__ == "__main__":
    run()