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
from google.api_core.exceptions import NotFound
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.transforms.userstate import BagStateSpec
import apache_beam.coders

from apache_beam import window

# Załaduj zmienne z .env
load_dotenv()

# Konfiguracja loggera
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Parametry kolizji i retencji
CPA_THRESHOLD        = 0.5   # mile morskie
TCPA_THRESHOLD       = 10.0  # minuty
STATE_RETENTION_SEC  = 120   # 2 minuty

def compute_cpa_tcpa(shipA, shipB):
    """
    Oblicza (CPA, TCPA) w milach morskich i minutach, 
    wykorzystując lokalny układ współrzędnych:
      - Pozycje lat/lon konwertowane na (x,y) w metrach,
      - SOG w węzłach przeliczane na m/min,
      - COG w stopniach (0° = North, rosnąco cw).
    
    Wymagane pola:
      shipX['latitude']  (°)
      shipX['longitude'] (°)
      shipX['cog']       (°)
      shipX['sog']       (kn)
    
    Zwraca (cpa_val, tcpa_val):
      cpa_val w milach morskich (nm),
      tcpa_val w minutach.
    Jeśli dane nie są wystarczające, lub TCPA <0, zwraca (9999, -1).
    """

    required = ['latitude','longitude','cog','sog']
    for f in required:
        if f not in shipA or f not in shipB:
            return (9999, -1)

    # Średnia szerokość geogr. jako punkt odniesienia
    latRef = (shipA['latitude'] + shipB['latitude']) / 2.0

    # Skale w metrach (przybliżenie 1° lat ~ 111 km)
    scaleLat = 111000.0
    scaleLon = 111000.0 * math.cos(math.radians(latRef))

    def toXY(lat, lon):
        # Konwersja lat/lon -> (x, y) w METRACH (lokalny układ)
        x = lon * scaleLon
        y = lat * scaleLat
        return (x, y)

    xA, yA = toXY(shipA['latitude'], shipA['longitude'])
    xB, yB = toXY(shipB['latitude'], shipB['longitude'])

    sogA = float(shipA['sog'] or 0)  # kn
    sogB = float(shipB['sog'] or 0)

    def cogToVector(cog_deg, sog_kn):
        # sog_kn (nm/h), cog_deg (stopnie)
        # vx, vy w nm/h (przed skalowaniem do m/min)
        r = math.radians(cog_deg or 0)
        vx = sog_kn * math.sin(r)
        vy = sog_kn * math.cos(r)
        return (vx, vy)

    vxA_kn, vyA_kn = cogToVector(shipA['cog'], sogA)
    vxB_kn, vyB_kn = cogToVector(shipB['cog'], sogB)

    # Pozycja względna
    dx = xA - xB  # metry
    dy = yA - yB  # metry

    # Przeliczenie prędkości z nm/h na m/min (1 nm = 1852 m, 1 h = 60 min)
    speed_scale = 1852.0 / 60.0

    dvx = (vxA_kn - vxB_kn) * speed_scale  # m/min
    dvy = (vyA_kn - vyB_kn) * speed_scale  # m/min

    VV = dvx**2 + dvy**2
    PV = dx * dvx + dy * dvy

    if VV == 0:
        # Statki "stoją" względem siebie w tym modelu
        tcpa = 0.0
    else:
        tcpa = -PV / VV

    if tcpa < 0:
        return (9999, -1)

    # Pozycje przy CPA
    xA2 = xA + vxA_kn * speed_scale * tcpa
    yA2 = yA + vyA_kn * speed_scale * tcpa
    xB2 = xB + vxB_kn * speed_scale * tcpa
    yB2 = yB + vyB_kn * speed_scale * tcpa

    dist_m = math.sqrt((xA2 - xB2)**2 + (yA2 - yB2)**2)
    dist_nm = dist_m / 1852.0

    return (dist_nm, tcpa)

def parse_ais(record_bytes):
    """Parsuje JSON AIS z Pub/Sub."""
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

        # geohash
        data["geohash"]   = data.get("geohash","none")

        return data
    except:
        return None

def is_ship_long_enough(ship):
    """
    Zwraca True, jeśli statek ma dim_a i dim_b (nie None),
    a ich suma > 50. W przeciwnym razie False.
    """
    dim_a = ship.get("dim_a")
    dim_b = ship.get("dim_b")
    if dim_a is None or dim_b is None:
        return False
    return (dim_a + dim_b) > 50

class CollisionDoFn(beam.DoFn):
    """
    Stateful DoFn do wykrywania kolizji w ramach geohash.
    Tutaj odrzucamy kolizje, jeśli cpa >= 0.5 nm lub tcpa >= 10 min.
    """
    RECORDS_STATE = BagStateSpec("records_state", beam.coders.TupleCoder((
        beam.coders.FastPrimitivesCoder(),
        beam.coders.FastPrimitivesCoder()
    )))

    def process(self, element, records_state=beam.DoFn.StateParam(RECORDS_STATE)):
        gh, ship = element
        now_sec = time.time()

        # Zapis do stanu
        records_state.add((ship, now_sec))

        # Czytamy dotychczasowe dane i czyścimy stare
        old_list = list(records_state.read())
        fresh = [(s,t) for (s,t) in old_list if (now_sec - t)<=STATE_RETENTION_SEC]

        records_state.clear()
        for (s,t) in fresh:
            records_state.add((s,t))

        # Wykrycie kolizji
        for (old_ship, _) in fresh:
            if old_ship["mmsi"] == ship["mmsi"]:
                continue

            cpa, tcpa = compute_cpa_tcpa(old_ship, ship)
            # Filtr: cpa < 0.5 i 0 <= tcpa < 10
            if cpa < CPA_THRESHOLD and 0 <= tcpa < TCPA_THRESHOLD:
                logging.info(f"Collision detected between {old_ship['mmsi']} & {ship['mmsi']} cpa={cpa}, tcpa={tcpa}")
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
    """Usunięcie geohash i dim_* z docelowego wiersza do ships_positions."""
    new_row = dict(row)
    new_row.pop("geohash", None)
    for d in ["dim_a","dim_b","dim_c","dim_d"]:
        new_row.pop(d, None)
    return new_row

class DeduplicateStaticDoFn(beam.DoFn):
    """
    Naiwna deduplikacja w pamięci (cache w worker).
    Jeśli autoscaling >1, to może nieco przepuszczać duplikaty.
    """
    def __init__(self):
        self.seen = set()

    def process(self, row):
        mmsi = row["mmsi"]
        if mmsi not in self.seen:
            self.seen.add(mmsi)
            yield row

def keep_static_fields(row):
    """Zostawiamy tylko statyczne kolumny + update_time."""
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
    """
    def process(self, table_ref):
        client_local = bigquery.Client()

        table_id = table_ref['table_id']
        schema    = table_ref['schema']['fields']
        time_part = table_ref.get('time_partitioning')
        cluster   = table_ref.get('clustering_fields')

        table = bigquery.Table(table_id, schema=schema)

        if time_part:
            time_part_corrected = {k if k != 'type' else 'type_': v for k, v in time_part.items()}
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

    # Ustawiamy retencję (expiration_ms) na 24h = 86400000 ms w ships_positions i collisions
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
                # retencja 24h (86400000 ms)
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
                # retencja 24h (86400000 ms)
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
            # Brak retencji w ships_static
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
        # 1) Tworzymy tabele, o ile nie istnieją
        _ = (
            tables_to_create
            | "CreateTables" >> beam.ParDo(CreateBQTableDoFn())
        )

        # 2) Odczyt z PubSub i parse
        lines = p | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=input_sub)
        parsed = (
            lines
            | "ParseAIS"   >> beam.Map(parse_ais)
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
        )

        # 3) ships_positions (co 10s)
        w_pos = (
            parsed
            | "WinPositions"    >> beam.WindowInto(window.FixedWindows(10))
            | "KeyPositions"    >> beam.Map(lambda r: (None, r))
            | "GroupPositions"  >> beam.GroupByKey()
            | "FlatPositions"   >> beam.FlatMap(lambda kv: kv[1])
            | "RmGeohashDims"   >> beam.Map(remove_geohash_and_dims)
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

        # 4) ships_static (deduplicate in-memory) z oknami 5 minutowymi
        ships_static = (
            parsed
            | "FilterDims" >> beam.Filter(
                lambda r: any(r.get(dim) for dim in ["dim_a","dim_b","dim_c","dim_d"])
            )
            | "WindowStatic" >> beam.WindowInto(window.FixedWindows(300))  # 5 minut
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
        #  - Dodajemy dodatkowy krok "FilterShortShips" -> is_ship_long_enough
        #    aby NIE przekazywać do CollisionDoFn statków krótszych niż 50m lub z brakiem wymiarów
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