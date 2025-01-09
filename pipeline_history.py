#!/usr/bin/env python3
import os
import json
import math
import datetime
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from google.cloud import bigquery

# Klasy/fileio niezbędne do zapisu do jednego pliku.
from apache_beam.io.fileio import WriteToFiles, FileSink

# ------------------------------------------------------------
# Funkcja pomocnicza: Prosta interpolacja klatek
# ------------------------------------------------------------
def interpolate_positions(records, step_seconds=60):
    """
    records: lista krotek (ts, lat, lon, sog, cog),
             posortowana rosnąco po ts.
    step_seconds: co ile sekund generujemy klatkę.

    Zwraca listę (ts, lat, lon, sog, cog).
    """
    if not records:
        return []

    # Sortujemy rosnąco po czasie
    records.sort(key=lambda x: x[0])
    start_t = records[0][0]
    end_t   = records[-1][0]

    out = []
    current_t = start_t
    idx = 0

    while current_t <= end_t:
        # Znajdujemy segment [records[idx], records[idx+1]], w którym mieści się current_t
        while idx < len(records) - 1 and records[idx+1][0] < current_t:
            idx += 1

        if idx == len(records) - 1:
            # koniec listy
            t_last, la, lo, sg, cg = records[-1]
            out.append((current_t, la, lo, sg, cg))
        else:
            tA, laA, loA, sgA, cgA = records[idx]
            tB, laB, loB, sgB, cgB = records[idx+1]
            if current_t < tA:
                # jeszcze przed tA → bierzemy wartości z tA
                out.append((current_t, laA, loA, sgA, cgA))
            else:
                dt = (tB - tA).total_seconds()
                if dt <= 0:
                    # w razie błędu czasowego
                    out.append((current_t, laA, loA, sgA, cgA))
                else:
                    ratio = (current_t - tA).total_seconds() / dt
                    lat_i = laA + (laB - laA) * ratio
                    lon_i = loA + (loB - loA) * ratio
                    sog_i = sgA + (sgB - sgA) * ratio
                    cog_i = cgA + (cgB - cgA) * ratio
                    out.append((current_t, lat_i, lon_i, sog_i, cog_i))

        current_t += datetime.timedelta(seconds=step_seconds)

    return out


# ------------------------------------------------------------
# DoFn przetwarzający jedną kolizję w "animację"
# ------------------------------------------------------------
class ProcessCollisionsFn(beam.DoFn):
    def setup(self):
        self.bq_client = bigquery.Client()

    def process(self, collision_row):
        """
        collision_row to dict z BQ, np.:
        {
          'collision_id': "...",
          'mmsi_a': 123,
          'mmsi_b': 456,
          'timestamp': datetime,
          'cpa': 0.23,
          'tcpa': 5.0,
          ...
        }
        """
        collision_id = collision_row['collision_id']
        mmsi_a       = collision_row['mmsi_a']
        mmsi_b       = collision_row['mmsi_b']
        cpa_val      = collision_row['cpa']
        tcpa_val     = collision_row['tcpa']
        ts_coll      = collision_row['timestamp']  # obiekt datetime

        # Filtr — chcemy faktyczne kolizje
        if cpa_val is None or cpa_val >= 0.5:
            return
        if tcpa_val is None or tcpa_val > 10 or tcpa_val < 0:
            return

        # Zakres animacji: [T - 20 min, T + 5 min]
        coll_time = ts_coll
        start_t   = coll_time - datetime.timedelta(minutes=20)
        end_t     = coll_time + datetime.timedelta(minutes=5)

        # Zapytanie do BQ
        query = f"""
        SELECT
          mmsi,
          timestamp,
          latitude,
          longitude,
          sog,
          cog,
          ANY_VALUE(ship_name) AS ship_name,
          ANY_VALUE(ship_length) AS ship_length
        FROM `ais_dataset_us.ships_positions`
        WHERE mmsi IN ({mmsi_a}, {mmsi_b})
          AND timestamp BETWEEN '{start_t.isoformat()}' AND '{end_t.isoformat()}'
        GROUP BY mmsi, timestamp, latitude, longitude, sog, cog
        ORDER BY timestamp
        """

        rows = list(self.bq_client.query(query).result())

        data_a = []
        data_b = []

        for r in rows:
            t  = r.timestamp
            la = float(r.latitude or 0.0)
            lo = float(r.longitude or 0.0)
            sg = float(r.sog or 0.0)
            cg = float(r.cog or 0.0)
            nm = r.ship_name
            ln = r.ship_length

            if r.mmsi == mmsi_a:
                data_a.append((t, la, lo, sg, cg, nm, ln))
            else:
                data_b.append((t, la, lo, sg, cg, nm, ln))

        # Stripped do interpolacji
        def stripped(x):
            return (x[0], x[1], x[2], x[3], x[4])  # (ts, lat, lon, sog, cog)

        data_a_str = [stripped(x) for x in data_a]
        data_b_str = [stripped(x) for x in data_b]

        interp_a = interpolate_positions(data_a_str, 60)
        interp_b = interpolate_positions(data_b_str, 60)

        # Nazwa i długość z ostatniego wiersza
        name_a = data_a[-1][5] if data_a else None
        len_a  = data_a[-1][6] if data_a else None
        name_b = data_b[-1][5] if data_b else None
        len_b  = data_b[-1][6] if data_b else None

        from collections import defaultdict
        time_map = defaultdict(list)

        def add_ships(interp_list, mmsi_val, name_val, length_val):
            for (ts, la, lo, sg, cg) in interp_list:
                key_ts = ts.replace(microsecond=0)
                time_map[key_ts].append({
                    "mmsi": mmsi_val,
                    "lat": la,
                    "lon": lo,
                    "sog": sg,
                    "cog": cg,
                    "name": name_val or str(mmsi_val),
                    "ship_length": length_val or 0
                })

        add_ships(interp_a, mmsi_a, name_a, len_a)
        add_ships(interp_b, mmsi_b, name_b, len_b)

        frames = []
        for tstamp in sorted(time_map.keys()):
            frames.append({
                "time": tstamp.isoformat(),
                "shipPositions": time_map[tstamp]
            })

        # Zwracamy finalny obiekt kolizji z klatkami
        collision_obj = {
            "collision_id": collision_id,
            "mmsi_a": mmsi_a,
            "mmsi_b": mmsi_b,
            "cpa": cpa_val,
            "tcpa": tcpa_val,
            "timestamp": ts_coll.isoformat() if ts_coll else None,
            "frames": frames
        }
        yield collision_obj


# ------------------------------------------------------------
# Funkcja łącząca listę kolizji w jeden JSON
# ------------------------------------------------------------
def combine_collisions_into_single_json(items):
    """
    items jest listą obiektów (collision_obj).
    """
    arr = list(items)
    result = {
        "collisions": arr
    }
    return json.dumps(result, indent=2, default=str)


# ------------------------------------------------------------
# Klasa SingleJSONSink
# ------------------------------------------------------------
class SingleJSONSink(FileSink):
    """
    FileSink, który przyjmuje element (string) i zapisuje do jednego pliku.
    Beam w nowszych wersjach wywołuje sink.write(element).
    """
    def open(self, fh):
        self._fh = fh

    def write(self, element):
        # element jest stringiem (JSON)
        self._fh.write(element.encode("utf-8"))
        self._fh.write(b"\n")

    def flush(self):
        pass


def run():
    logging.getLogger().setLevel(logging.INFO)

    # Ustawienia potoku
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = False

    # Parametry z ENV lub wartości domyślne
    lookback_minutes = int(os.getenv('LOOKBACK_MINUTES', '60'))
    output_prefix = os.getenv(
        'HISTORY_OUTPUT_PREFIX',
        'gs://ais-collision-detection-bucket/history_collisions/hourly'
    )

    # Czas i warunek
    now = datetime.datetime.utcnow()
    start_time = now - datetime.timedelta(minutes=lookback_minutes)

    # Zapytanie do BQ (pole collisions)
    query = f"""
    SELECT
      CONCAT(CAST(mmsi_a AS STRING), '_',
             CAST(mmsi_b AS STRING), '_',
             FORMAT_TIMESTAMP('%Y%m%d%H%M%S', timestamp)) AS collision_id,
      mmsi_a,
      mmsi_b,
      timestamp,
      cpa,
      tcpa
    FROM `ais_dataset_us.collisions`
    WHERE timestamp >= TIMESTAMP('{start_time.isoformat()}')
      AND cpa < 0.5
      AND tcpa >= 0
      AND tcpa <= 10
    ORDER BY timestamp
    """

    # Nazwa wynikowego pliku
    timestamp_str = now.strftime("%Y%m%d%H%M%S")
    filename = f"{output_prefix}/collisions_{timestamp_str}.json"

    with beam.Pipeline(options=pipeline_options) as p:

        # 1) Odczyt kolizji z BQ
        collisions = (
            p
            | "ReadCollisionsBQ" >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
        )

        # 2) Procesowanie kolizji → klatki animacji
        expanded = collisions | "ProcessCollisions" >> beam.ParDo(ProcessCollisionsFn())

        # 3) Grupujemy do jednej listy (wszystkie kolizje w potoku)
        collisions_list = expanded | "ToList" >> beam.combiners.ToList()

        # 4) Jedno duże JSON
        single_json_str = collisions_list | "ToSingleJson" >> beam.Map(
            combine_collisions_into_single_json
        )

        # 5) Zapis do pojedynczego pliku
        (
            single_json_str
            | "WriteSingleFile" >> WriteToFiles(
                path=filename,
                max_writers_per_bundle=1,
                sink=SingleJSONSink()
            )
        )


if __name__ == "__main__":
    run()