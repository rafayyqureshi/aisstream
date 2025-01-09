#!/usr/bin/env python3
import os
import json
import math
import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from google.cloud import bigquery
from apache_beam.transforms import window
from apache_beam.io.fileio import WriteToFiles, FileSink

# ========================================
# Funkcja pomocnicza do interpolacji klatek
# ========================================
def interpolate_positions(records, step_seconds=60):
    """
    records: lista krotek (ts, lat, lon, sog, cog), posortowana po ts (datetime).
    step_seconds: co ile sekund generujemy klatkę (domyślnie 60s).
    Zwraca listę (ts, lat, lon, sog, cog).
    """
    if not records:
        return []

    records.sort(key=lambda x: x[0])  # sortowanie rosnąco po timestamp
    start_t = records[0][0]
    end_t   = records[-1][0]

    out = []
    current_t = start_t
    idx = 0

    while current_t <= end_t:
        # znajdź segment [records[idx], records[idx+1]] zawierający current_t
        while idx < len(records) - 1 and records[idx+1][0] < current_t:
            idx += 1

        if idx == len(records) - 1:
            # jesteśmy na końcu
            t_last, la, lo, sg, cg = records[-1]
            out.append((current_t, la, lo, sg, cg))
        else:
            tA, laA, loA, sgA, cgA = records[idx]
            tB, laB, loB, sgB, cgB = records[idx+1]

            if current_t < tA:
                # jeszcze przed tA → bierzemy tA
                out.append((current_t, laA, loA, sgA, cgA))
            else:
                dt = (tB - tA).total_seconds()
                if dt <= 0:
                    out.append((current_t, laA, loA, sgA, cgA))
                else:
                    ratio = (current_t - tA).total_seconds() / dt
                    lat_i = laA + (laB - laA)*ratio
                    lon_i = loA + (loB - loA)*ratio
                    sog_i = sgA + (sgB - sgA)*ratio
                    cog_i = cgA + (cgB - cgA)*ratio
                    out.append((current_t, lat_i, lon_i, sog_i, cg_i))

        current_t += datetime.timedelta(seconds=step_seconds)

    return out

# ========================================
# DoFn: Rozwijamy kolizje w klatki animacji
# ========================================
class ProcessCollisionsFn(beam.DoFn):
    def setup(self):
        # klient BQ tworzony raz na worker
        self.bq_client = bigquery.Client()

    def process(self, collision_row):
        """
        collision_row: obiekt z polami:
          - collision_id (str)
          - mmsi_a, mmsi_b (int)
          - timestamp (datetime)
          - cpa, tcpa (float)
          - ...
        """
        collision_id = collision_row.collision_id
        mmsi_a       = collision_row.mmsi_a
        mmsi_b       = collision_row.mmsi_b
        cpa_val      = collision_row.cpa
        tcpa_val     = collision_row.tcpa
        ts_coll      = collision_row.timestamp  # datetime

        # Filtry bezpieczeństwa (opcjonalne)
        if cpa_val >= 0.5:
            return
        if tcpa_val < 0 or tcpa_val > 10:
            return

        # Załóżmy, że bierzemy [T - 20 min, T + 10 min]
        coll_time = ts_coll
        start_t = coll_time - datetime.timedelta(minutes=20)
        end_t   = coll_time + datetime.timedelta(minutes=10)

        # Zapytanie do BQ o pozycje statków
        query = f"""
        SELECT
          mmsi,
          timestamp,
          latitude,
          longitude,
          sog,
          cog,
          ANY_VALUE(ship_name) as ship_name,
          ANY_VALUE(ship_length) as ship_length
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
            t = r.timestamp
            la = float(r.latitude or 0.0)
            lo = float(r.longitude or 0.0)
            sg = float(r.sog or 0.0)
            cg = float(r.cog or 0.0)
            # nazwa i długość (opcjonalnie)
            nm = r.ship_name
            ln = r.ship_length

            if r.mmsi == mmsi_a:
                data_a.append((t, la, lo, sg, cg, nm, ln))
            else:
                data_b.append((t, la, lo, sg, cg, nm, ln))

        # Zamieniamy w formę (ts, lat, lon, sog, cog) do interpolacji
        # => bo nasza funkcja interpolate_positions ma 5-tuple
        #  Dlatego do tymczasowej listy (ts, lat, lon, sog, cog)
        #  dopiszemy name, length w inny sposób.

        def strip_for_interp(row):
            # row: (ts, la, lo, sg, cg, nm, ln)
            return (row[0], row[1], row[2], row[3], row[4])  # 5-tuple

        data_a_stripped = [strip_for_interp(x) for x in data_a]
        data_b_stripped = [strip_for_interp(x) for x in data_b]

        interp_a = interpolate_positions(data_a_stripped, 60)
        interp_b = interpolate_positions(data_b_stripped, 60)

        # Budujemy finalną strukturę w postaci klatek:
        # frames = [
        #   {
        #     "time": "...",
        #     "shipPositions": [
        #       { "mmsi":..., "lat":..., "lon":..., "sog":..., "cog":..., "ship_name":..., "ship_length":... },
        #       { ... }
        #     ]
        #   },
        #   ...
        # ]
        # Możemy klatki scalić wg tego samego timespampu. Najprościej – bo mamy max 2 statki.

        # Zamieniamy listę (ts,lat,lon,sog,cog) => {lat,lon,sog,cog} i dołączamy name/len
        #   name/len bierzemy z oryginalnej listy data_a / data_b w najbliższym TS (lub 1. rekordu)
        #   aby mieć np. stąd:
        name_a = data_a[-1][5] if data_a else None
        len_a  = data_a[-1][6] if data_a else None
        name_b = data_b[-1][5] if data_b else None
        len_b  = data_b[-1][6] if data_b else None

        # Budujemy mapę {timestamp -> [positions...]} lub 2-lista
        # Ale prościej: iterujemy po time-synced klatkach.
        # Niech idx to "numer klatki" (0..N) – bierzemy min(len(interp_a), len(interp_b))?
        # Lepiej zuniować 2 listy i brać union timestampów, ale tu wystarczmy big "time set".
        # Dla uproszczenia – bierzemy unikalne times z interp_a i interp_b, sort i parujemy.

        from collections import defaultdict
        time_map = defaultdict(lambda: [])

        def add_ships(interp_list, mmsi_val, name_val, length_val):
            for (ts, la, lo, sg, cg) in interp_list:
                key_ts = ts.replace(microsecond=0)  # zaokrąglamy
                # dodajmy do time_map
                time_map[key_ts].append({
                    "mmsi": mmsi_val,
                    "lat": la,
                    "lon": lo,
                    "sog": sg,
                    "cog": cg,
                    "ship_name": name_val,
                    "ship_length": length_val
                })

        add_ships(interp_a, mmsi_a, name_a, len_a)
        add_ships(interp_b, mmsi_b, name_b, len_b)

        frames = []
        for tstamp in sorted(time_map.keys()):
            shipPos = time_map[tstamp]
            frames.append({
                "time": tstamp.isoformat(),
                "shipPositions": shipPos
            })

        # Budujemy finalny obiekt "collision"
        # Ten obiekt będzie np. tak:
        # {
        #   "collision_id": "...",
        #   "mmsi_a": ...,
        #   "mmsi_b": ...,
        #   "cpa": ...,
        #   "tcpa": ...,
        #   "timestamp": ...,
        #   "frames": [ ... ]
        # }
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

# ========================================
# Flatten do JSON lines
# ========================================
def record_to_jsonl(rec):
    # rec jest dict
    return json.dumps(rec, default=str) + "\n"

# ========================================
# Combine do jednego wielkiego JSON
# ========================================
def combine_collisions_into_single_json(items):
    # items to lista collision_obj
    # Zwracamy stringa z JSONem: np. {"collisions":[ {...}, {...} ]}
    # lub tablicę: [ {...}, {...} ]
    # W zależności co wolisz. Ja zaproponuję tablicę collisions:
    arr = list(items)
    big_dict = {"collisions": arr}
    return json.dumps(big_dict, default=str, indent=2)

class SingleJSONSink(FileSink):
    def open(self, fh):
        return fh
    def write(self, fh, element):
        # element = (key, collisions_json)
        # Ale możemy nie używać klucza. Zwracamy poprostu collisions_json
        fh.write(element.encode("utf-8"))
    def flush(self, fh):
        pass

def run():
    # Ustawienia pipeline (batch)
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = False

    # Ilość minut wstecz – np. 60
    lookback_minutes = int(os.getenv('LOOKBACK_MINUTES','60'))
    # Gdzie zapisujemy?
    output_prefix = os.getenv('HISTORY_OUTPUT_PREFIX',
                              'gs://ais-collision-detection-bucket/history_collisions/hourly')

    with beam.Pipeline(options=pipeline_options) as p:
        now = datetime.datetime.utcnow()
        start_time = now - datetime.timedelta(minutes=lookback_minutes)

        # Kwerenda – collisions z ostatniej godziny
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

        collisions = ( p 
            | "ReadCollisionsBQ" >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
        )

        # Rozwinięcie kolizji w formę klatek
        expanded = collisions | "ProcessCollisions" >> beam.ParDo(ProcessCollisionsFn())

        # Chcemy zapisać jedną paczkę JSON do jednego pliku, a nie per-kolizja
        # Więc 1) zbieramy to do listy; 2) konwertujemy w combine do single JSON
        collisions_pcoll = expanded | "ToList" >> beam.combiners.ToList()
        
        # Convert do jednego JSON
        single_json_str = collisions_pcoll | "ToSingleJson" >> beam.Map(combine_collisions_into_single_json)

        # Zapis do GCS (jeden plik na godzinę)
        timestamp_str = now.strftime("%Y%m%d%H%M%S")
        filename = f"{output_prefix}/collisions_{timestamp_str}.json"

        # Prosto "WriteToText" z suffix=".json" i shard=1
        # lub używamy fileio:
        ( single_json_str 
            | "WriteSingleFile" >> fileio.WriteToFiles(
                path=filename,
                file_naming=fileio.destination_prefix_naming(".json"),
                max_writers_per_bundle=1,
                sink=SingleJSONSink()
            )
        )

if __name__ == "__main__":
    run()