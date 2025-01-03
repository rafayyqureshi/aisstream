#!/usr/bin/env python3
import os
import json
import math
import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from google.cloud import bigquery
from apache_beam.io import fileio
from apache_beam.transforms import window

# ------------------------------------------------------------
# Funkcja pomocnicza: Prosta interpolacja klatek
# ------------------------------------------------------------
def interpolate_positions(records, step_seconds=60):
    """
    records: lista krotek (ts, lat, lon, sog, cog),
             posortowana według czasu.
    step_seconds: co ile sekund generujemy klatkę.
    Zwraca listę (ts, lat, lon, sog, cog) z równomiernymi odstępami czasu.
    """
    if not records:
        return []

    # Sortowanie rosnąco po timestamp
    records.sort(key=lambda x: x[0])
    start_t = records[0][0]
    end_t   = records[-1][0]

    out = []
    current_t = start_t
    idx = 0

    while current_t <= end_t:
        # Znajdujemy segment [records[idx], records[idx+1]] w którym mieści się current_t
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
                # jeszcze przed tA → bierzemy wartości z tA
                out.append((current_t, laA, loA, sgA, cgA))
            else:
                dt = (tB - tA).total_seconds()
                if dt <= 0:
                    # jeżeli tA == tB
                    out.append((current_t, laA, loA, sgA, cgA))
                else:
                    ratio = (current_t - tA).total_seconds() / dt
                    lat_i = laA + (laB - laA)*ratio
                    lon_i = loA + (loB - loA)*ratio
                    sog_i = sgA + (sgB - sgA)*ratio
                    cog_i = cgA + (cgB - cgA)*ratio
                    out.append((current_t, lat_i, lon_i, sog_i, cog_i))

        current_t += datetime.timedelta(seconds=step_seconds)

    return out

# ------------------------------------------------------------
# DoFn: Pobieramy kolizje z BQ → dla każdej generujemy animację
# ------------------------------------------------------------
class ProcessCollisionsFn(beam.DoFn):
    def setup(self):
        # Tworzymy klienta BigQuery raz na worker
        self.bq_client = bigquery.Client()

    def process(self, collision_row):
        """
        collision_row: obiekt z polami:
          - collision_id
          - mmsi_a, mmsi_b
          - timestamp (datetime)
          - cpa, tcpa
          - ...
        Zakładamy, że w BQ mamy kolumny o tych nazwach.
        """
        collision_id = collision_row.collision_id
        mmsi_a       = collision_row.mmsi_a
        mmsi_b       = collision_row.mmsi_b
        cpa_val      = collision_row.cpa
        tcpa_val     = collision_row.tcpa
        ts_coll      = collision_row.timestamp  # datetime

        # Filtr bezpieczeństwa – chcemy tylko faktyczne kolizje
        # np. cpa < 0.5 i (tcpa < 10?), ale faktycznie wystąpiło zbliżenie (tcpa>=0)
        # Tutaj możemy też sprawdzać inne warunki
        if cpa_val is None or cpa_val >= 0.5:
            return  # pomijamy
        if tcpa_val is None or tcpa_val > 10:
            return  # pomijamy

        # Zakres czasowy: [T - 20 min, T + 5 min]
        coll_time = ts_coll
        start_t = coll_time - datetime.timedelta(minutes=20)
        end_t   = coll_time + datetime.timedelta(minutes=5)

        # Pobieramy dane AIS z BQ
        query = f"""
        SELECT 
          mmsi,
          timestamp,
          latitude,
          longitude,
          sog,
          cog
        FROM `ais_dataset_us.ships_positions`
        WHERE mmsi IN ({mmsi_a}, {mmsi_b})
          AND timestamp BETWEEN '{start_t.isoformat()}' AND '{end_t.isoformat()}'
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

            if r.mmsi == mmsi_a:
                data_a.append((t, la, lo, sg, cg))
            else:
                data_b.append((t, la, lo, sg, cg))

        # Interpolacja co 60 sek
        interp_a = interpolate_positions(data_a, 60)
        interp_b = interpolate_positions(data_b, 60)

        # Budujemy finalne rekordy
        out = []
        for (ts, la, lo, sg, cg) in interp_a:
            out.append({
                "collision_id": collision_id,
                "mmsi": mmsi_a,
                "timestamp": ts.isoformat(),
                "latitude": la,
                "longitude": lo,
                "sog": sg,
                "cog": cg
            })
        for (ts, la, lo, sg, cg) in interp_b:
            out.append({
                "collision_id": collision_id,
                "mmsi": mmsi_b,
                "timestamp": ts.isoformat(),
                "latitude": la,
                "longitude": lo,
                "sog": sg,
                "cog": cg
            })

        # Zwrot wierszy
        for rec in out:
            yield rec

# ------------------------------------------------------------
# Konwersja do JSON Lines
# ------------------------------------------------------------
def record_to_jsonl(rec):
    return json.dumps(rec, default=str) + "\n"

def run():
    # Jest to job *batch*, uruchamiany np. raz na godzinę
    pipeline_options = PipelineOptions()
    # StandardOptions.streaming = False – to jest batch
    pipeline_options.view_as(StandardOptions).streaming = False

    # Ścieżka GCS, np.: "gs://ais-collision-detection-bucket/history_collisions"
    output_prefix = os.getenv('HISTORY_COLLISIONS_PATH','gs://ais-collision-detection-bucket/history_collisions_batch')

    # Parametr, ile minut wstecz – np. 60
    # (możesz też brać parametry wierszy z collisions z *ostatnich x godzin*)
    lookback_minutes = int(os.getenv('LOOKBACK_MINUTES','60'))

    with beam.Pipeline(options=pipeline_options) as p:
        # 1) Odczyt kolizji z BigQuery (tylko te z ostatniej godziny)
        # Filtry cpa < 0.5 i tcpa < 10 – możesz je dać tutaj lub dopiero w DoFn
        now = datetime.datetime.utcnow()
        start_time = now - datetime.timedelta(minutes=lookback_minutes)

        query = f"""
        SELECT
          CONCAT(CAST(mmsi_a AS STRING), '_',
                 CAST(mmsi_b AS STRING), '_',
                 FORMAT_TIMESTAMP('%Y%m%d%H%M%S', timestamp)) as collision_id,
          mmsi_a,
          mmsi_b,
          timestamp,
          cpa,
          tcpa
        FROM `ais_dataset_us.collisions`
        WHERE timestamp >= TIMESTAMP('{start_time.isoformat()}')
          AND cpa < 0.5
          AND tcpa < 10
          AND tcpa >= 0
        ORDER BY timestamp
        """

        collisions = (
            p
            | "ReadCollisionsBQ" >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
        )

        # 2) Dla każdej kolizji -> pobierz data AIS i interpoluj
        expanded = collisions | "ProcessCollisions" >> beam.ParDo(ProcessCollisionsFn())

        # 3) Konwersja do JSON Lines
        jsonl = expanded | "ToJSONL" >> beam.Map(record_to_jsonl)

        # 4) KeyBy(collision_id), aby 1 plik per kolizja
        def key_by_collisionid(line):
            r = json.loads(line)
            cid = r["collision_id"]
            return (cid, line)

        keyed = jsonl | "KeyByCollisionId" >> beam.Map(key_by_collisionid)

        # 5) GroupByKey (bo to jest batch, możemy użyć global window)
        grouped = keyed | "GroupByCollisionId" >> beam.GroupByKey()

        # 6) Łączenie
        def combine_lines(kv):
            cid, lines_list = kv
            combined_str = "".join(lines_list)  # Każda linia ma już \n
            yield (cid, combined_str)

        combined = grouped | "CombineLines" >> beam.FlatMap(combine_lines)

        # 7) Zapis do GCS
        from apache_beam.io.fileio import WriteToFiles, FileSink

        class CollisionSink(FileSink):
            def open(self, fh):
                return fh
            def write(self, fh, element):
                # element = (cid, big_str)
                cid, big_str = element
                fh.write(big_str.encode('utf-8'))
            def flush(self, fh):
                pass

        def collision_file_naming(element, context):
            cid, _ = element
            # Możesz dodać datę batcha
            now_str = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
            return f"{cid}_{now_str}.json"

        (
            combined
            | "WriteToGCS" >> WriteToFiles(
                path=output_prefix,
                sink=CollisionSink(),
                # W batchu możesz zrobić 1 writer per key => wystarczy 1 shard
                max_writers_per_bundle=1,
                file_naming=collision_file_naming
            )
        )

if __name__ == "__main__":
    run()