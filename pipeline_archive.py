#!/usr/bin/env python3
import os
import json
import math
import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from google.cloud import bigquery
from apache_beam.io import fileio

# ------------------------------------------------------------
# Funkcja interpolacji prostych klatek co X sekund
# (tu jest pokazana koncepcyjnie, można ulepszyć).
# ------------------------------------------------------------
def interpolate_positions(records, step_seconds=60):
    """
    records – lista krotek (ts: datetime, lat, lon, sog, cog).
    step_seconds – co ile sekund generujemy klatkę (np. 60).
    Zwraca nową listę (ts, lat, lon, sog, cog) z równomiernymi odstępami.
    """
    if not records:
        return []

    records.sort(key=lambda x: x[0])
    start_t = records[0][0]
    end_t   = records[-1][0]

    out = []
    current_t = start_t
    idx = 0

    while current_t <= end_t:
        # szukamy segmentu [records[idx], records[idx+1]] gdzie current_t się mieści
        while idx < len(records)-1 and records[idx+1][0] < current_t:
            idx += 1

        if idx == len(records)-1:
            # doszliśmy do końca
            out.append((current_t, records[-1][1], records[-1][2], records[-1][3], records[-1][4]))
        else:
            tA, latA, lonA, sogA, cogA = records[idx]
            tB, latB, lonB, sogB, cogB = records[idx+1]
            if current_t < tA:
                # bierzemy tA
                out.append((current_t, latA, lonA, sogA, cogA))
            else:
                dt = (tB - tA).total_seconds()
                if dt <= 0:
                    out.append((current_t, latA, lonA, sogA, cogA))
                else:
                    ratio = (current_t - tA).total_seconds() / dt
                    lat_i = latA + (latB - latA)*ratio
                    lon_i = lonA + (lonB - lonA)*ratio
                    sog_i = sogA + (sogB - sogA)*ratio
                    cog_i = cogA + (cogB - cogA)*ratio
                    out.append((current_t, lat_i, lon_i, sog_i, cog_i))

        current_t += datetime.timedelta(seconds=step_seconds)

    return out

# ------------------------------------------------------------
# DoFn pobierający dane kolizji i ładujący z BQ potrzebny
# wycinek pozycji statków, interpoluje i zwraca "rozszerzone klatki"
# ------------------------------------------------------------
class PrepareCollisionDataFn(beam.DoFn):
    def setup(self):
        self.client = bigquery.Client()

    def process(self, collision_msg):
        """
        collision_msg to bajty JSON z Pub/Sub, np.:
        {
          'collision_id': '111222_999888_2025-01-10T12:30:00Z',
          'mmsi_a': 111222,
          'mmsi_b': 999888,
          'timestamp': '2025-01-10T12:30:00Z',
          ...
        }
        """
        data = json.loads(collision_msg.decode('utf-8'))
        collision_id = data['collision_id']
        mmsi_a = data['mmsi_a']
        mmsi_b = data['mmsi_b']
        coll_ts_str = data['timestamp']  # "2025-01-10T12:30:00Z"

        # Okno czasowe: [T-20min, T+5min]
        fmt = "%Y-%m-%dT%H:%M:%SZ"  # zakładamy taki format
        coll_time = datetime.datetime.strptime(coll_ts_str, fmt)
        start_t = coll_time - datetime.timedelta(minutes=20)
        end_t   = coll_time + datetime.timedelta(minutes=5)

        # Query do BQ
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
        rows = list(self.client.query(query).result())

        data_a = []
        data_b = []
        for r in rows:
            t = r.timestamp
            la = float(r.latitude or 0)
            lo = float(r.longitude or 0)
            sg = float(r.sog or 0)
            cg = float(r.cog or 0)
            if r.mmsi == mmsi_a:
                data_a.append((t, la, lo, sg, cg))
            else:
                data_b.append((t, la, lo, sg, cg))

        # Interpolacja co 30s lub 1 min (dowolnie)
        interp_a = interpolate_positions(data_a, step_seconds=60)
        interp_b = interpolate_positions(data_b, step_seconds=60)

        # Składamy w jedną listę, dodajemy collision_id i mmsi
        final_records = []
        for (ts, la, lo, sg, cg) in interp_a:
            final_records.append({
                'collision_id': collision_id,
                'mmsi': mmsi_a,
                'timestamp': ts.isoformat(),
                'latitude': la,
                'longitude': lo,
                'sog': sg,
                'cog': cg
            })
        for (ts, la, lo, sg, cg) in interp_b:
            final_records.append({
                'collision_id': collision_id,
                'mmsi': mmsi_b,
                'timestamp': ts.isoformat(),
                'latitude': la,
                'longitude': lo,
                'sog': sg,
                'cog': cg
            })

        # Zwracamy
        for rec in final_records:
            yield rec

def record_to_jsonl(record):
    """
    Zwraca string JSON (jednolinijkowy), gotowy do zapisu do pliku w GCS.
    """
    return json.dumps(record, default=str) + "\n"

def run():
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True

    # Słuchamy collisions-topic
    collisions_topic = os.getenv('COLLISIONS_TOPIC','projects/ais-collision-detection/topics/collisions-topic')
    output_path = os.getenv('HISTORY_COLLISIONS_PATH','gs://ais-collision-detection-bucket/history_collisions')

    with beam.Pipeline(options=pipeline_options) as p:
        collisions = p | "ReadCollisions" >> beam.io.ReadFromPubSub(topic=collisions_topic)

        # 1) Dla każdej kolizji -> pobieramy, interpolujemy
        collision_expanded = collisions | "PrepareCollisionData" >> beam.ParDo(PrepareCollisionDataFn())

        # 2) Konwertujemy do JSON lines
        collision_jsonl = collision_expanded | "ToJSONL" >> beam.Map(record_to_jsonl)

        # 3) Grupujemy wg collision_id, by każdy plik w GCS miał dane jednej kolizji
        #    (można użyć np. key = collision_id)
        keyed = collision_jsonl | "KeyByCollisionId" >> beam.Map(lambda row: (json.loads(row)['collision_id'], row))

        # 4) Window - w prostym przypadku global window + np. trigger on_after processing, 
        #    ale w streaming trudniej jest "zebrać" w 1 plik – można zapisać do wielu shardów 
        #    i potem scalić.
        #    Dla uproszczenia – zapiszemy do jednego folderu per collision_id.
        #    Skorzystamy z FileIO WriteToFiles, z custom naming, 
        #    tak by collision_id stał się częścią nazwy.
        from apache_beam.io.fileio import WriteToFiles, FileSink

        class CollisionFileNaming(fileio.FileNaming):
            def __init__(self, prefix):
                self.prefix = prefix
            def file_name(self, window, pane, shard_index, total_shards, compression):
                # np: collisions-<collision_id>-shard-<idx>.json
                # prefix tu powinien zawierać collision_id
                return self.prefix + f"-{shard_index:03d}.json"

        def collision_id_to_filename(kv):
            # k, v
            collision_id, row = kv
            return collision_id

        # W tym przykłdzie zrobimy sobie transform Flatten, GroupByKey,
        # bo WriteToFiles łatwiej jest użyć z jednym strumieniem (każdy collision_id).
        # Można też użyć transformacji direct: WriteToFiles with dynamic destinations (Beam 2.30+)
        grouped = keyed | "GroupByCollisionId" >> beam.GroupByKey()

        # Następnie zapiszemy do GCS każdy collision_id w osobnym pliku:
        # Niestety w streamingu pliki będą się "doklejać" do istn. outputu. 
        # Lepiej to robić w systemie Batch lub z użyciem dynamic destinations.
        def expand_records(kv):
            collision_id, rows = kv
            # Każdy wiersz = row (string)
            # Możemy złączyć w jeden plik
            # Zrobimy yield jednego "dużego" stringa?
            combined = "".join(rows)  # bo each row already has \n at the end
            yield (collision_id, combined)

        combined_pcoll = grouped | "CombineRecords" >> beam.FlatMap(expand_records)

        # Teraz zapiszemy => WriteToFiles
        # Użyjemy custom transformu, w którym collision_id jest częścią nazwy
        class CollisionSink(FileSink):
            def open(self, file_handle):
                # Zwraca "writer"
                return file_handle
            def write(self, file_handle, element):
                # element = (collision_id, big_string)
                (cid, data_str) = element
                file_handle.write(data_str.encode('utf-8'))
            def flush(self, file_handle):
                pass

        # Tworzymy transform WriteToFiles:
        # path=output_path – folder prefix
        # file_naming – obiekt, który w runtime otrzyma collision_id i shard
        #    => tu widać, że w standardzie file_naming nie pobiera klucza 
        #    => więc typowo używamy partition (Beam 2.30+ dynamicWrite) 
        #      lub trick z GroupByKey.
        # Dla uproszczenia – do jednego pliku zawsze shard=1:
        # (będzie 1 plik per collision).
        # W streamingu to i tak jest trudne do spójnego scalania 
        # – docelowo zwykle batch wywołujemy.

        write_result = (
            combined_pcoll
            | "WriteCollisionFiles" >> fileio.WriteToFiles(
                path=output_path,
                sink=CollisionSink(),
                max_writers_per_bundle=1,
                shards=1,
                file_naming=lambda w, p, si, ts, c: f"collision-{time.time_ns()}.json"
            )
        )

        # Powyższe – dość uproszczone podejście. 
        # W praktyce w streaming + pliki do GCS = musisz liczyć się z wieloma plikami. 
        # Ewentualnie da się stosować "FileBasedSink" i Dataflow batch.

if __name__ == '__main__':
    run()