#!/usr/bin/env python3
import os
import json
import math
import datetime
import logging
from collections import defaultdict, deque

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from google.cloud import bigquery

# WriteToFiles + FileSink do zapisu do jednego pliku:
from apache_beam.io.fileio import WriteToFiles, FileSink


def interpolate_positions(records, step_seconds=60):
    """
    records: lista krotek (ts, lat, lon, sog, cog),
             posortowana rosnąco po ts.
    Generuje klatki co step_seconds (domyślnie 60).
    Zwraca listę (ts, lat, lon, sog, cog).
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
        # Szukamy segmentu [records[idx], records[idx+1]] w którym mieści się current_t
        while idx < len(records) - 1 and records[idx+1][0] < current_t:
            idx += 1

        if idx == len(records) - 1:
            # już na końcu
            t_last, la, lo, sg, cg = records[-1]
            out.append((current_t, la, lo, sg, cg))
        else:
            tA, laA, loA, sgA, cgA = records[idx]
            tB, laB, loB, sgB, cgB = records[idx+1]
            if current_t < tA:
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
                    out.append((current_t, lat_i, lon_i, sog_i, cog_i))

        current_t += datetime.timedelta(seconds=step_seconds)

    return out


class GroupCollisionsIntoScenarios(beam.DoFn):
    """
    Wczytuje listę kolizji z ostatniego okna.
    Tworzy spójne scenariusze wielostatkowe (graf).
    """
    def process(self, collisions_list):
        # collisions_list: List[dict], np.:
        # [{
        #    'collision_id': ...,
        #    'mmsi_a': ...,
        #    'mmsi_b': ...,
        #    'timestamp': datetime,
        #    ...
        # }, ...]

        # 1) Sortuj po timestamp
        collisions_list.sort(key=lambda c: c['timestamp'])

        # 2) Budujemy graf: węzły = mmsi, krawędzie = pary kolizyjne
        graph = defaultdict(set)
        collision_map = []

        for c in collisions_list:
            a = c['mmsi_a']
            b = c['mmsi_b']
            graph[a].add(b)
            graph[b].add(a)
            collision_map.append(c)

        # 3) Znajdowanie spójnych komponentów (DFS/BFS)
        visited = set()
        scenarios = []

        def bfs(start):
            comp = set([start])
            queue = deque([start])
            visited.add(start)
            while queue:
                curr = queue.popleft()
                for neigh in graph[curr]:
                    if neigh not in visited:
                        visited.add(neigh)
                        comp.add(neigh)
                        queue.append(neigh)
            return comp

        all_mmsi = set(graph.keys())
        for m in all_mmsi:
            if m not in visited:
                compSet = bfs(m)
                # Dla danego compSet (np. {123, 456, 789}) – zbiór statków
                # Zbiór kolizji w tym scenariuszu:
                relevant_collisions = []
                min_ts = None
                max_ts = None
                for coll in collision_map:
                    if (coll['mmsi_a'] in compSet) and (coll['mmsi_b'] in compSet):
                        relevant_collisions.append(coll)
                        ts = coll['timestamp']
                        if (not min_ts) or ts < min_ts:
                            min_ts = ts
                        if (not max_ts) or ts > max_ts:
                            max_ts = ts

                scenario_id = f"scenario_{int(min_ts.timestamp()) if min_ts else 0}_" \
                              + "_".join(map(str, sorted(list(compSet))))

                yield {
                    "scenario_id": scenario_id,
                    "ships_involved": sorted(list(compSet)),
                    "collisions_in_scenario": relevant_collisions,
                    "min_ts": min_ts,
                    "max_ts": max_ts
                }


class BuildScenarioFramesFn(beam.DoFn):
    """
    Dla scenariusza (wiele statków, kolizje, min_ts, max_ts),
    pobiera dane z BQ i tworzy klatki animacji.
    """
    def setup(self):
        self.bq_client = bigquery.Client()

    def process(self, scenario):
        scenario_id = scenario["scenario_id"]
        ships = scenario["ships_involved"]
        collisions = scenario["collisions_in_scenario"]
        min_ts = scenario.get("min_ts", None)
        max_ts = scenario.get("max_ts", None)

        if not min_ts or not max_ts:
            return

        # Zakres: 15 min przed min_ts, 5 min po max_ts
        start_t = min_ts - datetime.timedelta(minutes=15)
        end_t   = max_ts + datetime.timedelta(minutes=5)

        # Pobieramy pozycje statków
        ships_str = ",".join(map(str, ships))
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
        WHERE mmsi IN ({ships_str})
          AND timestamp BETWEEN '{start_t.isoformat()}' AND '{end_t.isoformat()}'
        GROUP BY mmsi, timestamp, latitude, longitude, sog, cog
        ORDER BY timestamp
        """

        rows = list(self.bq_client.query(query).result())

        from collections import defaultdict
        data_map = defaultdict(list)
        name_map = {}
        len_map  = {}

        for r in rows:
            t  = r.timestamp
            la = float(r.latitude or 0.0)
            lo = float(r.longitude or 0.0)
            sg = float(r.sog or 0.0)
            cg = float(r.cog or 0.0)
            nm = r.ship_name
            ln = r.ship_length
            m = r.mmsi

            data_map[m].append((t, la, lo, sg, cg))
            name_map[m] = nm
            len_map[m]  = ln

        # Interpol co 60s
        time_map = defaultdict(list)
        for m in ships:
            entries = data_map[m]
            if not entries:
                continue
            interp = interpolate_positions(entries, 60)
            nm = name_map.get(m, str(m))
            ln = len_map.get(m, 0) or 0

            for (ts, la, lo, sg, cg) in interp:
                key_ts = ts.replace(microsecond=0)
                time_map[key_ts].append({
                    "mmsi": m,
                    "name": nm,
                    "ship_length": ln,
                    "lat": la,
                    "lon": lo,
                    "sog": sg,
                    "cog": cg
                })

        frames = []
        for tstamp in sorted(time_map.keys()):
            frames.append({
                "time": tstamp.isoformat(),
                "shipPositions": time_map[tstamp]
            })

        scenario_obj = {
            "scenario_id": scenario_id,
            "ships_involved": ships,
            "frames": frames,
            # Możesz dołączyć collisions_in_scenario, jeśli chcesz
            "collisions_in_scenario": collisions
        }
        yield scenario_obj


def scenario_list_to_json(scenarios):
    arr = list(scenarios)
    return json.dumps({"scenarios": arr}, default=str, indent=2)


class SingleScenarioJSONSink(FileSink):
    def open(self, fh):
        self._fh = fh

    def write(self, element):
        self._fh.write(element.encode("utf-8"))
        self._fh.write(b"\n")

    def flush(self):
        pass


def run():
    logging.getLogger().setLevel(logging.INFO)

    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = False

    # np. potok uruchamiany co godzinę (z Cloud Scheduler),
    # w parametrach LOOKBACK_MINUTES=60
    lookback_minutes = int(os.getenv("LOOKBACK_MINUTES", "60"))

    output_prefix = os.getenv(
        "HISTORY_OUTPUT_PREFIX",
        "gs://ais-collision-detection-bucket/history_collisions/hourly"
    )

    now_utc = datetime.datetime.utcnow()
    # Załóżmy, że chcemy poprzednią godzinę:
    this_hour = now_utc.replace(minute=0, second=0, microsecond=0)
    prev_hour = this_hour - datetime.timedelta(hours=1)

    start_time = prev_hour
    end_time   = this_hour

    # Budujemy query: cpa < 0.5, ale np. w final chcemy < 0.3
    # Możesz zmienić w zależności od wymagań.
    query = f"""
    SELECT
      CONCAT(
         CAST(mmsi_a AS STRING), '_',
         CAST(mmsi_b AS STRING), '_',
         FORMAT_TIMESTAMP('%Y%m%d%H%M%S', timestamp)
      ) AS collision_id,
      mmsi_a,
      mmsi_b,
      timestamp,
      cpa,
      tcpa,
      latitude_a,
      longitude_a,
      latitude_b,
      longitude_b
    FROM `ais_dataset_us.collisions`
    WHERE timestamp >= TIMESTAMP('{start_time.isoformat()}')
      AND timestamp <  TIMESTAMP('{end_time.isoformat()}')
      AND cpa < 0.3
      AND tcpa >= 0
      AND tcpa <= 10
    ORDER BY timestamp
    """

    # Nazwa pliku: collisions_YYYYmmdd_HH.json
    date_str = prev_hour.strftime("%Y%m%d_%H")
    filename = f"{output_prefix}/multiship_{date_str}.json"

    with beam.Pipeline(options=pipeline_options) as p:
        # 1) Odczyt kolizji z BQ
        collisions = (
            p
            | "ReadCollisions" >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
        )

        # 2) Zamieniamy w jedną listę
        collisions_list = collisions | "GroupAll" >> beam.combiners.ToList()

        # 3) Tworzymy scenariusze wielostatkowe
        scenarios = collisions_list | "GroupCollisions" >> beam.ParDo(GroupCollisionsIntoScenarios())

        # 4) Budujemy frames
        scenarios_with_frames = scenarios | "BuildFrames" >> beam.ParDo(BuildScenarioFramesFn())

        # 5) Zbieramy do jednej list
        scenario_list = scenarios_with_frames | "ToList" >> beam.combiners.ToList()

        # 6) Przerabiamy na JSON
        scenario_json = scenario_list | "ToJSON" >> beam.Map(scenario_list_to_json)

        # 7) Zapis do pojedynczego pliku
        scenario_json | "WriteSingleFile" >> WriteToFiles(
            path=filename,
            max_writers_per_bundle=1,
            sink=SingleScenarioJSONSink()
        )

if __name__ == "__main__":
    run()