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

# Klasy/fileio do zapisu do jednego pliku:
from apache_beam.io.fileio import WriteToFiles, FileSink


# ------------------------------------------------------------
# Funkcja pomocnicza: interpolacja klatek
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

    # 1. Sortujemy rosnąco
    records.sort(key=lambda x: x[0])
    start_t = records[0][0]
    end_t   = records[-1][0]

    out = []
    current_t = start_t
    idx = 0

    while current_t <= end_t:
        # Znajdujemy segment [records[idx], records[idx+1]]
        while idx < len(records) - 1 and records[idx+1][0] < current_t:
            idx += 1

        if idx == len(records) - 1:
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


# ------------------------------------------------------------
# DoFn: wykrywanie scenariuszy wielostatkowych (graf)
# ------------------------------------------------------------
class GroupCollisionsIntoScenarios(beam.DoFn):
    """
    Zakładamy, że mamy listę kolizji w pewnym horyzoncie (np. z 1h).
    Każda kolizja jest parą (mmsi_a, mmsi_b, timestamp, cpa, tcpa, ...).
    Tworzymy spójne komponenty (graf) – węzły=statki, krawędzie=pary kolizyjne.
    Pod warunkiem, że występują w *zbliżonym* czasie i rejonie.

    Uwaga: Dla uproszczenia bierzemy "zbliżony czas" = ±10min od coll_time.
    "Zbliżony rejon" – ewentualnie geohash itp.
    W realnej implementacji możesz sprawdzać (latitude_a, longitude_a),
    by odrzucić pary kolizji, które są kilkadziesiąt km dalej.
    """
    def process(self, collisions):
        # collisions = lista obiektów (dict), np.:
        # {
        #   'collision_id': '123_456_20250109123000',
        #   'mmsi_a': 123,
        #   'mmsi_b': 456,
        #   'timestamp': datetime,
        #   'cpa': 0.2,
        #   'tcpa': 5.0,
        #   ...
        # }

        # 1) Sortujemy po 'timestamp'
        collisions.sort(key=lambda c: c['timestamp'])

        # 2) Budujemy graf
        # klucz = mmsi
        # edges = zestaw par
        graph = defaultdict(set)  # np. graph[mmsiA] = {mmsiB, mmsiC, ...}
        collision_map = []  # przechowuje (timestamp, a, b, collision_id, cpa, ...)

        for c in collisions:
            t = c['timestamp']
            a = c['mmsi_a']
            b = c['mmsi_b']
            collision_map.append(c)
            # Dodaj krawędź w grafie
            graph[a].add(b)
            graph[b].add(a)

        # 3) Znajdowanie spójnych komponentów
        visited = set()
        scenarios = []
        
        # Szybka metoda DFS
        def bfs(start):
            queue = deque([start])
            comp  = set([start])
            visited.add(start)
            while queue:
                curr = queue.popleft()
                for neigh in graph[curr]:
                    if neigh not in visited:
                        visited.add(neigh)
                        comp.add(neigh)
                        queue.append(neigh)
            return comp

        # Tworzymy listę zestawów statków (componentSet).
        all_mmsi = set(graph.keys())
        for m in all_mmsi:
            if m not in visited:
                comp = bfs(m)
                scenarios.append(comp)

        # 4) Teraz każdy "comp" to zbiór statków np. {123,456,789}.
        # Tworzymy *scenario_id*, np. generujemy hash, albo łączymy mmsi w string.
        # Słownik: scenarioID -> setOfMMSI
        # np. scenario_1: {123,456,789}, scenario_2: {555,666}, ...
        scenario_results = []
        for compSet in scenarios:
            # kluczem może być najwcześniejszy timestamp
            # w wierszu w collisions, który dotyczy par z compSet.
            # W wersji minimal: collision_id = "multiship_<time>_<...>" 
            #  – tu uproszczenie, bierzemy min timestamp
            relevant_collisions = []
            min_ts = None

            for c in collision_map:
                # c dotyczy pary (a,b). Jeśli a i b w compSet, to kolizja w tym scenariuszu
                if c['mmsi_a'] in compSet and c['mmsi_b'] in compSet:
                    relevant_collisions.append(c)
                    t = c['timestamp']
                    if (not min_ts) or (t < min_ts):
                        min_ts = t

            # Budujemy scenario_id
            scenario_id = f"scenario_{int(min_ts.timestamp()) if min_ts else 0}_{'_'.join(map(str, sorted(compSet)))}"

            scenario_results.append({
                "scenario_id": scenario_id,
                "ships_involved": sorted(list(compSet)),
                "collisions_in_scenario": relevant_collisions
            })

        # yield bo to DoFn
        yield from scenario_results


# ------------------------------------------------------------
# Funkcja: budowa animacji (frames) dla "scenario"
# ------------------------------------------------------------
class BuildScenarioFramesFn(beam.DoFn):
    def setup(self):
        self.bq_client = bigquery.Client()

    def process(self, scenario):
        """
        scenario = {
          "scenario_id": "...",
          "ships_involved": [mmsi1, mmsi2, ...],
          "collisions_in_scenario": [ { c1 }, { c2 }, ... ]
        }
        Chcemy zbudować "frames" => klatki animacji 20 min przed min timestamp i 5 min po max...
        """
        scenario_id = scenario["scenario_id"]
        ships = scenario["ships_involved"]
        coll_list = scenario["collisions_in_scenario"]
        
        if not coll_list:
            return

        # Określamy min i max timestamp z collisions
        min_ts = min(c['timestamp'] for c in coll_list)
        max_ts = max(c['timestamp'] for c in coll_list)

        start_t = min_ts - datetime.timedelta(minutes=20)
        end_t   = max_ts + datetime.timedelta(minutes=5)

        # 1) Pobieramy z BQ pozycje statków (ships) w [start_t, end_t]
        #    Dla uproszczenia: SELECT z ais_dataset_us.ships_positions
        #    w pętli: mmsi IN (ships)
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
        # Szykujemy mapę: mmsi -> lista (ts, lat, lon, sog, cog)
        from collections import defaultdict
        data_map = defaultdict(list)
        name_map = {}
        length_map = {}

        for r in rows:
            t  = r.timestamp
            la = float(r.latitude or 0.0)
            lo = float(r.longitude or 0.0)
            sg = float(r.sog or 0.0)
            cg = float(r.cog or 0.0)
            nm = r.ship_name
            ln = r.ship_length
            mmsi_val = r.mmsi

            data_map[mmsi_val].append((t, la, lo, sg, cg))
            name_map[mmsi_val]   = nm
            length_map[mmsi_val] = ln

        # Interpolujemy co 60 sek:
        # Tworzymy time_map[ts] = [ {mmsi, lat, lon, sog, cog, ...}, ...]
        time_map = defaultdict(list)

        for mmsi_val in ships:
            entries = data_map[mmsi_val]
            if not entries:
                continue
            # Interpol
            interped = interpolate_positions(entries, 60)
            nm = name_map.get(mmsi_val, str(mmsi_val))
            ln = length_map.get(mmsi_val, 0) or 0

            for (ts, la, lo, sg, cg) in interped:
                key_ts = ts.replace(microsecond=0)
                time_map[key_ts].append({
                    "mmsi": mmsi_val,
                    "lat": la,
                    "lon": lo,
                    "sog": sg,
                    "cog": cg,
                    "name": nm,
                    "ship_length": ln
                })

        # Sort i budowa frames
        frames = []
        for tstamp in sorted(time_map.keys()):
            frames.append({
                "time": tstamp.isoformat(),
                "shipPositions": time_map[tstamp]
            })

        scenario_obj = {
            "scenario_id": scenario_id,
            "ships_involved": ships,
            "collisions_in_scenario": coll_list,
            "frames": frames
        }
        yield scenario_obj


def scenario_list_to_json(scenario_list):
    # scenario_list: [ {scenario_id, ships_involved, frames, ...}, {...}, ... ]
    # opakowujemy w "scenarios"
    data = {
        "scenarios": scenario_list
    }
    return json.dumps(data, default=str, indent=2)


class SingleScenarioJSONSink(FileSink):
    """
    FileSink do zapisu jednego JSONa z polami "scenarios": [ ... ].
    """
    def open(self, fh):
        self._fh = fh

    def write(self, element):
        # element to string w formacie JSON
        self._fh.write(element.encode("utf-8"))
        self._fh.write(b"\n")

    def flush(self):
        pass


# ------------------------------------------------------------
# GŁÓWNY potok
# ------------------------------------------------------------
def run():
    logging.getLogger().setLevel(logging.INFO)
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = False

    lookback_minutes = int(os.getenv('LOOKBACK_MINUTES', '60'))
    output_prefix = os.getenv(
        'HISTORY_OUTPUT_PREFIX',
        'gs://ais-collision-detection-bucket/history_collisions/hourly'
    )
    now = datetime.datetime.utcnow()
    start_time = now - datetime.timedelta(minutes=lookback_minutes)

    # 1) Odczyt kolizji (parami) z BQ
    query = f"""
    SELECT
      CONCAT(CAST(mmsi_a AS STRING), '_',
             CAST(mmsi_b AS STRING), '_',
             FORMAT_TIMESTAMP('%Y%m%d%H%M%S', timestamp)) AS collision_id,
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
      AND cpa < 0.5
      AND tcpa >= 0
      AND tcpa <= 10
    ORDER BY timestamp
    """
    timestamp_str = now.strftime("%Y%m%d%H%M%S")
    filename = f"{output_prefix}/multiship_{timestamp_str}.json"

    with beam.Pipeline(options=pipeline_options) as p:
        collisions = (
            p
            | "ReadCollisions" >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
        )
        # collisions jest PCollection[dict{...}]

        # 2) Grupujemy w listę, bo musimy mieć *wspólną* listę w tym 1h:
        grouped = collisions | "GroupAll" >> beam.combiners.ToList()
        # => PCollection[List[dict]]

        # 3) Znajdujemy spójne komponenty => scenariusze
        # => Flatten: PCollection of scenario-objects
        scenario_objs = (
            grouped
            | "GroupIntoScenarios" >> beam.ParDo(GroupCollisionsIntoScenarios())
        )

        # 4) Dla każdego scenariusza budujemy frames
        scenario_frames = (
            scenario_objs
            | "BuildFrames" >> beam.ParDo(BuildScenarioFramesFn())
        )

        # 5) Zbieramy w jedną list i zamieniamy na JSON
        scenario_list = (
            scenario_frames
            | "ToList" >> beam.combiners.ToList()
        )
        scenario_json = scenario_list | "ToJSON" >> beam.Map(scenario_list_to_json)

        # 6) Zapis do pliku
        scenario_json | "WriteSingleFile" >> WriteToFiles(
            path=filename,
            max_writers_per_bundle=1,
            sink=SingleScenarioJSONSink()
        )

if __name__ == "__main__":
    run()