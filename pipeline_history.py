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
from apache_beam.io.fileio import WriteToFiles, FileSink


def interpolate_positions(records, step_seconds=60):
    """
    records: lista krotek (ts, lat, lon, sog, cog),
             posortowana rosnąco po ts.
    Generuje klatki co step_seconds (domyślnie 60s).
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
            # koniec listy
            t_last, la, lo, sg, cg = records[-1]
            out.append((current_t, la, lo, sg, cg))
        else:
            tA, laA, loA, sgA, cgA = records[idx]
            tB, laB, loB, sgB, cgB = records[idx+1]
            if current_t < tA:
                # jeszcze przed tA -> bierzemy z tA
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
    Tworzy “scenariusze” wielostatkowe przez graf spójny.
    Ale docelowo i tak w BuildScenarioFramesFn
    możemy generować pary (focus) z 15/5-min oknem.
    """
    def process(self, collisions_list):
        collisions_list.sort(key=lambda c: c['timestamp'])
        graph = defaultdict(set)
        collision_map = []

        for c in collisions_list:
            a = c['mmsi_a']
            b = c['mmsi_b']
            graph[a].add(b)
            graph[b].add(a)
            collision_map.append(c)

        visited = set()

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
        while all_mmsi - visited:
            start = (all_mmsi - visited).pop()
            comp_set = bfs(start)

            # Wyznacz kolizje należące do comp_set
            relevant_collisions = []
            min_ts = None
            max_ts = None
            for coll in collision_map:
                if (coll['mmsi_a'] in comp_set) and (coll['mmsi_b'] in comp_set):
                    relevant_collisions.append(coll)
                    t = coll['timestamp']
                    if (not min_ts) or (t < min_ts):
                        min_ts = t
                    if (not max_ts) or (t > max_ts):
                        max_ts = t

            scenario_id = f"scenario_{int(min_ts.timestamp()) if min_ts else 0}_" \
                          + "_".join(map(str, sorted(comp_set)))

            yield {
                "scenario_id": scenario_id,
                "ships_involved": sorted(list(comp_set)),
                "collisions_in_scenario": relevant_collisions,
                "min_ts": min_ts,
                "max_ts": max_ts
            }


class BuildScenarioFramesFn(beam.DoFn):
    """
    Dla scenariusza budujemy animacje dla *każdej* pary w collisions_in_scenario
    z 15min/5min oknem, ALE do frames dołączamy inne statki towarzyszące.
    """
    def setup(self):
        self.bq_client = bigquery.Client()

    def _haversine_nm(self, lat1, lon1, lat2, lon2):
        """
        Proste liczenie dystansu (nm) miedzy dwoma punktami (lat, lon).
        """
        R_earth_nm = 3440.065  # promień Ziemi w nm
        dLat = math.radians(lat2 - lat1)
        dLon = math.radians(lon2 - lon1)
        a = (math.sin(dLat/2)**2
             + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dLon/2)**2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        return R_earth_nm * c

    def process(self, scenario):
        scenario_id = scenario["scenario_id"]
        scenario_mmsi = scenario["ships_involved"]
        collisions = scenario["collisions_in_scenario"]

        # Zbieramy w jedną list “scenarioOutputs” – bo może być np. 2-3 pary w wielostatkowej sytuacji.
        scenarioOutputs = []

        # Najpierw pobierzmy dane AIS *wszystkich* statków scenario_mmsi w [min_ts-15, max_ts+5].
        min_ts = scenario.get("min_ts")
        max_ts = scenario.get("max_ts")
        if not min_ts or not max_ts:
            return  # brak kolizji?

        start_t = min_ts - datetime.timedelta(minutes=15)
        end_t   = max_ts + datetime.timedelta(minutes=5)

        # Wyciągamy dane z BQ
        ships_str = ",".join(map(str, scenario_mmsi))
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
            mm = r.mmsi
            data_map[mm].append((t, la, lo, sg, cg))
            name_map[mm] = nm
            len_map[mm]  = ln

        # Interpolacja
        full_time_map = defaultdict(list)
        for mm in scenario_mmsi:
            entries = data_map[mm]
            if not entries:
                continue
            interped = interpolate_positions(entries, 60)
            nm = name_map.get(mm, str(mm))
            ln = len_map.get(mm, 0) or 0

            for (ts, la, lo, sg, cg) in interped:
                key_ts = ts.replace(microsecond=0)
                full_time_map[key_ts].append({
                    "mmsi": mm,
                    "name": nm,
                    "ship_length": ln,
                    "lat": la,
                    "lon": lo,
                    "sog": sg,
                    "cog": cg
                })

        # Dla *każdej* pary kolizyjnej w “collisions”
        # tworzymy 1 obiekt “collision scenario output”
        for coll in collisions:
            # focus: A–B
            a = coll["mmsi_a"]
            b = coll["mmsi_b"]
            t_min = coll["timestamp"]  # moment minimalnego zbliżenia
            if not t_min:
                continue
            # times:
            t_min_dt = t_min
            sub_start = t_min_dt - datetime.timedelta(minutes=15)
            sub_end   = t_min_dt + datetime.timedelta(minutes=5)

            # Budujemy frames T w subrange
            # bierzemy from full_time_map klatki w zakresie [sub_start, sub_end]
            # -> sort i budowa
            frames_list = []
            for tstamp in sorted(full_time_map.keys()):
                if tstamp < sub_start or tstamp > sub_end:
                    continue
                shipsArr = full_time_map[tstamp]
                # Obliczamy dystans miedzy (a,b), jesli obie pozycje są:
                posA = None
                posB = None
                for sdat in shipsArr:
                    if sdat["mmsi"] == a:
                        posA = sdat
                    if sdat["mmsi"] == b:
                        posB = sdat

                dist_nm = None
                if posA and posB:
                    dist_nm = self._haversine_nm(posA["lat"], posA["lon"], posB["lat"], posB["lon"])
                # delta_minutes = (tstamp - t_min_dt).total_seconds() / 60.0

                frames_list.append({
                    "time": tstamp.isoformat(),
                    "shipPositions": shipsArr,
                    # przykładowe dodatkowe pola:
                    "focus_dist": dist_nm,
                    "delta_minutes": round((tstamp - t_min_dt).total_seconds()/60.0, 2)
                })

            collision_id = coll["collision_id"]  # np. "123_456_20250109123000"
            cpa_val = coll["cpa"]
            scenario_out = {
                "collision_id": collision_id,
                "focus_mmsi": [a, b],
                "t_min": t_min_dt.isoformat(),
                "cpa": cpa_val,
                "frames": frames_list,
                "all_involved_mmsi": scenario_mmsi,  # bo to jest scenariusz wielostatkowy
            }
            scenarioOutputs.append(scenario_out)

        # yield wszystkich
        yield from scenarioOutputs


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

    # Chcemy potok co godzinę
    now_utc = datetime.datetime.utcnow()
    this_hour = now_utc.replace(minute=0, second=0, microsecond=0)
    prev_hour = this_hour - datetime.timedelta(hours=1)

    start_time = prev_hour
    end_time   = this_hour

    # cpa < 0.3 => minimalny dystans
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

    output_prefix = os.getenv(
        "HISTORY_OUTPUT_PREFIX",
        "gs://ais-collision-detection-bucket/history_collisions/hourly"
    )
    date_str = prev_hour.strftime("%Y%m%d_%H")
    filename = f"{output_prefix}/multiship_{date_str}.json"

    with beam.Pipeline(options=pipeline_options) as p:
        collisions = (
            p
            | "ReadCollisions" >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
        )

        # single list
        collisions_list = collisions | "GroupAll" >> beam.combiners.ToList()

        scenarios = collisions_list | "GroupCollisions" >> beam.ParDo(GroupCollisionsIntoScenarios())

        # Budujemy frames – w tym rozbijamy pary w scenario
        scenario_frames = scenarios | "BuildFrames" >> beam.ParDo(BuildScenarioFramesFn())

        # Zbieramy
        scenario_list = scenario_frames | "ToList" >> beam.combiners.ToList()

        # JSON
        scenario_json = scenario_list | "ToJSON" >> beam.Map(scenario_list_to_json)

        # Zapis
        scenario_json | "WriteSingleFile" >> WriteToFiles(
            path=filename,
            max_writers_per_bundle=1,
            sink=SingleScenarioJSONSink()
        )


if __name__ == "__main__":
    run()