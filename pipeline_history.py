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

# ------------------------------------------------------------
# 1) Interpolacja pozycji (co 60s)
# ------------------------------------------------------------
def interpolate_positions(records, step_seconds=60):
    """
    records: [(ts, lat, lon, sog, cog), ...], posortowane rosnąco po ts.
    Zwraca listę klatek co step_seconds (domyślnie 60).
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
        while idx < len(records) - 1 and records[idx+1][0] < current_t:
            idx += 1

        if idx == len(records) - 1:
            # Ostatni punkt
            _, la, lo, sg, cg = records[-1]
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
                    lat_i = laA + (laB - laA) * ratio
                    lon_i = loA + (loB - loA) * ratio
                    sg_i  = sgA + (sgB - sgA) * ratio
                    cg_i  = cgA + (cgB - cgA) * ratio
                    out.append((current_t, lat_i, lon_i, sg_i, cg_i))

        current_t += datetime.timedelta(seconds=step_seconds)
    return out

# ------------------------------------------------------------
# 2) DedupCollisionsFn – minimalne cpa
# ------------------------------------------------------------
class DedupCollisionsFn(beam.DoFn):
    """
    Zbiera kolizje w 1 godzinie, tworzy mapę (mmsiA,mmsiB)->collision,
    wybiera minimalne cpa, aby uniknąć duplikatów tej samej pary.
    """
    def process(self, collisions):
        from collections import defaultdict
        pair_map = {}
        for c in collisions:
            a = min(c["mmsi_a"], c["mmsi_b"])
            b = max(c["mmsi_a"], c["mmsi_b"])
            key = (a, b)
            old = pair_map.get(key)
            if not old:
                pair_map[key] = c
            else:
                # Bierzemy kolizję o najmniejszym cpa
                if c["cpa"] < old["cpa"]:
                    pair_map[key] = c
        yield list(pair_map.values())

# ------------------------------------------------------------
# 3) GroupCollisionsIntoScenarios – umbrella (graf spójny)
# ------------------------------------------------------------
class GroupCollisionsIntoScenarios(beam.DoFn):
    def process(self, collisions_list):
        collisions_list.sort(key=lambda c: c["timestamp"])
        graph = defaultdict(set)
        col_map = []
        for c in collisions_list:
            a = c["mmsi_a"]
            b = c["mmsi_b"]
            graph[a].add(b)
            graph[b].add(a)
            col_map.append(c)

        visited = set()

        def bfs(start):
            queue = deque([start])
            visited.add(start)
            comp = {start}
            while queue:
                cur = queue.popleft()
                for neigh in graph[cur]:
                    if neigh not in visited:
                        visited.add(neigh)
                        comp.add(neigh)
                        queue.append(neigh)
            return comp

        all_mmsi = set(graph.keys())
        while all_mmsi - visited:
            start = (all_mmsi - visited).pop()
            comp_set = bfs(start)

            relevant = []
            min_ts = None
            max_ts = None
            for c in col_map:
                if c["mmsi_a"] in comp_set and c["mmsi_b"] in comp_set:
                    relevant.append(c)
                    t = c["timestamp"]
                    if (min_ts is None) or t < min_ts:
                        min_ts = t
                    if (max_ts is None) or t > max_ts:
                        max_ts = t

            scenario_id = f"scenario_{int(min_ts.timestamp()) if min_ts else 0}_" \
                          + "_".join(map(str, sorted(comp_set)))

            yield {
                "_parent": True,  # flaga umbrella
                "scenario_id": scenario_id,
                "ships_involved": sorted(list(comp_set)),
                "collisions_in_scenario": relevant,
                "min_ts": min_ts,
                "max_ts": max_ts
            }

# ------------------------------------------------------------
# 4) BuildScenarioFramesFn – sub-scenariusze (A–B), z wypełnieniem luki
# ------------------------------------------------------------
class BuildScenarioFramesFn(beam.DoFn):
    def setup(self):
        self.bq_client = bigquery.Client()

    def _haversine_nm(self, lat1, lon1, lat2, lon2):
        R_earth_nm = 3440.065
        dLat = math.radians(lat2 - lat1)
        dLon = math.radians(lon2 - lon1)
        a = (math.sin(dLat/2)**2 +
             math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dLon/2)**2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        return R_earth_nm * c

    def process(self, umbrella):
        # Jeśli to już sub-scenario => emit bez zmian
        if not umbrella.get("_parent"):
            yield umbrella
            return

        scenario_id = umbrella["scenario_id"]
        ships_all  = umbrella["ships_involved"] or []
        collisions = umbrella["collisions_in_scenario"] or []
        min_ts     = umbrella["min_ts"]
        max_ts     = umbrella["max_ts"]

        # Emisja "umbrella" info do listy
        parent_obj = {
            "_parent": True,
            "scenario_id": scenario_id,
            "ships_involved": ships_all,
            "collisions_count": len(collisions),
            "min_ts": min_ts.isoformat() if min_ts else None,
            "max_ts": max_ts.isoformat() if max_ts else None
        }
        yield parent_obj

        if not collisions or not min_ts or not max_ts:
            return

        # Zakres [min_ts-15, max_ts+5]
        global_start = min_ts - datetime.timedelta(minutes=15)
        global_end   = max_ts + datetime.timedelta(minutes=5)

        # Pobranie AIS
        str_mmsi = ",".join(map(str, ships_all))
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
        WHERE mmsi IN ({str_mmsi})
          AND timestamp BETWEEN '{global_start.isoformat()}' AND '{global_end.isoformat()}'
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
            data_map[r.mmsi].append((t, la, lo, sg, cg))
            name_map[r.mmsi] = r.ship_name or str(r.mmsi)
            len_map[r.mmsi]  = r.ship_length or 0

        # (A) Interpolacja + last_known => full_time_map[tstamp][mmsi]
        full_time_map = defaultdict(dict)
        last_known = {mm: None for mm in ships_all}  # do wypełniania luk

        # 1) interpolacja dla każdego statku
        for mm in ships_all:
            arr = data_map[mm]
            if not arr:
                continue
            interped = interpolate_positions(arr, 60)
            nm = name_map[mm]
            ln = len_map[mm]

            for (ts, la, lo, sg, cg) in interped:
                key_ts = ts.replace(microsecond=0)
                full_time_map[key_ts][mm] = {
                    "mmsi": mm,
                    "name": nm,
                    "ship_length": ln,
                    "lat": la,
                    "lon": lo,
                    "sog": sg,
                    "cog": cg
                }
                last_known[mm] = full_time_map[key_ts][mm]

        # 2) Uzupełniamy luki – w każdej minucie każdy statek
        all_times = sorted(full_time_map.keys())
        for tstamp in all_times:
            # sprawdzamy, czy mamy info dla każdego mm
            for mm in ships_all:
                if mm not in full_time_map[tstamp]:
                    # wstawiamy kopię last_known
                    if last_known[mm] is not None:
                        clone = dict(last_known[mm])  # kopia
                        # lat, lon, sog, cog z last_known
                        # (zakładamy, że "statek stoi" jeśli brak nowych danych)
                        full_time_map[tstamp][mm] = clone
            # update last_known
            for mm in ships_all:
                if mm in full_time_map[tstamp]:
                    last_known[mm] = full_time_map[tstamp][mm]

        # (B) Budowa sub-scenariuszy (A–B)
        for c in collisions:
            collision_id = c["collision_id"]
            a = c["mmsi_a"]
            b = c["mmsi_b"]
            cpa_val = c["cpa"]
            t_min   = c["timestamp"]
            if not t_min:
                continue

            sub_start = t_min - datetime.timedelta(minutes=15)
            sub_end   = t_min + datetime.timedelta(minutes=5)

            frames = []
            icon_lat = None
            icon_lon = None

            for tstamp in all_times:
                if tstamp < sub_start or tstamp > sub_end:
                    continue
                # mamy dictionary: full_time_map[tstamp] = { mmsi -> {...}, ...}
                ships_dict = full_time_map[tstamp]
                # konwertujemy na listę
                shipsArr = list(ships_dict.values())

                posA = ships_dict.get(a)
                posB = ships_dict.get(b)

                dist_nm = None
                if posA and posB:
                    dist_nm = self._haversine_nm(posA["lat"], posA["lon"],
                                                 posB["lat"], posB["lon"])
                    # ikona, jeśli tstamp bliski t_min
                    if abs((tstamp - t_min).total_seconds()) < 1.0:
                        icon_lat = 0.5*(posA["lat"] + posB["lat"])
                        icon_lon = 0.5*(posA["lon"] + posB["lon"])

                delta_minutes = round((tstamp - t_min).total_seconds()/60.0, 2)

                frames.append({
                    "time": tstamp.isoformat(),
                    "shipPositions": shipsArr,
                    "focus_dist": dist_nm,
                    "delta_minutes": delta_minutes
                })

            # fallback – ikona
            if icon_lat is None and icon_lon is None and frames:
                # Najbliższa klatka do t_min
                best_fr = None
                best_dt = 999999
                for fr in frames:
                    fts = datetime.datetime.fromisoformat(fr["time"])
                    diff_sec = abs((fts - t_min).total_seconds())
                    if diff_sec < best_dt:
                        best_dt=diff_sec
                        best_fr=fr
                if best_fr:
                    posA = None
                    posB = None
                    for sdat in best_fr["shipPositions"]:
                        if sdat["mmsi"] == a: posA=sdat
                        if sdat["mmsi"] == b: posB=sdat
                    if posA and posB:
                        icon_lat=0.5*(posA["lat"]+posB["lat"])
                        icon_lon=0.5*(posA["lon"]+posB["lon"])

            # Tytuł
            nmA = name_map.get(a) or str(a)
            nmB = name_map.get(b) or str(b)
            dist_str = f"{cpa_val:.3f} nm"
            scTitle = f"{nmA} – {nmB}, {dist_str}"

            yield {
                "_parent": False,
                "scenario_id": scenario_id,
                "collision_id": collision_id,
                "focus_mmsi": [a,b],
                "title": scTitle,
                "cpa": cpa_val,
                "t_min": t_min.isoformat(),
                "all_involved_mmsi": ships_all,
                "icon_lat": icon_lat,
                "icon_lon": icon_lon,
                "frames": frames
            }

# ------------------------------------------------------------
# 5) Konwersja do JSON
# ------------------------------------------------------------
def scenario_list_to_json(items):
    arr = list(items)
    return json.dumps({"scenarios": arr}, default=str, indent=2)

class SingleScenarioJSONSink(FileSink):
    def open(self, fh):
        self._fh = fh
    def write(self, element):
        self._fh.write(element.encode("utf-8"))
        self._fh.write(b"\n")
    def flush(self):
        pass

# ------------------------------------------------------------
# 6) main potok
# ------------------------------------------------------------
def run():
    logging.getLogger().setLevel(logging.INFO)
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = False

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
      mmsi_a, mmsi_b,
      timestamp,
      cpa, tcpa,
      latitude_a, longitude_a,
      latitude_b, longitude_b
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
        collisions_list = collisions | "GroupAll" >> beam.combiners.ToList()

        deduped = collisions_list | "Deduplicate" >> beam.ParDo(DedupCollisionsFn())
        umbrella = deduped | "GroupCollisions" >> beam.ParDo(GroupCollisionsIntoScenarios())
        scenario_frames = umbrella | "BuildFrames" >> beam.ParDo(BuildScenarioFramesFn())

        final_list = scenario_frames | "ToList" >> beam.combiners.ToList()
        scenario_json = final_list | "MakeJSON" >> beam.Map(scenario_list_to_json)

        scenario_json | "WriteFile" >> WriteToFiles(
            path=filename,
            max_writers_per_bundle=1,
            sink=SingleScenarioJSONSink()
        )

if __name__ == "__main__":
    run()