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
        pair_map = {}
        for c in collisions:
            a = min(c["mmsi_a"], c["mmsi_b"])
            b = max(c["mmsi_a"], c["mmsi_b"])
            key = (a, b)
            old = pair_map.get(key)
            if not old:
                pair_map[key] = c
            else:
                # Wybieramy kolizję o najmniejszym cpa
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
# 4) BuildScenarioFramesFn – sub-scenariusze (A–B), z wide oknem, wyliczaniem real_min_dist, itd.
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
        # Jeśli to już sub-scenario => emit bez zmian (zazwyczaj nie wystąpi, ale na wszelki wypadek)
        if not umbrella.get("_parent"):
            yield umbrella
            return

        scenario_id = umbrella["scenario_id"]
        ships_all  = umbrella["ships_involved"] or []
        collisions = umbrella["collisions_in_scenario"] or []
        min_ts     = umbrella["min_ts"]
        max_ts     = umbrella["max_ts"]

        # Emitujemy umbrella (zbiorczy) obiekt
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

        # Budowa sub-scenariuszy (A–B) z szerokim oknem i wyznaczeniem real_min_dist
        for c in collisions:
            collision_id = c["collision_id"]
            a = c["mmsi_a"]
            b = c["mmsi_b"]
            cpa_val = c["cpa"]  # oryginalne cpa z BQ
            t_min   = c["timestamp"]
            if not t_min:
                continue

            # 1) Ustalamy szerokie okno: [t_min - 20, t_min + 10]
            wide_start = t_min - datetime.timedelta(minutes=20)
            wide_end   = t_min + datetime.timedelta(minutes=10)

            # 2) Pobranie AIS z wide okna
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
              AND timestamp BETWEEN '{wide_start.isoformat()}' AND '{wide_end.isoformat()}'
            GROUP BY mmsi, timestamp, latitude, longitude, sog, cog
            ORDER BY timestamp
            """
            rows = list(self.bq_client.query(query).result())

            data_map = defaultdict(list)
            name_map = {}
            len_map  = {}

            for r in rows:
                ts = r.timestamp
                la = float(r.latitude or 0.0)
                lo = float(r.longitude or 0.0)
                sg = float(r.sog or 0.0)
                cg = float(r.cog or 0.0)
                data_map[r.mmsi].append((ts, la, lo, sg, cg))
                name_map[r.mmsi] = r.ship_name or str(r.mmsi)
                len_map[r.mmsi]  = r.ship_length or 0

            # 3) Interpolacja (co 60s) + fill-luki => full_time_map[tstamp][mmsi]
            full_time_map = defaultdict(dict)
            last_known = {mm: None for mm in ships_all}

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

            # fill-luki (jeśli w danej minucie brak wpisu dla statku => użyj last_known)
            all_times = sorted(full_time_map.keys())
            for tstamp in all_times:
                for mm in ships_all:
                    if mm not in full_time_map[tstamp]:
                        if last_known[mm] is not None:
                            clone = dict(last_known[mm])
                            full_time_map[tstamp][mm] = clone
                for mm in ships_all:
                    if mm in full_time_map[tstamp]:
                        last_known[mm] = full_time_map[tstamp][mm]

            # 4) Znajdź real_min_dist + real_min_time w tym szerokim oknie
            real_min_dist = 999999.0
            real_min_time = t_min  # fallback

            framesWide = []  # przechowa (tstamp, ships_dict, distAB)

            for tstamp in all_times:
                if tstamp < wide_start or tstamp > wide_end:
                    continue
                ships_dict = full_time_map[tstamp]
                distAB = None
                posA = ships_dict.get(a)
                posB = ships_dict.get(b)
                if posA and posB:
                    distAB = self._haversine_nm(posA["lat"], posA["lon"],
                                                posB["lat"], posB["lon"])
                    if distAB < real_min_dist:
                        real_min_dist = distAB
                        real_min_time = tstamp

                framesWide.append((tstamp, ships_dict, distAB))

            # 5) Przycinamy do finalnych 20 min: [real_min_time - 15, real_min_time + 5]
            final_start = real_min_time - datetime.timedelta(minutes=15)
            final_end   = real_min_time + datetime.timedelta(minutes=5)

            frames = []
            best_dist = 999999
            best_dist_ts = real_min_time

            for (tstamp, ships_dict, distAB) in framesWide:
                if tstamp < final_start or tstamp > final_end:
                    continue
                shipsArr = list(ships_dict.values())
                delta_minutes = round((tstamp - real_min_time).total_seconds() / 60.0, 2)

                if distAB is not None and distAB < best_dist:
                    best_dist = distAB
                    best_dist_ts = tstamp

                frames.append({
                    "time": tstamp.isoformat(),
                    "shipPositions": shipsArr,
                    "focus_dist": distAB,
                    "delta_minutes": delta_minutes
                })

            # 6) Ustal miejsce ikony "C" (w momencie best_dist_ts)
            icon_lat = None
            icon_lon = None
            if frames:
                # Wybieramy klatkę najbliższą best_dist_ts
                closest_frame = min(
                    frames,
                    key=lambda fr: abs(datetime.datetime.fromisoformat(fr["time"]) - best_dist_ts)
                )
                posA = None
                posB = None
                for sdat in closest_frame["shipPositions"]:
                    if sdat["mmsi"] == a:
                        posA = sdat
                    if sdat["mmsi"] == b:
                        posB = sdat
                if posA and posB:
                    icon_lat = 0.5*(posA["lat"] + posB["lat"])
                    icon_lon = 0.5*(posA["lon"] + posB["lon"])

            # 7) Tytuł, final emission
            nmA = name_map.get(a) or str(a)
            nmB = name_map.get(b) or str(b)
            dist_str = f"{real_min_dist:.3f} nm"
            scTitle = f"{nmA} – {nmB}, realMinDist={dist_str}"

            yield {
                "_parent": False,
                "scenario_id": scenario_id,
                "collision_id": collision_id,
                "focus_mmsi": [a, b],
                "title": scTitle,
                # cpaVal or real_min_dist? Zapisujemy real_min_dist 
                "cpa": real_min_dist,
                "t_min": real_min_time.isoformat(),
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

    # cpa < 0.3 => minimalny dystans (jeśli chcesz, możesz ustawić 0.5)
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
        # 1) Odczyt kolizji z BQ
        collisions = (
            p
            | "ReadCollisions" >> beam.io.ReadFromBigQuery(
                query=query,
                use_standard_sql=True
                # location="us-east1"  # jeśli Twój dataset jest w regionie us-east1, dodaj to
            )
        )

        # 2) Grupujemy w 1 listę
        collisions_list = collisions | "GroupAll" >> beam.combiners.ToList()

        # 3) Deduplikacja par A–B
        deduped = collisions_list | "Deduplicate" >> beam.ParDo(DedupCollisionsFn())

        # 4) Budowa umbrella (graf spójny)
        umbrella = deduped | "GroupCollisions" >> beam.ParDo(GroupCollisionsIntoScenarios())

        # 5) Budowa klatek sub-scenariusza (z wide oknem i real_min_dist)
        scenario_frames = umbrella | "BuildFrames" >> beam.ParDo(BuildScenarioFramesFn())

        # 6) Zapis do listy
        final_list = scenario_frames | "ToList" >> beam.combiners.ToList()
        scenario_json = final_list | "MakeJSON" >> beam.Map(scenario_list_to_json)

        # 7) Zapis do GCS w formacie JSON
        scenario_json | "WriteFile" >> WriteToFiles(
            path=filename,
            max_writers_per_bundle=1,
            sink=SingleScenarioJSONSink()
        )

if __name__ == "__main__":
    run()