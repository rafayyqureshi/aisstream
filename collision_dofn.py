# collision_dofn.py

import time
import apache_beam as beam
import apache_beam.coders
from apache_beam.transforms.userstate import BagStateSpec

# Z pliku cpa_utils.py importuj potrzebne funkcje
from cpa_utils import (
    compute_cpa_tcpa,
    local_distance_nm,
    is_approaching
)

# Ustawiamy podstawowe progi (możesz je dopasować do potrzeb)
CPA_THRESHOLD        = 0.5
TCPA_THRESHOLD       = 10.0
STATE_RETENTION_SEC  = 120   # 2 min
DISTANCE_THRESHOLD_NM = 5.0

class CollisionDoFn(beam.DoFn):
    """
    Stateful DoFn do wykrywania kolizji w ramach geohash.
    Wstępne filtrowanie:
      1) lokalny dystans < 5 nm,
      2) statki się zbliżają (dot product < 0).
    Następnie obliczanie CPA/TCPA.
    Dodatkowo:
      - side input z ships_static => pobieramy ship_name,
      - obliczamy distance (aktualny),
      - is_active => true, jeśli aktualny distance <= minimalny distance dotąd; 
        gdy distance zacznie rosnąć, is_active=false.
    """

    RECORDS_STATE = BagStateSpec(
        "records_state",
        beam.coders.TupleCoder((
            beam.coders.FastPrimitivesCoder(),
            beam.coders.FastPrimitivesCoder()
        ))
    )
    PAIRS_STATE = BagStateSpec(
        "pairs_state",
        beam.coders.TupleCoder((
            beam.coders.FastPrimitivesCoder(),
            beam.coders.FastPrimitivesCoder()
        ))
    )

    def __init__(self, static_side):
        super().__init__()
        self.static_side = static_side  # side input (dict)

    def process(
        self,
        element,
        records_state=beam.DoFn.StateParam(RECORDS_STATE),
        pairs_state=beam.DoFn.StateParam(PAIRS_STATE)
    ):
        gh, ship = element
        now_sec = time.time()

        # 1. Zapis do stanu (trzymamy listę statków w tym geohash)
        records_state.add((ship, now_sec))

        # 2. Czytamy dotychczasowe dane i czyścimy stare
        old_list = list(records_state.read())
        fresh = [(s, t) for (s, t) in old_list if (now_sec - t) <= STATE_RETENTION_SEC]

        records_state.clear()
        for (s, t) in fresh:
            records_state.add((s, t))

        # 3. Side input => dict {mmsi: {ship_name, ...}}
        static_dict = self.static_side

        # 4. Odczyt pairs_state => minimalny dystans per para
        pairs_list = list(pairs_state.read())
        pairs_dict = {}
        for pair_key, dist_val in pairs_list:
            pairs_dict[pair_key] = dist_val
        pairs_state.clear()  # wyczyść – zapiszemy aktualne wartości

        # 5. Szukamy kolizji
        for (old_ship, _) in fresh:
            if old_ship["mmsi"] == ship["mmsi"]:
                continue

            dist_nm = local_distance_nm(old_ship, ship)
            if dist_nm > DISTANCE_THRESHOLD_NM:
                continue
            if not is_approaching(old_ship, ship):
                continue

            cpa, tcpa = compute_cpa_tcpa(old_ship, ship)
            if cpa < CPA_THRESHOLD and 0 <= tcpa < TCPA_THRESHOLD:
                # Mamy kolizję
                mA = old_ship["mmsi"]
                mB = ship["mmsi"]
                pair_key = tuple(sorted([mA, mB]))

                prev_min_dist = pairs_dict.get(pair_key, None)
                if prev_min_dist is None:
                    prev_min_dist = dist_nm

                # is_active => false jeśli aktualny dist_nm > prev_min_dist
                is_active = dist_nm <= prev_min_dist

                new_min_dist = min(prev_min_dist, dist_nm)
                pairs_dict[pair_key] = new_min_dist

                # Nazwy statków
                infoA = static_dict.get(mA, {})
                infoB = static_dict.get(mB, {})
                nameA = infoA.get("ship_name", "Unknown")
                nameB = infoB.get("ship_name", "Unknown")

                yield {
                    "mmsi_a": mA,
                    "ship_name_a": nameA,
                    "mmsi_b": mB,
                    "ship_name_b": nameB,
                    "timestamp": ship["timestamp"],
                    "cpa": cpa,
                    "tcpa": tcpa,
                    "distance": dist_nm,
                    "is_active": is_active,
                    "latitude_a": old_ship["latitude"],
                    "longitude_a": old_ship["longitude"],
                    "latitude_b": ship["latitude"],
                    "longitude_b": ship["longitude"]
                }

        # 6. Zapisz pairs_dict do pairs_state
        for pk, dval in pairs_dict.items():
            pairs_state.add((pk, dval))