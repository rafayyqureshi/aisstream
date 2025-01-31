# collision_dofn.py

import time
import logging
import apache_beam as beam
from apache_beam.transforms.userstate import ValueStateSpec
from apache_beam.coders import FloatCoder

from cpa_utils import (
    compute_cpa_tcpa,
    local_distance_nm,
    is_approaching
)

# Ustawienia progów
CPA_THRESHOLD = 2.0            # mile morskie
TCPA_THRESHOLD = 20.0          # minuty
DISTANCE_THRESHOLD_NM = 10.0   # mile morskie

class CollisionGeneratorDoFn(beam.DoFn):
    """
    Transformacja przyjmująca elementy w postaci:
       (geohash, [lista_statków])
    i generująca dla każdej pary statków (shipA, shipB) rekord kolizyjny.
    
    Warunki:
      - Aktualny dystans między statkami < DISTANCE_THRESHOLD_NM,
      - Funkcja is_approaching zwraca True,
      - Obliczone CPA < CPA_THRESHOLD i 0 <= TCPA < TCPA_THRESHOLD.
    
    Side input `static_side` (dict, mmsi -> statyczne dane) służy do uzupełnienia nazw.
    Rekord wyjściowy jest kluczowany przez (mmsi_a, mmsi_b) (w porządku rosnącym).
    """
    def __init__(self, static_side):
        super().__init__()
        self.static_side = static_side

    def process(self, element, static_side):
        geohash, ships = element
        for i in range(len(ships)):
            for j in range(i + 1, len(ships)):
                shipA = ships[i]
                shipB = ships[j]

                dist_nm = local_distance_nm(shipA, shipB)
                logging.warning(
                    f"[DetectCollisions] GH={geohash} dist_nm={dist_nm:.3f} nm for pair "
                    f"({shipA['mmsi']}, {shipB['mmsi']})"
                )
                if dist_nm > DISTANCE_THRESHOLD_NM:
                    continue

                approaching = is_approaching(shipA, shipB)
                logging.warning(
                    f"[DetectCollisions] GH={geohash} is_approaching={approaching} for pair "
                    f"({shipA['mmsi']}, {shipB['mmsi']})"
                )
                if not approaching:
                    continue

                cpa, tcpa = compute_cpa_tcpa(shipA, shipB)
                logging.warning(
                    f"[DetectCollisions] GH={geohash} cpa={cpa:.3f}, tcpa={tcpa:.3f} for pair "
                    f"({shipA['mmsi']}, {shipB['mmsi']})"
                )
                if cpa < CPA_THRESHOLD and 0 <= tcpa < TCPA_THRESHOLD:
                    mA = shipA["mmsi"]
                    mB = shipB["mmsi"]
                    pair_key = tuple(sorted([mA, mB]))
                    
                    static_dict = static_side
                    infoA = static_dict.get(mA, {})
                    infoB = static_dict.get(mB, {})
                    nameA = infoA.get("ship_name", "Unknown")
                    nameB = infoB.get("ship_name", "Unknown")
                    
                    # Przygotowujemy rekord – flagę is_active ustawimy na None,
                    # bo zostanie uaktualniona przez kolejną transformację.
                    record = {
                        "mmsi_a": mA,
                        "ship_name_a": nameA,
                        "mmsi_b": mB,
                        "ship_name_b": nameB,
                        "timestamp": shipB["timestamp"],
                        "cpa": cpa,
                        "tcpa": tcpa,
                        "distance": dist_nm,
                        "is_active": None,  # Do ustalenia przez CollisionPairDoFn
                        "latitude_a": shipA["latitude"],
                        "longitude_a": shipA["longitude"],
                        "latitude_b": shipB["latitude"],
                        "longitude_b": shipB["longitude"]
                    }
                    yield (pair_key, record)

class CollisionPairDoFn(beam.DoFn):
    """
    Stateful transformacja kluczowana według pary statków (mmsi_a, mmsi_b).
    Używa ValueState do przechowywania minimalnego dystansu (min_dist) dla danej pary.
    Jeśli bieżący dystans jest mniejszy niż poprzednio zarejestrowany, aktualizuje stan i
    ustawia flagę is_active na True; jeśli bieżący dystans jest wyższy, flagę is_active ustawia na False.
    """
    MIN_DIST_STATE = ValueStateSpec('min_dist', FloatCoder())

    def process(self, element, static_side):
        pair_key, record = element
        current_distance = record['distance']
        min_dist = self.MIN_DIST_STATE.read()
        if min_dist is None:
            min_dist = current_distance
            self.MIN_DIST_STATE.write(min_dist)
            is_active = True
        else:
            if current_distance < min_dist:
                min_dist = current_distance
                self.MIN_DIST_STATE.write(min_dist)
                is_active = True
            else:
                is_active = False
        record['is_active'] = is_active
        logging.info(
            f"[CollisionPairDoFn] Pair {pair_key}: current_distance={current_distance:.3f}, "
            f"min_dist={min_dist:.3f}, is_active={is_active}"
        )
        yield record