# collision_dofn.py

import logging
import apache_beam as beam

# Import przydatnych funkcji z cpa_utils:
from cpa_utils import (
    compute_cpa_tcpa,
    local_distance_nm,
    is_approaching
)

# Ustawiamy progi
CPA_THRESHOLD        = 2.0   # nm
TCPA_THRESHOLD       = 20.0  # min
DISTANCE_THRESHOLD_NM= 10.0  # nm

class CollisionDoFn(beam.DoFn):
    """
    DoFn wykrywania kolizji po zgrupowaniu statków w ramach geohash.
    Otrzymujemy (geohash, [lista_statkow]), i w pętli tworzymy pary (A,B).
    
    Warunki wstępne:
      1) distance < DISTANCE_THRESHOLD_NM
      2) is_approaching(...) == True
      3) cpa < CPA_THRESHOLD
      4) 0 <= tcpa < TCPA_THRESHOLD
    
    Dodatkowo (na potrzeby zapisu do collisions):
      - side input z ships_static => pobieramy ship_name
      - liczymy distance (aktualny)
      - is_active => placeholder, jeśli chcesz dalej rozbudować logikę
    """

    def __init__(self, static_side):
        super().__init__()
        self.static_side = static_side  # side input: dict {mmsi -> {...}}

    def process(self, element, static_side):
        """
        element = (geohash, [list_of_ships])
        side_static => dict {mmsi -> {ship_name, dim_a, ...}}
        """
        geohash, ships_list = element

        # Dla każdego pair (i, j)
        for i in range(len(ships_list)):
            for j in range(i + 1, len(ships_list)):
                shipA = ships_list[i]
                shipB = ships_list[j]

                # Oblicz distance
                dist_nm = local_distance_nm(shipA, shipB)
                logging.warning(
                    f"[DetectCollisions] GH={geohash}, dist_nm={dist_nm:.3f} nm "
                    f"for pair ({shipA['mmsi']}, {shipB['mmsi']})"
                )
                if dist_nm > DISTANCE_THRESHOLD_NM:
                    continue

                approaching = is_approaching(shipA, shipB)
                logging.warning(
                    f"[DetectCollisions] GH={geohash}, is_approaching={approaching} "
                    f"for pair ({shipA['mmsi']}, {shipB['mmsi']})"
                )
                if not approaching:
                    continue

                # Oblicz cpa/tcpa
                cpa, tcpa = compute_cpa_tcpa(shipA, shipB)
                logging.warning(
                    f"[DetectCollisions] GH={geohash}, cpa={cpa:.3f}, tcpa={tcpa:.3f} "
                    f"for pair ({shipA['mmsi']}, {shipB['mmsi']})"
                )
                if cpa < CPA_THRESHOLD and 0 <= tcpa < TCPA_THRESHOLD:
                    # Kolizja
                    mA = shipA["mmsi"]
                    mB = shipB["mmsi"]

                    # Nazwy statków z side input
                    infoA = static_side.get(mA, {})
                    infoB = static_side.get(mB, {})
                    nameA = infoA.get("ship_name", "Unknown")
                    nameB = infoB.get("ship_name", "Unknown")

                    # is_active = True jako placeholder (np. do dalszej logiki)
                    # w razie potrzeby można dodać warunek rosnącego/mającego maleć dystansu
                    is_active = True

                    logging.info(
                        f"[DetectCollisions] YIELD collision for pair "
                        f"({mA}, {mB}) cpa={cpa:.3f}, tcpa={tcpa:.3f}, dist={dist_nm:.3f}, "
                        f"is_active={is_active}"
                    )

                    # Wybierz timestamp z któregoś statku (np. nowszy)
                    # lub można wziąć minimum/maximum. Poniżej np. bierzemy shipB
                    yield {
                        "mmsi_a": mA,
                        "ship_name_a": nameA,
                        "mmsi_b": mB,
                        "ship_name_b": nameB,
                        "timestamp": shipB["timestamp"],
                        "cpa": cpa,
                        "tcpa": tcpa,
                        "distance": dist_nm,
                        "is_active": is_active,
                        "latitude_a": shipA["latitude"],
                        "longitude_a": shipA["longitude"],
                        "latitude_b": shipB["latitude"],
                        "longitude_b": shipB["longitude"]
                    }