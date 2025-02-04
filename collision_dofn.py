import time
import logging
import apache_beam as beam
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.coders import FloatCoder

# Import funkcji z cpa_utils.py (lub innych plików, jeżeli są)
from cpa_utils import (
    compute_cpa_tcpa,
    local_distance_nm,
    is_approaching
)

# Ustawienia progów
CPA_THRESHOLD       = 0.5   # mile morskie
TCPA_THRESHOLD      = 10.0  # minuty
STATE_RETENTION_SEC = 120   # 2 minuty
DISTANCE_THRESHOLD_NM = 5.0 # mile morskie

class CollisionGeneratorDoFn(beam.DoFn):
    """
    Dla każdej grupy statków (o tym samym geohash) tworzy rekordy kolizyjne.
    Zasady:
      1) shipA["mmsi"] != shipB["mmsi"] (pomijamy statki z tym samym MMSI).
      2) Dystans <= DISTANCE_THRESHOLD_NM.
      3) Statki się zbliżają (is_approaching).
      4) CPA < CPA_THRESHOLD oraz 0 <= TCPA < TCPA_THRESHOLD.

    Dodajemy też ustalanie, że 'shipA' jest statkiem o WYŻSZYM MMSI – wówczas
    para (shipA, shipB) i (shipB, shipA) nie powtarza się, bo generujemy tylko jedną.
    
    Side input `static_side` (dict: mmsi -> dane statyczne statku) – by
    dopełnić nazwy i wymiary (jeśli potrzeba). Wygenerowany rekord ma formę:
      (pair_key, record)
    gdzie pair_key to (mmsiA, mmsiB).
    """
    def __init__(self, static_side):
        super().__init__()
        self.static_side = static_side  # side input

    def process(self, element, static_side):
        geohash, ships = element

        for i in range(len(ships)):
            for j in range(i + 1, len(ships)):
                shipA = ships[i]
                shipB = ships[j]

                # 1) Ten sam MMSI => pomijamy
                if shipA["mmsi"] == shipB["mmsi"]:
                    continue

                # NOWA LOGIKA: ustalamy, że shipA jest tym z wyższym MMSI
                # (lub odwrotnie, byle spójnie).
                if shipA["mmsi"] < shipB["mmsi"]:
                    # Zamieniamy w całości, tak by finalnie:
                    #  shipA.mmsi >= shipB.mmsi
                    tmp = shipA
                    shipA = shipB
                    shipB = tmp

                # Sprawdzamy dystans
                dist_nm = local_distance_nm(shipA, shipB)
                logging.debug(
                    f"[CollisionGeneratorDoFn] GH={geohash}, dist={dist_nm:.3f} nm "
                    f"({shipA['mmsi']}, {shipB['mmsi']})"
                )
                if dist_nm > DISTANCE_THRESHOLD_NM:
                    continue

                # Zbliżanie?
                approaching = is_approaching(shipA, shipB)
                if not approaching:
                    continue

                # CPA/TCPA
                cpa, tcpa = compute_cpa_tcpa(shipA, shipB)
                if cpa < CPA_THRESHOLD and 0 <= tcpa < TCPA_THRESHOLD:
                    mA = shipA["mmsi"]
                    mB = shipB["mmsi"]

                    infoA = static_side.get(mA, {})
                    infoB = static_side.get(mB, {})
                    nameA = infoA.get("ship_name", "Unknown")
                    nameB = infoB.get("ship_name", "Unknown")

                    record = {
                        "mmsi_a": mA,
                        "ship_name_a": nameA,
                        "mmsi_b": mB,
                        "ship_name_b": nameB,
                        "timestamp": shipB["timestamp"],  # albo np. shipA["timestamp"], w zależności od konwencji
                        "cpa": cpa,
                        "tcpa": tcpa,
                        "distance": dist_nm,
                        "is_active": None,  # Uzupełniane w CollisionPairDoFn
                        "latitude_a": shipA["latitude"],
                        "longitude_a": shipA["longitude"],
                        "latitude_b": shipB["latitude"],
                        "longitude_b": shipB["longitude"]
                    }

                    # pair_key: (mmsiA, mmsiB). Tu mmsiA >= mmsiB, więc klucz zawsze w tej samej kolejności
                    pair_key = (mA, mB)

                    yield (pair_key, record)


class CollisionPairDoFn(beam.DoFn):
    """
    Ustalanie flagi is_active w oparciu o minimalny historyczny dystans (BagStateSpec).
    Jeżeli bieżący dystans <= min_dist w stanie => is_active = True i aktualizujemy min_dist,
    w przeciwnym wypadku is_active = False.
    """
    MIN_DIST_STATE = BagStateSpec('min_dist', FloatCoder())

    def process(self, element, min_dist_state=beam.DoFn.StateParam(MIN_DIST_STATE)):
        pair_key, record = element
        current_distance = record['distance']

        old_vals = list(min_dist_state.read())
        if old_vals:
            min_dist = old_vals[0]
        else:
            min_dist = current_distance

        if current_distance <= min_dist:
            is_active = True
            min_dist = current_distance
        else:
            is_active = False

        min_dist_state.clear()
        min_dist_state.add(min_dist)

        record['is_active'] = is_active

        logging.info(
            f"[CollisionPairDoFn] Pair {pair_key} distance={current_distance:.3f}, "
            f"min_dist={min_dist:.3f}, is_active={is_active}"
        )

        yield record