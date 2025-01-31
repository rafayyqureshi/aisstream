# collision_dofn.py

import time
import logging
import apache_beam as beam
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.coders import FloatCoder

# Import funkcji z cpa_utils.py
from cpa_utils import (
    compute_cpa_tcpa,
    local_distance_nm,
    is_approaching
)

# Ustawienia progów
CPA_THRESHOLD = 2.0            # mile morskie
TCPA_THRESHOLD = 20.0          # minuty
# Próg filtrowania dystansu – tutaj ustawiony na 5 Nm (możesz dostosować)
DISTANCE_THRESHOLD_NM = 5.0

# Użyjemy stanu do przechowywania minimalnego dystansu dla danej pary.
# Ponieważ ValueStateSpec nie zawsze jest dostępny, symulujemy stan jako BagState z jednym elementem.
MIN_DIST_STATE = BagStateSpec('min_dist', FloatCoder())

class CollisionGeneratorDoFn(beam.DoFn):
    """
    DoFn przyjmujący elementy w postaci:
       (geohash, [lista_statków])
    i generujący dla każdej pary statków (shipA, shipB) rekord kolizyjny, jeśli:
       - aktualny dystans < DISTANCE_THRESHOLD_NM,
       - funkcja is_approaching zwraca True,
       - obliczone CPA < CPA_THRESHOLD oraz 0 <= TCPA < TCPA_THRESHOLD.
       
    Side input `static_side` (dict: mmsi -> dane statyczne) służy do uzupełnienia nazwy statku.
    Rekord wyjściowy ma strukturę: (pair_key, record), gdzie pair_key to uporządkowana para (mmsi_a, mmsi_b).
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

                # Krok 1: Oblicz dystans między statkami
                dist_nm = local_distance_nm(shipA, shipB)
                logging.warning(
                    f"[CollisionGeneratorDoFn] GH={geohash} dist_nm={dist_nm:.3f} nm for pair "
                    f"({shipA['mmsi']}, {shipB['mmsi']})"
                )
                if dist_nm > DISTANCE_THRESHOLD_NM:
                    continue

                # Krok 2: Sprawdź, czy statki się zbliżają (is_approaching)
                approaching = is_approaching(shipA, shipB)
                logging.warning(
                    f"[CollisionGeneratorDoFn] GH={geohash} is_approaching={approaching} for pair "
                    f"({shipA['mmsi']}, {shipB['mmsi']})"
                )
                if not approaching:
                    continue

                # Krok 3: Oblicz CPA i TCPA
                cpa, tcpa = compute_cpa_tcpa(shipA, shipB)
                logging.warning(
                    f"[CollisionGeneratorDoFn] GH={geohash} cpa={cpa:.3f}, tcpa={tcpa:.3f} for pair "
                    f"({shipA['mmsi']}, {shipB['mmsi']})"
                )
                if cpa < CPA_THRESHOLD and 0 <= tcpa < TCPA_THRESHOLD:
                    mA = shipA["mmsi"]
                    mB = shipB["mmsi"]
                    pair_key = tuple(sorted([mA, mB]))

                    # Pobierz dane statyczne – nazwy statków, itd.
                    infoA = static_side.get(mA, {})
                    infoB = static_side.get(mB, {})
                    nameA = infoA.get("ship_name", "Unknown")
                    nameB = infoB.get("ship_name", "Unknown")

                    # Przygotowujemy rekord – is_active ustawiamy na None (do uzupełnienia w kolejnym etapie)
                    record = {
                        "mmsi_a": mA,
                        "ship_name_a": nameA,
                        "mmsi_b": mB,
                        "ship_name_b": nameB,
                        "timestamp": shipB["timestamp"],
                        "cpa": cpa,
                        "tcpa": tcpa,
                        "distance": dist_nm,
                        "is_active": None,  # Do ustalenia w kolejnym etapie
                        "latitude_a": shipA["latitude"],
                        "longitude_a": shipA["longitude"],
                        "latitude_b": shipB["latitude"],
                        "longitude_b": shipB["longitude"]
                    }
                    yield (pair_key, record)

class CollisionPairDoFn(beam.DoFn):
    """
    Stateful DoFn kluczowany według pary statków (pair_key).
    Używa stanu (przechowywanego w BagState) do zapamiętania minimalnego dystansu
    dla danej pary.
    
    Jeśli bieżący dystans (record['distance']) jest mniejszy lub równy zapisanej minimalnej wartości,
    aktualizuje stan i ustawia is_active na True. Jeśli dystans rośnie – is_active ustawiane jest na False.
    """
    MIN_DIST_STATE = BagStateSpec('min_dist', FloatCoder())

    def process(self, element):
        pair_key, record = element
        current_distance = record['distance']

        # Odczytaj stan – minimalny dystans zapisany dla tej pary.
        current_state = list(self.MIN_DIST_STATE.read())
        if current_state:
            min_dist = current_state[0]
        else:
            min_dist = current_distance

        # Porównaj bieżący dystans z minimalnym
        if current_distance <= min_dist:
            min_dist = current_distance
            is_active = True
        else:
            is_active = False

        # Zaktualizuj stan – wyczyść i zapisz nowy minimalny dystans
        self.MIN_DIST_STATE.clear()
        self.MIN_DIST_STATE.add(min_dist)

        record['is_active'] = is_active

        logging.info(
            f"[CollisionPairDoFn] Pair {pair_key}: current_distance={current_distance:.3f}, "
            f"min_dist={min_dist:.3f}, is_active={is_active}"
        )

        yield record