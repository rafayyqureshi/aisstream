import os
import json
import math
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.userstate import BagStateSpec, on_timer, TimerSpec, TimeDomain

# Parametry wykrywania kolizji
CPA_THRESHOLD = 0.5     # mile morskie
TCPA_THRESHOLD = 10.0   # minuty
STATE_RETENTION_SEC = 30 # jak długo trzymamy stare rekordy w pamięci (np. 30 sek)

def parse_message(record):
    """
    Prosta funkcja parsująca wiadomość AIS z Pub/Sub.
    Zwraca None, jeśli nie spełnia minimalnych wymagań.
    """
    try:
        data = json.loads(record.decode('utf-8'))
        required = ['mmsi','latitude','longitude','cog','sog','timestamp']
        if not all(k in data for k in required):
            return None
        return data
    except:
        return None

def compute_cpa_tcpa(ship_a, ship_b):
    """
    Najlepszy (bardziej rozbudowany) algorytm obliczania CPA/TCPA,
    wykorzystujący skalowanie współrzędnych i prędkości.
    
    Założenia:
      - SOG w węzłach (nm/h).
      - COG w stopniach (0..359).
      - latitude, longitude w stopniach.
      - Brak kolizji, jeśli któryś statek <50m albo brak ship_length.
    """

    # Odrzucamy z obliczeń, jeśli statek <50m
    if (ship_a.get('ship_length') is None or ship_b.get('ship_length') is None
        or ship_a['ship_length'] < 50 or ship_b['ship_length'] < 50):
        return (9999, -1)

    # Współczynnik geograficzny
    latRef = (ship_a['latitude'] + ship_b['latitude']) / 2.0
    scaleLat = 111000.0
    scaleLon = 111000.0 * math.cos(math.radians(latRef))  # w przybliżeniu

    def to_xy(lat, lon):
        return (lon * scaleLon, lat * scaleLat)

    xA, yA = to_xy(ship_a['latitude'], ship_a['longitude'])
    xB, yB = to_xy(ship_b['latitude'], ship_b['longitude'])

    # SOG w węzłach (nm/h).
    # Do obliczeń w metrach/min trzeba przeskalować.
    sogA = ship_a['sog']  # nm/h
    sogB = ship_b['sog']

    def cog_to_vector(cog_deg, sog_nm_h):
        cog_rad = math.radians(cog_deg)
        # W standardzie AIS, COG=0 oznacza "północ", rośnie w prawo (kompas).
        # Przyjmujemy vx= sog*sin(cog), vy= sog*cos(cog)
        vx = sog_nm_h * math.sin(cog_rad)
        vy = sog_nm_h * math.cos(cog_rad)
        return vx, vy

    vxA_nm_per_h, vyA_nm_per_h = cog_to_vector(ship_a['cog'], sogA)
    vxB_nm_per_h, vyB_nm_per_h = cog_to_vector(ship_b['cog'], sogB)

    # Pozycje względne
    dx = xA - xB
    dy = yA - yB
    dvx_nm_per_h = vxA_nm_per_h - vxB_nm_per_h
    dvy_nm_per_h = vyA_nm_per_h - vyB_nm_per_h

    # Przeliczamy nm/h na m/min
    # 1 nm = 1852 m, nm/h = (1852/60) m/min
    speed_scale = 1852.0 / 60.0
    dvx_m_per_min = dvx_nm_per_h * speed_scale
    dvy_m_per_min = dvy_nm_per_h * speed_scale

    # Obliczenie VV i PV
    VV_m = dvx_m_per_min ** 2 + dvy_m_per_min ** 2  # prędkość względna ^2
    PV_m = dx * dvx_m_per_min + dy * dvy_m_per_min

    if VV_m == 0:
        # statki płyną identycznie
        tcpa = 0.0
    else:
        tcpa = - PV_m / VV_m

    # Jeśli TCPA < 0, kolizja wystąpiła w przeszłości
    if tcpa < 0:
        return (9999, -1)

    # Po tcpa minutach
    vxA_m_per_min = vxA_nm_per_h * speed_scale
    vyA_m_per_min = vyA_nm_per_h * speed_scale
    vxB_m_per_min = vxB_nm_per_h * speed_scale
    vyB_m_per_min = vyB_nm_per_h * speed_scale

    xA2 = xA + vxA_m_per_min * tcpa
    yA2 = yA + vyA_m_per_min * tcpa
    xB2 = xB + vxB_m_per_min * tcpa
    yB2 = yB + vyB_m_per_min * tcpa

    dist_m = math.sqrt((xA2 - xB2) ** 2 + (yA2 - yB2) ** 2)
    dist_nm = dist_m / 1852.0

    return (dist_nm, tcpa)

class CollisionDoFn(beam.DoFn):
    """
    Trzyma poprzednie rekordy w stanie (ok. 30 sek),
    porównuje z nowym, oblicza cpa, tcpa => jeśli cpa < 0.5 nm i tcpa < 10 min => kolizja.
    """
    RECORDS_STATE = BagStateSpec('records_state', beam.coders.FastPrimitivesCoder())
    TIMER = TimerSpec('cleanup', TimeDomain.WATERMARK)

    def process(self, element, records_state=beam.DoFn.StateParam(RECORDS_STATE),
                timer=beam.DoFn.TimerParam(TIMER)):
        geohash, ship = element

        # Dodaj bieżący record do stanu
        records_state.add(ship)

        # Sprawdź kolizje z poprzednimi
        old_ships = list(records_state.read())
        for old_ship in old_ships:
            cpa, tcpa = compute_cpa_tcpa(old_ship, ship)
            if cpa < CPA_THRESHOLD and tcpa >= 0 and tcpa < TCPA_THRESHOLD:
                yield {
                    'mmsi_a': old_ship['mmsi'],
                    'mmsi_b': ship['mmsi'],
                    'timestamp': ship['timestamp'],
                    'cpa': cpa,
                    'tcpa': tcpa,
                    'latitude_a': old_ship['latitude'],
                    'longitude_a': old_ship['longitude'],
                    'latitude_b': ship['latitude'],
                    'longitude_b': ship['longitude']
                }

        # Ustaw timer cleanup
        timer.set(timer.now() + STATE_RETENTION_SEC)

    @on_timer(TIMER)
    def cleanup(self, records_state=beam.DoFn.StateParam(RECORDS_STATE)):
        # Dla uproszczenia – czyścimy cały stan
        records_state.clear()

class MyPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_subscription', type=str, required=True)
        parser.add_argument('--collisions_topic', type=str, required=True)

def run():
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True

    input_subscription = pipeline_options.view_as(MyPipelineOptions).input_subscription
    collisions_topic = pipeline_options.view_as(MyPipelineOptions).collisions_topic

    with beam.Pipeline(options=pipeline_options) as p:
        lines = (
            p
            | 'ReadPubSub' >> beam.io.ReadFromPubSub(subscription=input_subscription)
        )

        parsed = (
            lines
            | 'ParseAIS' >> beam.Map(parse_message)
            | 'FilterNone' >> beam.Filter(lambda x: x is not None)
        )

        # Key by geohash – klucz do stateful processing
        keyed = (
            parsed
            | 'KeyByGeohash' >> beam.Map(lambda r: (r.get('geohash','none'), r))
        )

        # Wykrywanie kolizji z StatefulDoFn
        collisions = (
            keyed
            | 'DetectCollisions' >> beam.ParDo(CollisionDoFn())
        )

        # Publikowanie do Pub/Sub collisions_topic
        collisions_str = (
            collisions
            | 'CollisionsToJson' >> beam.Map(lambda x: json.dumps(x).encode('utf-8'))
        )
        (
            collisions_str
            | 'PubCollisions' >> beam.io.WriteToPubSub(topic=collisions_topic)
        )

if __name__ == '__main__':
    run()