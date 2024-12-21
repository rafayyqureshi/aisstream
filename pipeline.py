import os
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterWatermark, AccumulationMode
from apache_beam import window
import math
import json
from datetime import datetime

CPA_THRESHOLD = 0.5
TCPA_THRESHOLD = 10.0

def parse_message(record):
    """Prosta funkcja parse, bez timestamp. 
       Zakładamy, że Beam przypisze event-time z PubSub (publishTime) 
       jeśli w ReadFromPubSub używasz 'timestamp_attribute'."""
    try:
        data = json.loads(record.decode('utf-8'))
        # minimalna weryfikacja:
        if not all(k in data for k in ['mmsi','latitude','longitude','cog','sog','timestamp']):
            return None
        return data
    except:
        return None

def compute_cpa_tcpa(ship_a, ship_b):
    if (ship_a.get('ship_length') is None or ship_b.get('ship_length') is None
       or ship_a['ship_length'] < 50 or ship_b['ship_length'] < 50):
        return (9999, -1)

    latRef = (ship_a['latitude'] + ship_b['latitude'])/2
    scaleLat = 111000
    scaleLon = 111000*math.cos(latRef*math.pi/180)

    def toXY(lat,lon):
        return [lon*scaleLon, lat*scaleLat]

    xA,yA = toXY(ship_a['latitude'], ship_a['longitude'])
    xB,yB = toXY(ship_b['latitude'], ship_b['longitude'])

    sogA = ship_a['sog'] or 0
    sogB = ship_b['sog'] or 0

    def cogToVector(cogDeg, sogNmH):
        cog_rad = math.radians(cogDeg)
        vx = sogNmH*math.sin(cog_rad)
        vy = sogNmH*math.cos(cog_rad)
        return vx, vy

    vxA,vyA = cogToVector(ship_a['cog'], sogA)
    vxB,vyB = cogToVector(ship_b['cog'], sogB)

    dx = xA - xB
    dy = yA - yB
    dvx = vxA - vxB
    dvy = vyA - vyB

    speedScale = 1852/60
    dvx_mpm = dvx*speedScale
    dvy_mpm = dvy*speedScale

    VV_m = dvx_mpm**2 + dvy_mpm**2
    PV_m = dx*dvx_mpm + dy*dvy_mpm

    if VV_m==0:
        tcpa=0.0
    else:
        tcpa=-PV_m/VV_m
    if tcpa<0:
        return (9999, -1)

    vxA_mpm = vxA*speedScale
    vyA_mpm = vyA*speedScale
    vxB_mpm = vxB*speedScale
    vyB_mpm = vyB*speedScale

    xA2 = xA + vxA_mpm*tcpa
    yA2 = yA + vyA_mpm*tcpa
    xB2 = xB + vxB_mpm*tcpa
    yB2 = yB + vyB_mpm*tcpa

    dist=math.sqrt((xA2-xB2)**2+(yA2-yB2)**2)
    distNm=dist/1852
    return distNm, tcpa

def find_collisions(kv):
    """kv = (geohash, [records])"""
    gh, records = kv
    recs = list(records)
    filtered = [r for r in recs if r.get('ship_length') and r['ship_length']>=50]
    out=[]
    for i in range(len(filtered)):
        for j in range(i+1,len(filtered)):
            a=filtered[i]
            b=filtered[j]
            cpa,tcpa=compute_cpa_tcpa(a,b)
            if tcpa>=0 and cpa<CPA_THRESHOLD and tcpa<TCPA_THRESHOLD:
                out.append({
                    'mmsi_a': a['mmsi'],
                    'mmsi_b': b['mmsi'],
                    'timestamp': a['timestamp'],
                    'cpa': cpa,
                    'tcpa': tcpa,
                    'latitude_a': a['latitude'],
                    'longitude_a': a['longitude'],
                    'latitude_b': b['latitude'],
                    'longitude_b': b['longitude']
                })
    return out

from apache_beam.options.pipeline_options import PipelineOptions

class MyPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_subscription', type=str, required=True)

def format_ships_csv(r):
    def s(x): return '' if x is None else str(x)
    return (f"{s(r.get('mmsi'))},{s(r.get('latitude'))},"
            f"{s(r.get('longitude'))},{s(r.get('cog'))},"
            f"{s(r.get('sog'))},{s(r.get('timestamp'))},"
            f"{s(r.get('ship_name'))},{s(r.get('ship_length'))}")

def format_collisions_csv(r):
    def s(x): return '' if x is None else str(x)
    return (f"{s(r.get('mmsi_a'))},{s(r.get('mmsi_b'))},"
            f"{s(r.get('timestamp'))},{s(r.get('cpa'))},"
            f"{s(r.get('tcpa'))},{s(r.get('latitude_a'))},"
            f"{s(r.get('longitude_a'))},{s(r.get('latitude_b'))},"
            f"{s(r.get('longitude_b'))}")

def run():
    import sys
    project_id=os.getenv('GOOGLE_CLOUD_PROJECT')
    if not project_id:
        raise ValueError("GOOGLE_CLOUD_PROJECT not set")

    pipeline_options=PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming=True

    # Dodaj parametry do PubSub, w tym 'timestamp_attribute' jeżeli Pub/Sub je obsługuje
    input_subscription=pipeline_options.view_as(MyPipelineOptions).input_subscription

    with beam.Pipeline(options=pipeline_options) as p:

        # Odczyt z PubSub, w trybie streaming
        # UWAGA: jeśli Twój sub nie zawiera publishTime w atrybucie,
        # usuń timestamp_attribute i polegaj na parse_message_with_timestamp
        lines = (
            p
            | 'ReadPubSub' >> beam.io.ReadFromPubSub(
                  subscription=input_subscription,
                  with_attributes=False,
                  timestamp_attribute='publishTime'  # lub usuń, jeśli Twój strumień nie ma publishTime
              )
        )

        # Parsowanie
        parsed = (
            lines
            | 'Parse' >> beam.Map(parse_message)  # dict lub None
            | 'FilterNotNone' >> beam.Filter(lambda r: r is not None)
        )

        # Gałąź 1: Ships (FixedWindow 1 min)
        ships_windowed = (
            parsed
            | 'WindowShips' >> beam.WindowInto(
                FixedWindows(60),
                trigger=AfterWatermark(),
                accumulation_mode=AccumulationMode.DISCARDING,
                allowed_lateness=0
            )
        )
        (
            ships_windowed
            | 'ShipsToCSV' >> beam.Map(format_ships_csv)
            | 'WriteShips' >> beam.io.WriteToText(
                file_path_prefix='gs://ais-collision-detection-bucket/ais_data/ships/ships',
                file_name_suffix='.csv',
                shard_name_template='-SSSS-of-NNNN'
            )
        )

        # Gałąź 2: Collisions (FixedWindow 1 min -> groupByKey(geohash))
        collisions_windowed = (
            parsed
            | 'WindowCollisions' >> beam.WindowInto(
                FixedWindows(60),
                trigger=AfterWatermark(),
                accumulation_mode=AccumulationMode.DISCARDING,
                allowed_lateness=0
            )
            | 'KeyByGeohash' >> beam.Map(lambda r: (r.get('geohash','none'), r))
            | 'GBK' >> beam.GroupByKey()
            | 'FindCols' >> beam.FlatMap(find_collisions)
        )

        (
            collisions_windowed
            | 'ColsToCSV' >> beam.Map(format_collisions_csv)
            | 'WriteCols' >> beam.io.WriteToText(
                file_path_prefix='gs://ais-collision-detection-bucket/ais_data/collisions/collisions',
                file_name_suffix='.csv',
                shard_name_template='-SSSS-of-NNNN'
            )
        )

if __name__=='__main__':
    run()