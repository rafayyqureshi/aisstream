#!/usr/bin/env python3
import os
import json
import math
import time
import logging
import datetime
from dotenv import load_dotenv

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from google.cloud import bigquery
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.transforms.userstate import BagStateSpec
import apache_beam.coders

from apache_beam import window

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Parametry kolizji
CPA_THRESHOLD       = 0.5
TCPA_THRESHOLD      = 10.0
STATE_RETENTION_SEC = 120

def compute_cpa_tcpa(shipA, shipB):
    required = ['latitude','longitude','cog','sog']
    for s in [shipA, shipB]:
        if not all(r in s and s[r] is not None for r in required):
            return (9999, -1)
    cogA = math.radians(shipA['cog'])
    cogB = math.radians(shipB['cog'])
    sogA = shipA['sog']/60.0
    sogB = shipB['sog']/60.0
    vxA, vyA = sogA*math.sin(cogA), sogA*math.cos(cogA)
    vxB, vyB = sogB*math.sin(cogB), sogB*math.cos(cogB)
    dx = shipA['longitude']-shipB['longitude']
    dy = shipA['latitude'] -shipB['latitude']
    dvx,dvy = vxA-vxB, vyA-vyB
    dv2 = dvx*dvx + dvy*dvy
    if dv2==0: return (9999,-1)
    pv = dx*dvx + dy*dvy
    tcpa= -pv/dv2
    if tcpa<0: return (9999,-1)
    cx = dx+dvx*tcpa
    cy = dy+dvy*tcpa
    cpa_deg = math.sqrt(cx*cx + cy*cy)
    cpa_nm  = cpa_deg*60.0
    return (cpa_nm, tcpa)

def parse_ais(record_bytes):
    try:
        data = json.loads(record_bytes.decode("utf-8"))
        if not all(k in data for k in ["mmsi","latitude","longitude","cog","sog","timestamp"]):
            return None
        data["mmsi"]     = int(data["mmsi"])
        data["latitude"] = float(data["latitude"])
        data["longitude"]= float(data["longitude"])
        data["cog"]      = float(data["cog"])
        data["sog"]      = float(data["sog"])
        data.pop("ship_length",None)
        if "heading" in data:
            try: data["heading"]=float(data["heading"])
            except: data["heading"]=None
        else:
            data["heading"]=None
        for d in ["dim_a","dim_b","dim_c","dim_d"]:
            val=data.get(d)
            if val is not None:
                try: data[d]=float(val)
                except: data[d]=None
            else:
                data[d]=None
        data["ship_name"]=data.get("ship_name","Unknown")
        data["geohash"]=data.get("geohash","none")
        return data
    except:
        return None

class CollisionDoFn(beam.DoFn):
    RECORDS_STATE=BagStateSpec("records_state",beam.coders.TupleCoder((
        beam.coders.FastPrimitivesCoder(),
        beam.coders.FastPrimitivesCoder()
    )))
    def process(self, element, records_state=beam.DoFn.StateParam(RECORDS_STATE)):
        gh, ship = element
        now_sec=time.time()
        records_state.add((ship, now_sec))
        old_list=list(records_state.read())
        fresh=[]
        for (s,t) in old_list:
            if (now_sec-t)<=STATE_RETENTION_SEC:
                fresh.append((s,t))
        records_state.clear()
        for (s,t) in fresh:
            records_state.add((s,t))
        for (old_ship,_) in fresh:
            if old_ship["mmsi"]==ship["mmsi"]:
                continue
            cpa, tcpa=compute_cpa_tcpa(old_ship,ship)
            if cpa<CPA_THRESHOLD and 0<=tcpa<TCPA_THRESHOLD:
                yield {
                    "mmsi_a":old_ship["mmsi"],
                    "mmsi_b":ship["mmsi"],
                    "timestamp":ship["timestamp"],
                    "cpa":cpa,
                    "tcpa":tcpa,
                    "latitude_a":old_ship["latitude"],
                    "longitude_a":old_ship["longitude"],
                    "latitude_b":ship["latitude"],
                    "longitude_b":ship["longitude"]
                }

def remove_geohash_and_dims(row):
    new_row=dict(row)
    new_row.pop("geohash",None)
    for d in ["dim_a","dim_b","dim_c","dim_d"]:
        new_row.pop(d,None)
    return new_row

class DeduplicateStaticDoFn(beam.DoFn):
    def __init__(self):
        self.seen_mmsi=set()
    def process(self,row):
        mmsi=row["mmsi"]
        if mmsi not in self.seen_mmsi:
            self.seen_mmsi.add(mmsi)
            yield row

def keep_static_fields(row):
    return {
        "mmsi":row["mmsi"],
        "ship_name":row["ship_name"],
        "dim_a":row["dim_a"],
        "dim_b":row["dim_b"],
        "dim_c":row["dim_c"],
        "dim_d":row["dim_d"],
        "update_time":datetime.datetime.utcnow().isoformat()+"Z"
    }

class CreateBQTableDoFn(beam.DoFn):
    def process(self, table_ref):
        client=bigquery.Client()
        table_id=table_ref['table_id']
        schema=table_ref['schema']
        tp=table_ref.get('time_partitioning')
        cf=table_ref.get('clustering_fields')
        table=bigquery.Table(table_id, schema=schema)
        if tp:
            table.time_partitioning=bigquery.TimePartitioning(**tp)
        if cf:
            table.clustering_fields=cf
        try:
            client.get_table(table_id)
            logging.info(f"Tabela {table_id} juz istnieje.")
        except bigquery.NotFound:
            client.create_table(table)
            logging.info(f"Tabela {table_id} utworzona.")
        except Exception as e:
            logging.error(f"Blad tworzenia tabeli {table_id}: {e}")

def run():
    from apache_beam.options.pipeline_options import PipelineOptions
    from google.cloud import bigquery

    project_id=os.getenv("GOOGLE_CLOUD_PROJECT","ais-collision-detection")
    dataset=os.getenv("LIVE_DATASET","ais_dataset_us")
    input_sub=os.getenv("INPUT_SUBSCRIPTION","projects/ais-collision-detection/subscriptions/ais-data-sub")

    region=os.getenv("REGION","us-east1")
    temp_loc=os.getenv("TEMP_LOCATION","gs://ais-collision-detection-bucket/temp")
    staging_loc=os.getenv("STAGING_LOCATION","gs://ais-collision-detection-bucket/staging")

    table_positions = f"{project_id}:{dataset}.ships_positions"
    table_collisions= f"{project_id}:{dataset}.collisions"
    table_static    = f"{project_id}:{dataset}.ships_static"

    tables_to_create=[
        {
            'table_id': table_positions,
            'schema':{
                "fields":[
                    {"name":"mmsi","type":"INTEGER","mode":"REQUIRED"},
                    {"name":"ship_name","type":"STRING","mode":"NULLABLE"},
                    {"name":"latitude","type":"FLOAT","mode":"REQUIRED"},
                    {"name":"longitude","type":"FLOAT","mode":"REQUIRED"},
                    {"name":"cog","type":"FLOAT","mode":"REQUIRED"},
                    {"name":"sog","type":"FLOAT","mode":"REQUIRED"},
                    {"name":"heading","type":"FLOAT","mode":"NULLABLE"},
                    {"name":"timestamp","type":"TIMESTAMP","mode":"REQUIRED"}
                ]
            },
            'time_partitioning':{"type":"DAY","field":"timestamp","expiration_ms":None},
            'clustering_fields':["mmsi"]
        },
        {
            'table_id': table_collisions,
            'schema':{
                "fields":[
                    {"name":"mmsi_a","type":"INTEGER","mode":"REQUIRED"},
                    {"name":"mmsi_b","type":"INTEGER","mode":"REQUIRED"},
                    {"name":"timestamp","type":"TIMESTAMP","mode":"REQUIRED"},
                    {"name":"cpa","type":"FLOAT","mode":"REQUIRED"},
                    {"name":"tcpa","type":"FLOAT","mode":"REQUIRED"},
                    {"name":"latitude_a","type":"FLOAT","mode":"REQUIRED"},
                    {"name":"longitude_a","type":"FLOAT","mode":"REQUIRED"},
                    {"name":"latitude_b","type":"FLOAT","mode":"REQUIRED"},
                    {"name":"longitude_b","type":"FLOAT","mode":"REQUIRED"}
                ]
            },
            'time_partitioning':{"type":"DAY","field":"timestamp","expiration_ms":None},
            'clustering_fields':["mmsi_a","mmsi_b"]
        },
        {
            'table_id': table_static,
            'schema':{
                "fields":[
                    {"name":"mmsi","type":"INTEGER","mode":"REQUIRED"},
                    {"name":"ship_name","type":"STRING","mode":"NULLABLE"},
                    {"name":"dim_a","type":"FLOAT","mode":"NULLABLE"},
                    {"name":"dim_b","type":"FLOAT","mode":"NULLABLE"},
                    {"name":"dim_c","type":"FLOAT","mode":"NULLABLE"},
                    {"name":"dim_d","type":"FLOAT","mode":"NULLABLE"},
                    {"name":"update_time","type":"TIMESTAMP","mode":"REQUIRED"}
                ]
            },
            'time_partitioning':None,
            'clustering_fields':["mmsi"]
        }
    ]

    pipeline_opts=PipelineOptions(
        runner='DataflowRunner',
        project=project_id,
        region=region,
        temp_location=temp_loc,
        staging_location=staging_loc,
        num_workers=1,
        max_num_workers=10,
        autoscaling_algorithm='THROUGHPUT_BASED'
    )

    with beam.Pipeline(options=pipeline_opts) as p:
        _=(
            tables_to_create
            | "CreateTables" >> beam.ParDo(CreateBQTableDoFn())
        )

        lines= p | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=input_sub)
        parsed=(
            lines
            | "ParseAIS"   >> beam.Map(parse_ais)
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
        )

        # Ships positions w oknach 10s
        windowed_pos=(
            parsed
            | "WinPos" >> beam.WindowInto(window.FixedWindows(10))
        )
        grouped_pos=(
            windowed_pos
            | "KeyPos" >> beam.Map(lambda r:(None,r))
            | "GroupPos" >> beam.GroupByKey()
        )
        flatten_pos=(
            grouped_pos
            | "FlattenPos" >> beam.FlatMap(lambda kv: kv[1])
        )
        final_pos= flatten_pos | "RemoveDims" >> beam.Map(remove_geohash_and_dims)
        final_pos | "WritePositions" >> WriteToBigQuery(
            table=table_positions,
            schema="""
                mmsi:INTEGER,
                ship_name:STRING,
                latitude:FLOAT,
                longitude:FLOAT,
                cog:FLOAT,
                sog:FLOAT,
                heading:FLOAT,
                timestamp:TIMESTAMP
            """,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            method="STREAMING_INSERTS"
        )

        # Ships static – deduplikacja i create if needed
        with_dims= parsed | "FilterDims" >> beam.Filter(
            lambda r:any(r.get(dim) is not None for dim in ["dim_a","dim_b","dim_c","dim_d"])
        )
        deduped_static= with_dims | "DedupStatic" >> beam.ParDo(DeduplicateStaticDoFn())
        prep_static= deduped_static | "KeepStaticFields" >> beam.Map(keep_static_fields)
        prep_static | "WriteStatic" >> WriteToBigQuery(
            table=table_static,
            schema="""
              mmsi:INTEGER,
              ship_name:STRING,
              dim_a:FLOAT,
              dim_b:FLOAT,
              dim_c:FLOAT,
              dim_d:FLOAT,
              update_time:TIMESTAMP
            """,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED, # Kluczowe!
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            method="STREAMING_INSERTS"
        )

        # collisions
        keyed= parsed | "KeyByGeohash" >> beam.Map(lambda r:(r["geohash"],r))
        collisions= keyed | "DetectCollisions" >> beam.ParDo(CollisionDoFn())
        windowed_coll=(
            collisions
            | "WinColl"   >> beam.WindowInto(window.FixedWindows(10))
            | "KeyColl"   >> beam.Map(lambda c:(None,c))
            | "GroupColl" >> beam.GroupByKey()
            | "FlattenColl" >> beam.FlatMap(lambda kv:kv[1])
        )
        windowed_coll | "WriteCollisions" >> WriteToBigQuery(
            table=table_collisions,
            schema="""
              mmsi_a:INTEGER,
              mmsi_b:INTEGER,
              timestamp:TIMESTAMP,
              cpa:FLOAT,
              tcpa:FLOAT,
              latitude_a:FLOAT,
              longitude_a:FLOAT,
              latitude_b:FLOAT,
              longitude_b:FLOAT
            """,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            method="STREAMING_INSERTS"
        )