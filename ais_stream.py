#!/usr/bin/env python3
import os
import asyncio
import json
from datetime import datetime, timezone
from google.cloud import pubsub_v1
from dotenv import load_dotenv
import websockets
import ssl
import certifi

load_dotenv()

AISSTREAM_TOKEN = os.getenv('AISSTREAM_TOKEN')
PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
TOPIC_ID = os.getenv('TOPIC_ID', 'ais-data-topic')

if not AISSTREAM_TOKEN:
    raise ValueError("AISSTREAM_TOKEN is not set.")
if not PROJECT_ID:
    raise ValueError("GOOGLE_CLOUD_PROJECT is not set.")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# Słownik przechowujący dane statyczne statków (mmsi -> dict)
ship_static_data = {}

def is_valid_coordinate(value):
    return isinstance(value, (int, float)) and -180 <= value <= 180

def process_ship_static_data(message: dict):
    metadata = message.get("MetaData", {})
    mmsi = metadata.get("MMSI")
    if not mmsi:
        return

    static_part = message.get("Message", {}).get("ShipStaticData", {})
    ship_name = static_part.get("ShipName") or static_part.get("Name") or "Unknown"
    dimension = static_part.get("Dimension", {})

    def to_float(val):
        return float(val) if isinstance(val, (int, float)) else None

    ship_static_data[mmsi] = {
        "ship_name": ship_name,
        "dim_a": to_float(dimension.get("A")),
        "dim_b": to_float(dimension.get("B")),
        "dim_c": to_float(dimension.get("C")),
        "dim_d": to_float(dimension.get("D"))
    }

async def publish_to_pubsub(msg: dict):
    loop = asyncio.get_running_loop()
    data = json.dumps(msg).encode("utf-8")
    future = publisher.publish(topic_path, data)
    await loop.run_in_executor(None, future.result)

async def process_position_report(message: dict):
    position_report = message.get("Message", {}).get("PositionReport", {})
    metadata = message.get("MetaData", {})
    mmsi = metadata.get("MMSI")
    if not position_report or not mmsi:
        return

    latitude = position_report.get("Latitude")
    longitude = position_report.get("Longitude")
    cog = position_report.get("Cog")
    sog = position_report.get("Sog")
    heading = position_report.get("TrueHeading")
    if heading is not None:
        try:
            heading = float(heading)
        except Exception:
            heading = None

    # Jeśli heading wynosi 511, zastąp go wartością cog
    if heading == 511.0:
        heading = cog

    # Pobieramy timestamp z pola "TimeReceived" w MetaData;
    # jeśli go nie ma, używamy bieżącego czasu w formacie ISO8601 (z 'Z' na końcu)
    time_received_str = metadata.get("TimeReceived")
    if not time_received_str:
        time_received_str = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    else:
        if time_received_str.endswith("+00:00"):
            time_received_str = time_received_str.replace("+00:00", "Z")
    try:
        dt_received = datetime.fromisoformat(time_received_str.replace("Z", "+00:00"))
    except Exception:
        dt_received = datetime.now(timezone.utc)

    # Filtr: publikujemy tylko, jeśli SOG >= 2 i współrzędne są poprawne
    if sog is None or sog < 2:
        return
    if not is_valid_coordinate(latitude) or not is_valid_coordinate(longitude):
        return

    # Pobieramy dane statyczne (jeśli dostępne)
    statics = ship_static_data.get(mmsi, {})
    ship_name = statics.get("ship_name", "Unknown")

    reduced_message = {
        "mmsi": mmsi,
        "ship_name": ship_name,
        "latitude": latitude,
        "longitude": longitude,
        "sog": sog,
        "cog": cog,
        "heading": heading,
        "timestamp": time_received_str,
        "dim_a": statics.get("dim_a"),
        "dim_b": statics.get("dim_b"),
        "dim_c": statics.get("dim_c"),
        "dim_d": statics.get("dim_d")
    }

    await publish_to_pubsub(reduced_message)

async def connect_ais_stream():
    uri = "wss://stream.aisstream.io/v0/stream"
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    while True:
        try:
            async with websockets.connect(uri, ping_interval=None, ssl=ssl_context) as websocket:
                subscribe_message = {
                    "APIKey": AISSTREAM_TOKEN,
                    "MessageType": "Subscribe",
                    "BoundingBoxes": [[[49.0, -2.0], [51.0, 2.0]]],
                    "OutputFormat": "JSON",
                    "Compression": "None",
                    "BufferSize": 1,
                    "FilterMessageTypes": ["PositionReport", "ShipStaticData"]
                }
                await websocket.send(json.dumps(subscribe_message))
                async for message_json in websocket:
                    try:
                        message = json.loads(message_json)
                    except json.JSONDecodeError:
                        continue
                    msg_type = message.get("MessageType")
                    if msg_type == "Error":
                        break
                    elif msg_type == "ShipStaticData":
                        process_ship_static_data(message)
                    elif msg_type == "PositionReport":
                        await process_position_report(message)
        except Exception:
            pass
        await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(connect_ais_stream())
    except KeyboardInterrupt:
        pass