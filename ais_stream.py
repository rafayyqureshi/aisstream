#!/usr/bin/env python3
import os
import asyncio
import json
from datetime import datetime, timezone
from google.cloud import pubsub_v1
from dotenv import load_dotenv
import websockets
import logging
import ssl
import certifi
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

# Słownik pamiętający dane statyczne statków (mmsi -> dict)
# klucz: mmsi (int), wartość: { 'ship_name':..., 'dim_a':..., 'dim_b':..., 'dim_c':..., 'dim_d':... }
ship_static_data = {}

received_count = 0
published_count = 0

async def log_stats():
    """
    Zadanie asynchroniczne: co minutę logujemy liczbę odebranych
    i opublikowanych wiadomości AIS.
    """
    global received_count, published_count
    while True:
        await asyncio.sleep(60)
        logger.info(
            f"Ostatnia minuta: odebrano {received_count} AIS, "
            f"opublikowano {published_count} do Pub/Sub"
        )
        received_count = 0
        published_count = 0

async def connect_ais_stream():
    """
    Główna pętla łącząca się z AISSTREAM.io i nasłuchująca wiadomości AIS (ShipStaticData, PositionReport).
    """
    global received_count, published_count
    uri = "wss://stream.aisstream.io/v0/stream"
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    asyncio.create_task(log_stats())  # statystyki co 60s

    while True:
        try:
            async with websockets.connect(uri, ping_interval=None, ssl=ssl_context) as websocket:
                logger.info("=== AIS STREAM (NEW CODE) === connected to AISSTREAM.io")

                subscribe_message = {
                    "APIKey": AISSTREAM_TOKEN,
                    "MessageType": "Subscribe",
                    "BoundingBoxes": [
                        [[49.0, -2.0], [51.0, 2.0]]
                    ],
                    "OutputFormat": "JSON",
                    "Compression": "None",
                    "BufferSize": 1,
                    "FilterMessageTypes": ["PositionReport", "ShipStaticData"]
                }

                await websocket.send(json.dumps(subscribe_message))
                logger.info("Subscription message sent")

                async for message_json in websocket:
                    received_count += 1
                    try:
                        message = json.loads(message_json)
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON: {message_json}")
                        continue

                    msg_type = message.get("MessageType")
                    if msg_type == "Error":
                        logger.error(f"Error from AISSTREAM: {message.get('Error')}")
                        break
                    elif msg_type == "ShipStaticData":
                        process_ship_static_data(message)
                    elif msg_type == "PositionReport":
                        await process_position_report(message)
                    else:
                        logger.warning(f"Unhandled MessageType: {msg_type}")

        except Exception as e:
            logger.error(f"WebSocket connection error: {e}")
        # Po rozłączeniu: odczekaj 5s i spróbuj ponownie
        await asyncio.sleep(5)

def process_ship_static_data(message: dict):
    """
    Obrabia wiadomości typu ShipStaticData: 
    - pobiera nazwy i wymiary A,B,C,D
    - zapamiętuje w ship_static_data[mmsi].
    """
    metadata = message.get("MetaData", {})
    mmsi = metadata.get('MMSI')
    if not mmsi:
        logger.warning("MMSI not found in ShipStaticData message")
        return

    static_part = message.get("Message", {}).get("ShipStaticData", {})
    ship_name = static_part.get('ShipName') or static_part.get('Name') or 'Unknown'

    dimension = static_part.get('Dimension', {})
    a = dimension.get('A')  # od anteny do dziobu
    b = dimension.get('B')  # od anteny do rufy
    c = dimension.get('C')  # od anteny do lewej burty
    d = dimension.get('D')  # od anteny do prawej burty

    def to_float(val):
        return float(val) if isinstance(val, (int, float)) else None

    dim_a = to_float(a)
    dim_b = to_float(b)
    dim_c = to_float(c)
    dim_d = to_float(d)

    ship_static_data[mmsi] = {
        'ship_name': ship_name,
        'dim_a': dim_a,
        'dim_b': dim_b,
        'dim_c': dim_c,
        'dim_d': dim_d
    }

def is_valid_coordinate(value):
    """
    Prosta walidacja współrzędnych lat/lon.
    """
    return isinstance(value, (int, float)) and -180 <= value <= 180

async def process_position_report(message: dict):
    """
    Obrabia wiadomości typu PositionReport:
    - dopisuje dane statyczne (dim_a, dim_b, etc.) z ship_static_data
    - pobiera TrueHeading => heading
    - Publikuje do Pub/Sub.
    """
    global published_count
    position_report = message.get("Message", {}).get("PositionReport", {})
    metadata = message.get("MetaData", {})
    mmsi = metadata.get('MMSI')

    if not position_report or not mmsi:
        return

    latitude = position_report.get('Latitude')
    longitude = position_report.get('Longitude')
    cog = position_report.get('Cog')
    sog = position_report.get('Sog')

    # Nowy parametr: TrueHeading
    heading = position_report.get('TrueHeading')  # według dokumentacji
    if heading is not None:
        try:
            heading = float(heading)
        except:
            heading = None

    # Timestamp
    timestamp = metadata.get('TimeReceived')
    if not timestamp:
        timestamp = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    else:
        if timestamp.endswith('+00:00'):
            timestamp = timestamp.replace('+00:00', 'Z')

    # Filtr SOG >= 2 + valid lat/lon
    if sog is None or sog < 2:
        return
    if not is_valid_coordinate(latitude) or not is_valid_coordinate(longitude):
        return

    # Dane statyczne z pamięci
    statics = ship_static_data.get(mmsi, {})
    ship_name = statics.get('ship_name', 'Unknown')
    dim_a = statics.get('dim_a')
    dim_b = statics.get('dim_b')
    dim_c = statics.get('dim_c')
    dim_d = statics.get('dim_d')

    # Wiadomość do Pub/Sub
    reduced_message = {
        'mmsi': mmsi,
        'ship_name': ship_name,
        'latitude': latitude,
        'longitude': longitude,
        'sog': sog,
        'cog': cog,
        'heading': heading,  # <-- nowa informacja
        'timestamp': timestamp,
        'dim_a': dim_a,
        'dim_b': dim_b,
        'dim_c': dim_c,
        'dim_d': dim_d
    }

    await publish_to_pubsub(reduced_message)
    published_count += 1

async def publish_to_pubsub(msg: dict):
    """
    Publikacja asynchroniczna do Pub/Sub.
    """
    loop = asyncio.get_running_loop()
    data = json.dumps(msg).encode('utf-8')
    future = publisher.publish(topic_path, data)
    await loop.run_in_executor(None, future.result)

if __name__ == "__main__":
    try:
        asyncio.run(connect_ais_stream())
    except KeyboardInterrupt:
        logger.info("Program terminated by user")