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

ship_static_data = {}
received_count = 0
published_count = 0

async def log_stats():
    global received_count, published_count
    while True:
        await asyncio.sleep(60)
        logger.info(f"W ciągu ostatniej minuty odebrano {received_count} wiadomości AIS, opublikowano {published_count} wiadomości do Pub/Sub")
        received_count = 0
        published_count = 0

async def connect_ais_stream():
    global received_count, published_count
    uri = "wss://stream.aisstream.io/v0/stream"
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    asyncio.create_task(log_stats())

    while True:
        try:
            async with websockets.connect(uri, ping_interval=None, ssl=ssl_context) as websocket:
                logger.info("Connected to AISSTREAM.io")

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
                        logger.warning(f"Invalid JSON message: {message_json}")
                        continue

                    message_type = message.get("MessageType")
                    if message_type == "Error":
                        logger.error(f"Error from server: {message.get('Error')}")
                        break
                    elif message_type == "ShipStaticData":
                        process_ship_static_data(message)
                    elif message_type == "PositionReport":
                        await process_position_report(message)
                    else:
                        logger.warning(f"Unhandled MessageType: {message_type}")
        except Exception as e:
            logger.error(f"WebSocket connection error: {e}")
        await asyncio.sleep(5)

def process_ship_static_data(message):
    metadata = message.get("MetaData", {})
    mmsi = metadata.get('MMSI')
    ship_static_data_message = message.get("Message", {}).get("ShipStaticData", {})

    if not mmsi:
        logger.warning("MMSI not found in ShipStaticData message")
        return

    ship_name = ship_static_data_message.get('ShipName') or ship_static_data_message.get('Name') or 'Unknown'
    dimension = ship_static_data_message.get('Dimension') or {}
    a = dimension.get('A')
    b = dimension.get('B')
    ship_length = 'Unknown'
    if a is not None and b is not None:
        try:
            ship_length = float(a) + float(b)
        except (ValueError, TypeError):
            pass

    ship_static_data[mmsi] = {
        'ship_name': ship_name,
        'ship_length': ship_length
    }

def is_valid_coordinate(value):
    return isinstance(value, (int, float)) and -180 <= value <= 180

async def process_position_report(message):
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

    timestamp = metadata.get('TimeReceived')
    if not timestamp:
        timestamp = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    else:
        if timestamp.endswith('+00:00'):
            timestamp = timestamp.replace('+00:00', 'Z')

    if sog is None or sog < 2:
        return
    if not is_valid_coordinate(latitude) or not is_valid_coordinate(longitude):
        return

    ship_data = ship_static_data.get(mmsi, {
        'ship_name': 'Unknown',
        'ship_length': 'Unknown'
    })
    ship_name = ship_data['ship_name']
    ship_length = ship_data['ship_length']

    reduced_message = {
        'mmsi': mmsi,
        'ship_name': ship_name,
        'latitude': latitude,
        'longitude': longitude,
        'sog': sog,
        'cog': cog,
        'ship_length': ship_length,
        'timestamp': timestamp
    }

    await publish_to_pubsub(reduced_message)
    published_count += 1

async def publish_to_pubsub(message):
    loop = asyncio.get_running_loop()
    data = json.dumps(message).encode('utf-8')
    future = publisher.publish(topic_path, data)
    await loop.run_in_executor(None, future.result)

if __name__ == "__main__":
    try:
        asyncio.run(connect_ais_stream())
    except KeyboardInterrupt:
        logger.info("Program terminated by user")