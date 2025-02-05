#!/usr/bin/env python3
import os
import math
import json
import logging
import datetime
from datetime import timezone, timedelta
from dotenv import load_dotenv
import asyncio
import websockets
import ssl
import certifi
import time

from google.cloud import pubsub_v1

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

# Global dictionaries and counters
ship_static_data = {}
hdg511_corrected_count = 0  # Counts messages where TrueHeading was 511 and corrected
TIMESTAMP_DIFFS = []  # List of timestamp differences (in minutes) for each message

# Logging summary: once per minute, log corrected hdg and timestamp differences
async def log_summary_stats():
    global hdg511_corrected_count, TIMESTAMP_DIFFS
    while True:
        await asyncio.sleep(60)
        if TIMESTAMP_DIFFS:
            avg_diff = sum(TIMESTAMP_DIFFS) / len(TIMESTAMP_DIFFS)
            count_over_4 = len([d for d in TIMESTAMP_DIFFS if d > 4])
        else:
            avg_diff = 0
            count_over_4 = 0
        logger.info(
            f"[SummaryStats] Wykryto {hdg511_corrected_count} statków z TrueHeading=511 (poprawiono na cog). "
            f"Średnia różnica timestamp: {avg_diff:.2f} minut, "
            f"{count_over_4} rekordów >4 min opóźnienia."
        )
        # Reset counters
        hdg511_corrected_count = 0
        TIMESTAMP_DIFFS.clear()

async def log_stats():
    """
    Logs the count of received and published AIS messages every minute.
    """
    global received_count, published_count
    while True:
        await asyncio.sleep(60)
        logger.info(
            f"Ostatnia minuta: odebrano {received_count} AIS, opublikowano {published_count} do Pub/Sub"
        )
        received_count = 0
        published_count = 0

async def connect_ais_stream():
    """
    Main loop: connects to AISSTREAM.io, subscribes, and processes incoming messages.
    """
    global received_count, published_count
    uri = "wss://stream.aisstream.io/v0/stream"
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    # Start background tasks for logging summary and general stats
    asyncio.create_task(log_stats())
    asyncio.create_task(log_summary_stats())

    while True:
        try:
            async with websockets.connect(uri, ping_interval=None, ssl=ssl_context) as websocket:
                logger.info("=== AIS STREAM === Connected to AISSTREAM.io")

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
        await asyncio.sleep(5)

def process_ship_static_data(message: dict):
    """
    Processes ShipStaticData messages.
    """
    metadata = message.get("MetaData", {})
    mmsi = metadata.get('MMSI')
    if not mmsi:
        logger.warning("MMSI not found in ShipStaticData message")
        return

    static_part = message.get("Message", {}).get("ShipStaticData", {})
    ship_name = static_part.get('ShipName') or static_part.get('Name') or 'Unknown'

    dimension = static_part.get('Dimension', {})
    a = dimension.get('A')
    b = dimension.get('B')
    c = dimension.get('C')
    d = dimension.get('D')

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
    return isinstance(value, (int, float)) and -180 <= value <= 180

async def process_position_report(message: dict):
    """
    Processes PositionReport messages:
      - Enriches with static data,
      - Checks if TrueHeading is 511; if so, replaces it with Cog and logs the correction,
      - Checks timestamp differences and appends them to a global list,
      - Publishes the reduced message to Pub/Sub.
    """
    global published_count, hdg511_corrected_count
    position_report = message.get("Message", {}).get("PositionReport", {})
    metadata = message.get("MetaData", {})
    mmsi = metadata.get('MMSI')

    if not position_report or not mmsi:
        return

    latitude = position_report.get('Latitude')
    longitude = position_report.get('Longitude')
    cog = position_report.get('Cog')
    sog = position_report.get('Sog')

    # Retrieve and process TrueHeading
    heading = position_report.get('TrueHeading')
    if heading is not None:
        try:
            heading = float(heading)
        except:
            heading = None

    # If heading equals 511, log and replace with cog
    if heading == 511:
        logger.info(f"MMSI {mmsi}: Detected TrueHeading=511; replacing with Cog value {cog}.")
        heading = cog
        hdg511_corrected_count += 1

    # Use TimeReceived from MetaData
    time_received = metadata.get("TimeReceived")
    if not time_received:
        time_received = datetime.datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    else:
        if time_received.endswith('+00:00'):
            time_received = time_received.replace('+00:00', 'Z')

    # Validate sog and coordinates
    if sog is None or sog < 2:
        return
    if not is_valid_coordinate(latitude) or not is_valid_coordinate(longitude):
        return

    # Check timestamp difference
    try:
        received_time = datetime.datetime.fromisoformat(time_received.replace("Z", "+00:00"))
        now_time = datetime.datetime.now(timezone.utc)
        diff_minutes = (now_time - received_time).total_seconds() / 60
        TIMESTAMP_DIFFS.append(diff_minutes)
        if diff_minutes > 3:
            logger.warning(f"MMSI {mmsi}: TimeReceived is delayed by {diff_minutes:.2f} minutes (> 3 min).")
    except Exception as e:
        logger.error(f"Error parsing TimeReceived {time_received} for MMSI {mmsi}: {e}")

    # Enrich with static data
    statics = ship_static_data.get(mmsi, {})
    ship_name = statics.get('ship_name', 'Unknown')
    dim_a = statics.get('dim_a')
    dim_b = statics.get('dim_b')
    dim_c = statics.get('dim_c')
    dim_d = statics.get('dim_d')

    reduced_message = {
        'mmsi': mmsi,
        'ship_name': ship_name,
        'latitude': latitude,
        'longitude': longitude,
        'sog': sog,
        'cog': cog,
        'heading': heading,
        'timestamp': time_received,
        'dim_a': dim_a,
        'dim_b': dim_b,
        'dim_c': dim_c,
        'dim_d': dim_d
    }

    await publish_to_pubsub(reduced_message)
    published_count += 1

async def publish_to_pubsub(msg: dict):
    """
    Publishes the message to Pub/Sub.
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