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

# Globalne liczniki dla statystyk
invalid_hdg_count = 0          # Liczba wykrytych hdg 511
corrected_hdg_count = 0        # Liczba poprawionych hdg przez COG
total_delay = 0.0              # Suma opóźnień w sekundach
message_count = 0              # Liczba przetworzonych wiadomości
delayed_messages = 0           # Wiadomości z opóźnieniem >4 minut

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

async def log_stats():
    """Logowanie statystyk co minutę"""
    global invalid_hdg_count, corrected_hdg_count, total_delay, message_count, delayed_messages
    
    while True:
        await asyncio.sleep(60)
        
        # Oblicz średnie opóźnienie
        avg_delay = total_delay / message_count if message_count > 0 else 0
        
        logger.info(
            "[MINUTE STATS] "
            f"Invalid HDG 511: {invalid_hdg_count}, "
            f"Corrected by COG: {corrected_hdg_count} | "
            f"Avg delay: {avg_delay:.1f}s | "
            f"Delayed >4min: {delayed_messages}"
        )
        
        # Reset liczników
        invalid_hdg_count = 0
        corrected_hdg_count = 0
        total_delay = 0.0
        message_count = 0
        delayed_messages = 0

def calculate_time_delay(metadata: dict) -> float:
    """Oblicza opóźnienie między TimeReceived a bieżącym czasem"""
    try:
        timestamp_str = metadata.get('TimeReceived')
        if not timestamp_str:
            return 0.0
            
        # Standaryzuj format czasu
        if timestamp_str.endswith('+00:00'):
            timestamp_str = timestamp_str.replace('+00:00', 'Z')
            
        msg_time = datetime.fromisoformat(timestamp_str).replace(tzinfo=timezone.utc)
        now_time = datetime.now(timezone.utc)
        delay = (now_time - msg_time).total_seconds()
        
        return max(delay, 0.0)
        
    except Exception as e:
        logger.error(f"Error calculating time delay: {e}")
        return 0.0

async def process_position_report(message: dict):
    global invalid_hdg_count, corrected_hdg_count, total_delay, message_count, delayed_messages
    
    position_report = message.get("Message", {}).get("PositionReport", {})
    metadata = message.get("MetaData", {})
    
    # Pomiar opóźnienia
    delay = calculate_time_delay(metadata)
    total_delay += delay
    message_count += 1
    
    if delay > 240:  # 4 minuty
        delayed_messages += 1

    # Obsługa heading 511
    heading = position_report.get('TrueHeading')
    if heading == 511:  # Specjalna wartość AIS dla braku danych
        invalid_hdg_count += 1
        cog = position_report.get('Cog')
        if cog is not None and 0 <= cog <= 360:
            heading = cog
            corrected_hdg_count += 1
        else:
            heading = None

    # Reszta przetwarzania pozostaje bez zmian...
    # ... (tu oryginalna logika przetwarzania)

async def connect_ais_stream():
    global invalid_hdg_count, corrected_hdg_count
    
    uri = "wss://stream.aisstream.io/v0/stream"
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    asyncio.create_task(log_stats())

    while True:
        try:
            async with websockets.connect(uri, ping_interval=None, ssl=ssl_context) as websocket:
                subscribe_message = {
                    "APIKey": AISSTREAM_TOKEN,
                    "MessageType": "Subscribe",
                    "BoundingBoxes": [[[49.0, -2.0], [51.0, 2.0]]],
                    "OutputFormat": "JSON",
                    "FilterMessageTypes": ["PositionReport", "ShipStaticData"]
                }

                await websocket.send(json.dumps(subscribe_message))

                async for message_json in websocket:
                    try:
                        message = json.loads(message_json)
                    except json.JSONDecodeError:
                        continue

                    msg_type = message.get("MessageType")
                    if msg_type == "PositionReport":
                        await process_position_report(message)
                    elif msg_type == "ShipStaticData":
                        process_ship_static_data(message)

        except Exception as e:
            logger.error(f"Connection error: {str(e)}")
            await asyncio.sleep(5)

# Reszta funkcji pozostaje bez zmian (process_ship_static_data, publish_to_pubsub itd.)

if __name__ == "__main__":
    try:
        asyncio.run(connect_ais_stream())
    except KeyboardInterrupt:
        logger.info("Service stopped")