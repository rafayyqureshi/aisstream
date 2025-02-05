#!/usr/bin/env python3
import os
import asyncio
import json
import logging
import ssl
import certifi
from datetime import datetime, timezone
from dotenv import load_dotenv
import websockets

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

AISSTREAM_TOKEN = os.getenv('AISSTREAM_TOKEN')
if not AISSTREAM_TOKEN:
    raise ValueError("AISSTREAM_TOKEN is not set.")

URI = "wss://stream.aisstream.io/v0/stream"
BOUNDING_BOXES = [[[49.0, -2.0], [51.0, 2.0]]]

async def connect_ais_stream():
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    async with websockets.connect(URI, ssl=ssl_context, ping_interval=None) as websocket:
        logger.info("Połączono z AISSTREAM.io.")

        subscribe_message = {
            "APIKey": AISSTREAM_TOKEN,
            "MessageType": "Subscribe",
            "BoundingBoxes": BOUNDING_BOXES,
            "OutputFormat": "JSON",
            "FilterMessageTypes": ["PositionReport"]
        }
        await websocket.send(json.dumps(subscribe_message))
        logger.info("Wysłano subskrypcję do AISSTREAM.")

        async for raw_msg in websocket:
            try:
                msg = json.loads(raw_msg)
                if msg.get("MessageType") == "PositionReport":
                    await process_position_report(msg)
            except json.JSONDecodeError:
                continue

async def process_position_report(message: dict):
    """
    Funkcja odczytuje wiadomość typu PositionReport,
    pobiera znacznik czasu (pole "Timestamp") i oblicza opóźnienie.
    """
    # Pobieramy dane raportu z wiadomości
    position_report = message.get("Message", {}).get("PositionReport", {})
    if not position_report:
        return

    # Pobieramy znacznik czasu z pola "Timestamp"
    timestamp_str = position_report.get("Timestamp")
    if not timestamp_str:
        return

    try:
        # Zamieniamy 'Z' na '+00:00', jeśli występuje, aby format był zgodny z ISO 8601
        timestamp_str = timestamp_str.replace("Z", "+00:00")
        dt_report = datetime.fromisoformat(timestamp_str)
    except Exception as e:
        logger.error(f"Błąd parsowania znacznika czasu: {e}")
        return

    now_utc = datetime.now(timezone.utc)
    delay_sec = (now_utc - dt_report).total_seconds()
    logger.info(f"Opóźnienie PositionReport: {delay_sec:.1f} s")

if __name__ == "__main__":
    try:
        asyncio.run(connect_ais_stream())
    except KeyboardInterrupt:
        logger.info("Przerwano przez użytkownika.")