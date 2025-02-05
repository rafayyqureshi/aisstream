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
    Funkcja odczytuje wiadomość typu PositionReport, korzystając z pełnego
    znacznika czasu (pole "time_utc" z MetaData) i oblicza opóźnienie.
    """
    meta = message.get("MetaData", {})
    time_utc_str = meta.get("time_utc")
    if not time_utc_str:
        logger.error("Brak pola time_utc w MetaData.")
        return

    try:
        # Usuń ewentualne końcowe " UTC", aby format pasował do oczekiwanego schematu.
        time_utc_str = time_utc_str.replace(" UTC", "")
        # Próba parsowania ze wzorcem z mikrosekundami, a w razie niepowodzenia bez mikrosekund.
        try:
            dt_report = datetime.strptime(time_utc_str, "%Y-%m-%d %H:%M:%S.%f %z")
        except ValueError:
            dt_report = datetime.strptime(time_utc_str, "%Y-%m-%d %H:%M:%S %z")
    except Exception as e:
        logger.error(f"Błąd parsowania time_utc: {e}")
        return

    now_utc = datetime.now(timezone.utc)
    delay_sec = (now_utc - dt_report).total_seconds()
    logger.info(f"Opóźnienie PositionReport: {delay_sec:.1f} s")

if __name__ == "__main__":
    try:
        asyncio.run(connect_ais_stream())
    except KeyboardInterrupt:
        logger.info("Przerwano przez użytkownika.")