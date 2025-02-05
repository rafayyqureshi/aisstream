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

    while True:
        try:
            async with websockets.connect(URI, ping_interval=None, ssl=ssl_context) as websocket:
                logger.info("Połączono z AISSTREAM.io.")

                subscribe_message = {
                    "APIKey": AISSTREAM_TOKEN,
                    "MessageType": "Subscribe",
                    "BoundingBoxes": BOUNDING_BOXES,
                    "OutputFormat": "JSON",
                    "FilterMessageTypes": ["PositionReport", "ShipStaticData", "Error"]
                }
                await websocket.send(json.dumps(subscribe_message))

                async for raw_msg in websocket:
                    try:
                        msg = json.loads(raw_msg)
                        msg_type = msg.get("MessageType")

                        if msg_type == "PositionReport":
                            await process_position_report(msg)
                        elif msg_type == "Error":
                            logger.error(f"Błąd: {msg.get('Error', 'Nieznany błąd')}")
                            break

                    except json.JSONDecodeError:
                        continue

        except Exception as e:
            logger.error(f"Błąd połączenia: {str(e)}")
            await asyncio.sleep(5)

async def process_position_report(message: dict):
    metadata = message.get("MetaData", {})
    report_ts_str = metadata.get("TimeReceived")

    if not report_ts_str:
        return

    try:
        report_ts_str = report_ts_str.replace('+00:00', 'Z').replace('Z', '+00:00')
        dt_report = datetime.fromisoformat(report_ts_str)
        
        if dt_report.tzinfo is None:
            dt_report = dt_report.replace(tzinfo=timezone.utc)
        else:
            dt_report = dt_report.astimezone(timezone.utc)

        now_utc = datetime.now(timezone.utc)
        delay_sec = (now_utc - dt_report).total_seconds()
        logger.info(f"Opóźnienie: {delay_sec:.1f}s")

    except Exception as e:
        logger.warning(f"Błąd parsowania czasu: {str(e)}")

if __name__ == "__main__":
    try:
        asyncio.run(connect_ais_stream())
    except KeyboardInterrupt:
        logger.info("Przerwano przez użytkownika.")