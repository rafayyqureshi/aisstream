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

################################################################################
# GŁÓWNE ZMIENNE KONFIGURACYJNE
################################################################################
URI = "wss://stream.aisstream.io/v0/stream"
BOUNDING_BOXES = [
    [[49.0, -2.0], [51.0, 2.0]]
]

################################################################################
# GŁÓWNA FUNKCJA ŁĄCZĄCA SIĘ Z AISSTREAM.IO
################################################################################
async def connect_ais_stream():
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    while True:
        try:
            async with websockets.connect(
                URI, ping_interval=None, ssl=ssl_context
            ) as websocket:
                logger.info("Połączono z AISSTREAM.io.")

                # Subskrybujemy określone typy wiadomości
                subscribe_message = {
                    "APIKey": AISSTREAM_TOKEN,
                    "MessageType": "Subscribe",
                    "BoundingBoxes": BOUNDING_BOXES,
                    "OutputFormat": "JSON",
                    "Compression": "None",
                    "BufferSize": 1,
                    "FilterMessageTypes": ["PositionReport"]
                }
                await websocket.send(json.dumps(subscribe_message))
                logger.info("Wysłano subskrypcję do AISSTREAM.")

                # Odbieramy wiadomości w pętli
                async for raw_msg in websocket:
                    try:
                        msg = json.loads(raw_msg)
                    except json.JSONDecodeError:
                        continue

                    # Sprawdzamy typ wiadomości
                    if msg.get("MessageType") == "PositionReport":
                        await process_position_report(msg)

                    elif msg.get("MessageType") == "Error":
                        err = msg.get('Error', 'Nieznany błąd')
                        logger.error(f"Błąd z AISSTREAM: {err}")
                        break

                    # inne typy pomijamy
        except Exception as e:
            logger.error(f"WebSocket connection error: {e}")

        # Po rozłączeniu - spróbuj ponownie po 5 sekundach
        await asyncio.sleep(5)

################################################################################
# OBRÓBKA PositionReport
################################################################################
async def process_position_report(message: dict):
    """
    Dla wiadomości typu PositionReport – odczytujemy pole "Timestamp"
    (zakładamy, że jest w formacie ISO8601, np. "2025-02-04T12:34:56Z").
    Liczymy różnicę w sekundach między tym czasem a teraz (UTC).
    Wypisujemy wyłącznie tę wartość opóźnienia.
    """
    position_report = message.get("Message", {}).get("PositionReport", {})
    if not position_report:
        return

    # Zakładamy, że w polu "Timestamp" jest data/czas
    report_ts_str = position_report.get("Timestamp")
    if not report_ts_str:
        # jeśli brak, pomijamy
        return

    # Parsujemy do datetime
    try:
        # Zastępujemy 'Z' → '+00:00', jeśli występuje
        report_ts_str = report_ts_str.replace('Z', '+00:00')
        dt_report = datetime.fromisoformat(report_ts_str)
    except Exception:
        return

    now_utc = datetime.now(timezone.utc)
    delay_sec = (now_utc - dt_report).total_seconds()

    # Wypisujemy tylko opóźnienie w sekundach
    logger.info(f"Opóźnienie PositionReport: {delay_sec:.1f} sek")

################################################################################
# URUCHOMIENIE
################################################################################
if __name__ == "__main__":
    try:
        asyncio.run(connect_ais_stream())
    except KeyboardInterrupt:
        logger.info("Program przerwany przez użytkownika.")