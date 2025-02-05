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
    Oblicza opóźnienie pomiędzy czasem zawartym w komunikacie a bieżącym czasem UTC.
    
    Najpierw próbuje wykorzystać pełny znacznik czasu z pola 'time_utc'
    (dostarczanego w MetaData). Jeśli nie jest on dostępny, używa pola 'Timestamp'
    z PositionReport – ale UWAGA: pole to zawiera tylko liczbę sekund (0–59)
    i nie jest pełnym znacznikiem czasu.
    """
    # Próba pobrania pełnego znacznika czasu z MetaData
    meta = message.get("MetaData", {})
    time_utc_str = meta.get("time_utc")
    if time_utc_str:
        try:
            # Przykładowy format: "2025-02-05 06:43:45.366066421 +0000 UTC"
            # Usuwamy ewentualne końcowe " UTC"
            time_utc_str = time_utc_str.replace(" UTC", "")
            if '.' in time_utc_str:
                date_part, frac_and_zone = time_utc_str.split('.', 1)
                if ' ' in frac_and_zone:
                    frac_str, tz_part = frac_and_zone.split(' ', 1)
                    # Skracamy część ułamkową do 6 cyfr (microsekundy)
                    frac_str = frac_str[:6]
                    fixed_time_str = f"{date_part}.{frac_str} {tz_part}"
                    dt_report = datetime.strptime(fixed_time_str, "%Y-%m-%d %H:%M:%S.%f %z")
                else:
                    dt_report = datetime.strptime(time_utc_str, "%Y-%m-%d %H:%M:%S.%f")
                    dt_report = dt_report.replace(tzinfo=timezone.utc)
            else:
                dt_report = datetime.strptime(time_utc_str, "%Y-%m-%d %H:%M:%S %z")
        except Exception as e:
            logger.error(f"Błąd parsowania MetaData time_utc: {e}")
            return
    else:
        # Jeśli MetaData nie zawiera pełnego czasu, wykorzystujemy pole Timestamp z PositionReport.
        # UWAGA: Pole Timestamp zawiera tylko liczbę sekund (0–59) i nie jest pełnym czasem.
        position_report = message.get("Message", {}).get("PositionReport", {})
        timestamp_value = position_report.get("Timestamp")
        if timestamp_value is None:
            logger.error("Brak pola Timestamp w PositionReport.")
            return
        try:
            timestamp_int = int(timestamp_value)
            # Założenie: komunikat dotyczy bieżącej minuty.
            now = datetime.now(timezone.utc)
            dt_report = now.replace(second=timestamp_int, microsecond=0)
            logger.warning("Używamy pola Timestamp z PositionReport – jest to tylko liczba sekund (0-59)!")
        except Exception as e:
            logger.error(f"Błąd konwersji Timestamp: {e}")
            return

    now_utc = datetime.now(timezone.utc)
    delay_sec = (now_utc - dt_report).total_seconds()
    logger.info(f"Opóźnienie PositionReport: {delay_sec:.1f} s")

if __name__ == "__main__":
    try:
        asyncio.run(connect_ais_stream())
    except KeyboardInterrupt:
        logger.info("Program przerwany przez użytkownika.")