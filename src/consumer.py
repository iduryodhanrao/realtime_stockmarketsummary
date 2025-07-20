# consumer.py
import os
import json
import time
from collections import defaultdict
from datetime import datetime, timezone

import psycopg2
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

# --- Kafka Configuration ---
KAFKA_TOPIC = "stock_ticks"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# --- Database Configuration ---
DB_NAME = "stock_db"
DB_USER = "user"
DB_PASSWORD = "password"
DB_HOST = "localhost"
DB_PORT = "5433"

# --- Aggregation Configuration ---
WINDOW_SIZE_SECONDS = 60
windows = defaultdict(lambda: {"ticks": [], "start_time": time.time()})

def get_db_connection():
    """Establishes connection to the PostgreSQL database."""
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )

def upsert_summary(conn, summary):
    """Inserts or updates a 1-minute stock summary."""
    sql = """
        INSERT INTO stock_summary_1min (ticker, summary_time, open_price, high_price, low_price, close_price, total_volume)
        VALUES (%(ticker)s, %(summary_time)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
        ON CONFLICT (ticker, summary_time) DO UPDATE SET
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            total_volume = EXCLUDED.total_volume;
    """
    with conn.cursor() as cur:
        cur.execute(sql, summary)
        conn.commit()

def process_window(conn, ticker, window_data):
    """Calculates summary statistics for a window and upserts to the DB."""
    ticks = window_data["ticks"]
    if not ticks:
        return

    # Sort ticks by timestamp to ensure correct OHLC
    ticks.sort(key=lambda x: x['t'])
    
    summary = {
        "ticker": ticker,
        "summary_time": datetime.fromtimestamp(window_data["start_time"], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:00'),
        "open": ticks[0]['p'],
        "high": max(t['p'] for t in ticks),
        "low": min(t['p'] for t in ticks),
        "close": ticks[-1]['p'],
        "volume": sum(t['v'] for t in ticks)
    }
    
    upsert_summary(conn, summary)
    print(f"Processed summary for {ticker}: {summary}")

def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest'
    )
    db_conn = get_db_connection()

    print("🚀 Consumer started. Listening for stock ticks...")

    try:
        for message in consumer:
            tick = message.value
            ticker = tick.get("s")
            
            if not ticker or tick.get("type") != "trade":
                continue # Skip non-trade messages or messages without a ticker

            # Add tick to the current window
            windows[ticker]["ticks"].append(tick)

            # Check if any windows need to be processed
            current_time = time.time()
            for t, window_data in list(windows.items()):
                if current_time - window_data["start_time"] >= WINDOW_SIZE_SECONDS:
                    process_window(db_conn, t, window_data)
                    # Reset the window for this ticker
                    windows[t] = {"ticks": [], "start_time": current_time}
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        db_conn.close()

if __name__ == "__main__":
    main()