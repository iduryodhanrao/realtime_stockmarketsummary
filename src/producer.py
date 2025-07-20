# producer.py
import os
import json
import asyncio
import websockets
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

FMP_API_KEY = os.getenv("FMP_API_KEY")
KAFKA_TOPIC = "stock_ticks"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# A list of tickers to subscribe to
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN"]

def create_kafka_producer():
    """Creates a Kafka producer instance."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

async def subscribe_to_fmp(producer):
    """Subscribes to the FMP WebSocket and sends data to Kafka."""
    uri = f"wss://financialmodelingprep.com/socket?apikey={FMP_API_KEY}"
    
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                print("✅ Connected to FMP WebSocket.")
                
                # Subscribe to tickers
                for ticker in TICKERS:
                    await websocket.send(json.dumps({"event": "subscribe", "data": {"ticker": ticker}}))
                    print(f"   -> Subscribed to {ticker}")

                # Listen for messages
                async for message in websocket:
                    data = json.loads(message)
                    # Use ticker symbol as the key for partitioning
                    key = data.get("s", "UNKNOWN").encode('utf-8')
                    producer.send(KAFKA_TOPIC, key=key, value=data)
                    print(f"Sent: {data}")

        except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.ConnectionClosedOK) as e:
            print(f"❌ WebSocket connection closed: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"❌ An unexpected error occurred: {e}. Reconnecting in 10 seconds...")
            await asyncio.sleep(10)


if __name__ == "__main__":
    kafka_producer = create_kafka_producer()
    asyncio.run(subscribe_to_fmp(kafka_producer))