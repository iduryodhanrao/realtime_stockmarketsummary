import os
import json
import asyncio
import websockets
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

FH_API_KEY = os.getenv("FH_API_KEY")
KAFKA_TOPIC = "stock_ticks"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# List of tickers to subscribe to
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN"]

def create_kafka_producer():
    """Creates a Kafka producer instance."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8')
    )

async def subscribe_and_stream(producer):
    """Subscribes to Finnhub WebSocket and streams data to Kafka."""
    uri = f"wss://ws.finnhub.io?token={FH_API_KEY}"
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                print("✅ Connected to Finnhub WebSocket.")

                # Subscribe to tickers
                for ticker in TICKERS:
                    await websocket.send(json.dumps({"type": "subscribe", "symbol": ticker}))
                    print(f"   -> Subscribed to {ticker}")

                # Listen for messages
                async for message in websocket:
                    try:
                        data = json.loads(message)
                        # Finnhub sends messages with 'data' key containing a list of ticks
                        if "data" in data and isinstance(data["data"], list):
                            for tick in data["data"]:
                                symbol = tick.get("s", "UNKNOWN")
                                producer.send(KAFKA_TOPIC, key=symbol, value=tick)
                                print(f"Sent to Kafka: {tick}")
                        else:
                            print(f"Received non-tick message: {data}")
                    except Exception as msg_err:
                        print(f"Error processing message: {msg_err}")

        except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.ConnectionClosedOK) as e:
            print(f"❌ WebSocket connection closed: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"❌ An unexpected error occurred: {e}. Reconnecting in 10 seconds...")
            await asyncio.sleep(10)

if __name__ == "__main__":
    kafka_producer = create_kafka_producer()
    asyncio.run(subscribe_and_stream(kafka_producer))