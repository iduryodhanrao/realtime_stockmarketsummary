# news_producer.py
import os
import json
import time
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

NEWS_API_KEY = os.getenv("NEWS_API_KEY")
KAFKA_TOPIC = "realtime_news"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
#KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

print(producer.config['api_version'])

def fetch_news():
    url = f"https://newsapi.org/v2/top-headlines?language=en&pageSize=5&apiKey={NEWS_API_KEY}"
    try:
        response = requests.get(url)
        articles = response.json().get("articles", [])
        return articles
    except Exception as e:
        print(f"Error fetching news: {e}")
        return []

def stream_news():
    while True:
        articles = fetch_news()
        for article in articles:
            headline = article.get("title", "No Title")
            source = article.get("source", {}).get("name", "Unknown")
            payload = {
                "headline": headline,
                "source": source,
                "timestamp": time.time()
            }
            producer.send(KAFKA_TOPIC, key=source, value=payload)

            print(f"📰 Sent: {headline}")
        time.sleep(60)  # Poll every minute

if __name__ == "__main__":
    stream_news()