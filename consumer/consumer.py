from kafka import KafkaConsumer
import os, json
import logging
import sys
sys.stdout.flush()

logging.basicConfig(level=logging.INFO)

x = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
logging.info(f"kafka server: {x}")
consumer = KafkaConsumer(
    os.getenv("TOPIC_NAME", "realtime_news"),
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    group_id=os.getenv("GROUP_ID", "news_analytics_group"),
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("📥 Listening for news updates...\n")
try: 
    for msg in consumer:
        logging.info(f"Consumed: {msg.offset} ")
        logging.info(f"[{msg.value['source']}] 🗞️ {msg.value['headline']}")
        print(f"[{msg.value['source']}] 🗞️ {msg.value['headline']}")
        consumer.commit()
except Exception as e:
    print(f"Error: {e}")
