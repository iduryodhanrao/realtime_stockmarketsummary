# news_consumer.py
import json
from kafka import KafkaConsumer
import logging
logging.basicConfig(level=logging.INFO)

KAFKA_TOPIC = "realtime_news"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',   #start from - 'latest' or  'earliest'
    group_id='news_group',
    enable_auto_commit=False,       # True - may lose messages if consumer crashes without processing
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None
    

)


print("📥 Listening for news updates...\n")
try: 
    for message in consumer:
        logging.info(f"Recieved: {message.value} (offset: {message.offset})")
        #news = message.value
        #print(f"[{news['source']}] 🗞️ {news['headline']}")
        consumer.commit()
        logging.info(f"Offset commited: {message.offset}")
except Exception as e:
    logging.error(f"Error during consumption: {e}")

finally:
    consumer.close()
