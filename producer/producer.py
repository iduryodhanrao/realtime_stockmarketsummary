from kafka import KafkaProducer
import os, json, time
x = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
print(f"kafka server: {x}")
producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = os.getenv("TOPIC_NAME", "realtime_news")

for i in range(5):
    msg = {"source": "Reuters", "headline": f"Breaking news #{i}"}
    producer.send(topic, value=msg)
    print(f"📤 Sent: {msg}")
    time.sleep(1)

producer.flush()