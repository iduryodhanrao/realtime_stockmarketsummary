from kafka import KafkaConsumer
import os

consumer = KafkaConsumer(
    os.getenv("TOPIC_NAME", "my-test-topic"),
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    auto_offset_reset='earliest',
    group_id='test-group',
    api_version=(2, 8, 0)
)

for msg in consumer:
    print(f"Received: {msg.value.decode('utf-8')}")