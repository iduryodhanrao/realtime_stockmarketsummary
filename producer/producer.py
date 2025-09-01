from kafka import KafkaProducer
import os

producer = KafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
topic = os.getenv("TOPIC_NAME", "my-test-topic")

for i in range(5):
    producer.send(topic, f"Message {i}".encode("utf-8"))
    print(f"Sent: Message {i}")
producer.flush()