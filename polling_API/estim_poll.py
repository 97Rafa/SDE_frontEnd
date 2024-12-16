from kafka import KafkaConsumer
import json

KAFKA_BROKER = 'localhost:9092'
EST_TOPIC = 'estimation_topic'

# Initialize Kafka consumer
consumer = KafkaConsumer(
    EST_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)


print("Listening for messages on topic 'EST_TOPIC'...")
i=0
# Continuously listen for new messages
for message in consumer:
    print(f"!!! {i} !!!Received message: {message.value}\n")
    i+=1