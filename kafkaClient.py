from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
import json, time

def get_kafka_producer(kafka_broker,retries=5, delay=5):
    for i in range(retries):
        try:
            producer =  KafkaProducer(
                bootstrap_servers=kafka_broker,  # Replace with your Kafka server
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
            return producer
        except KafkaError as e:
            print(f"Attempt {i+1} failed: Kafka broker not ready, retrying in {delay} seconds...")
            time.sleep(delay)
    raise Exception("Kafka broker is unreachable after multiple attempts")

def get_kafka_consumer(topic, kafka_broker, retries=5, delay=5):
    for i in range(retries):
        try:    
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=kafka_broker,  # Replace with your Kafka server
                auto_offset_reset='earliest',  # Adjust as needed ('latest' for recent messages)
                consumer_timeout_ms=1000,       # Time to wait before consumer stops if no messages
                enable_auto_commit=False,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                )
            return consumer
        except KafkaError as e:
            print(f"Attempt {i+1} failed: Kafka broker not ready, retrying in {delay} seconds...")
            time.sleep(delay)
    raise Exception("Kafka broker is unreachable after multiple attempts")