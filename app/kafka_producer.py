from aiokafka import AIOKafkaProducer
import json

KAFKA_BOOTSTRAP_SERVERS = "kafka:9093"

producer = None

async def get_producer():
    global producer
    if not producer:
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        await producer.start()
    return producer

async def send_message(topic: str, message: dict):
    producer = await get_producer()
    await producer.send_and_wait(topic, message)
