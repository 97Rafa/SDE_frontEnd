import asyncio
from app.config import settings
from aiokafka import AIOKafkaProducer
import json

KAFKA_BROKER = settings.kafka_broker
producer: AIOKafkaProducer | None = None

def serializer(value):
    return json.dumps(value).encode()

async def start_producer(retries: int = 5, delay: int = 3):
    """Start global Kafka producer with retries."""
    global producer
    if producer is None:
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=serializer
        )
        for attempt in range(1, retries + 1):
            try:
                await producer.start()
                print("✅ Kafka producer connected")
                return
            except Exception as e:
                print(f"⚠️ Kafka producer connection failed (attempt {attempt}/{retries}): {e}")
                await asyncio.sleep(delay)
        raise RuntimeError("❌ Failed to connect Kafka producer after retries")

async def stop_producer():
    global producer
    if producer:
        await producer.stop()
        producer = None

async def produce(topic: str, message: dict):
    if producer is None:
        raise RuntimeError("Producer not started. Call start_producer() first.")
    await producer.send_and_wait(topic, message)

