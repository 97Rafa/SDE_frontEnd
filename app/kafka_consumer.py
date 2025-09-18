import asyncio
import json
from aiokafka import AIOKafkaConsumer
from collections import defaultdict, deque
from app.config import settings

KAFKA_BROKER = settings.kafka_broker
MAX_BUFFER_SIZE = 1000  # max messages to keep per topic

# In-memory buffers per topic
message_buffers: dict[str, deque] = defaultdict(lambda: deque(maxlen=MAX_BUFFER_SIZE))

# Background consumer tasks
consumer_tasks: dict[str, asyncio.Task] = {}


async def consume_forever(topic: str, retries: int = 5, delay: int = 3):
    """
    Background task to continuously consume messages from a Kafka topic.
    Only dict messages are appended to the in-memory buffer.
    """
    for attempt in range(1, retries + 1):
        try:
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                group_id=None,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            await consumer.start()
            print(f"✅ Kafka consumer connected for topic {topic}")

            try:
                async for msg in consumer:
                    # Ensure only dicts are stored
                    value = msg.value
                    if isinstance(value, dict):
                        message_buffers[topic].append(value)
                    else:
                        print(f"⚠️ Skipping non-dict message on {topic}: {value}")

            finally:
                await consumer.stop()

            return

        except Exception as e:
            print(f"⚠️ Kafka consumer connection failed for {topic} (attempt {attempt}/{retries}): {e}")
            await asyncio.sleep(delay)

    raise RuntimeError(f"❌ Failed to connect Kafka consumer for {topic} after {retries} retries")


async def start_consumers(topics: list[str]):
    """Start one background consumer per topic."""
    global consumer_tasks
    for topic in topics:
        if topic not in consumer_tasks:
            task = asyncio.create_task(consume_forever(topic))
            consumer_tasks[topic] = task


async def stop_consumers():
    """Stop all background consumer tasks gracefully."""
    global consumer_tasks
    for task in consumer_tasks.values():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    consumer_tasks.clear()


def get_cons_messages(topic: str, limit: int = 100):
    """
    Retrieve the most recent N messages from the in-memory buffer.
    Only dict messages are returned.
    """
    if topic not in message_buffers:
        return []
    return [m for m in list(message_buffers[topic])[-limit:] if isinstance(m, dict)]
