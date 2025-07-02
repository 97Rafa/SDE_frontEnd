import asyncio
from aiokafka import AIOKafkaConsumer
import json

KAFKA_BOOTSTRAP_SERVERS = "kafka:9093"

async def consume():
    consumer = AIOKafkaConsumer(
        "topic1",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="my-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    await consumer.start()
    print("Kafka consumer started...")
    try:
        async for msg in consumer:
            print("\n📥 Νέο μήνυμα:")
            print(json.dumps(msg.value, indent=2))
    finally:
        await consumer.stop()

# Εκκίνηση αν τρέχεις standalone
if __name__ == "__main__":
    asyncio.run(consume())
