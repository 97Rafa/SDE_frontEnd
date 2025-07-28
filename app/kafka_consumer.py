import asyncio, json, time
from aiokafka import AIOKafkaConsumer

KAFKA_BROKER= 'kafka1:9092'
CONS_TIMEOUT=1000

async def consume(topic, max_duration=5):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=False,   
        group_id=None,  # Important: no committed offsets
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    await consumer.start()
    try:
        topic_msgs = []
        deadline = time.time() + max_duration

        while time.time() < deadline:
            result = await consumer.getmany(timeout_ms=CONS_TIMEOUT)

            if result:
                for tp, messages in result.items():
                    for msg in messages:
                        topic_msgs.append(msg.value)
            else:
                break
    finally:
        await consumer.stop()
        print("Consumer stopped.")
    return topic_msgs

if __name__ == '__main__':
    data = asyncio.run(consume('request_topic'))
    print(data)
