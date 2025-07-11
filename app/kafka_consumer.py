import asyncio, json
from aiokafka import AIOKafkaConsumer

KAFKA_BROKER= 'kafka:9093'
CONS_TIMEOUT = 1000

async def consume(topic):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,   
        group_id="sde_client_group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    await consumer.start()
    try:
        print(f"Consuming from topic '{topic}'...")
        topic_msgs = []

        while True:
            result = await consumer.getmany(timeout_ms=CONS_TIMEOUT)

            if result:
                for tp, messages in result.items():
                    for msg in messages:
                        # print(f"Received: {msg.value.decode('utf-8')}")
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
