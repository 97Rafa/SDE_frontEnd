from aiokafka import AIOKafkaProducer
import json


KAFKA_BROKER= 'kafka:9093'

producer = None

def serializer(value):
    return json.dumps(value).encode()

async def produce(topic: str, message: dict):
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=serializer)
    await producer.start()
    try:
        await producer.send_and_wait(topic, message)
    except Exception as e:
        print(e)
    finally:
        await producer.stop()
