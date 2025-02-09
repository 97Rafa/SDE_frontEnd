import json, time
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError


class KafkaProducer:
    def __init__(self, broker, client_id='python-producer'):
        """Initialize Kafka Producer."""
        self.broker = broker
        self.client_id = client_id
        self.producer = None
        self._create_producer()

    def _create_producer(self):
        """Create a Kafka producer instance."""
        conf = {
            'bootstrap.servers': self.broker,
            'client.id': self.client_id
        }
        self.producer = Producer(conf)

    def produce_message(self, topic, message_json):
        """Produce a JSON message to Kafka."""
        try:
            # Validate if the provided message is valid JSON
            json.loads(message_json)

            # Produce the JSON string message
            self.producer.produce(topic, message_json.encode('utf-8'))
            self.producer.flush()  # Ensure all messages are delivered
            print(f"Message sent to topic '{topic}': {message_json}")
        except KafkaException as e:
            print(f"Error in producing message: {e}")
        except json.JSONDecodeError:
            print("Provided message is not valid JSON.")


class KafkaConsumer:
    def __init__(self, broker, topic,group_id=None):
        """Initialize Kafka Consumer."""
        self.broker = broker
        self.group_id = group_id if group_id else "default-group"
        self.topic = topic
        self.consumer = None
        self._create_consumer()

    def _create_consumer(self):
        """Create a Kafka consumer instance."""
        conf = {
            'bootstrap.servers': self.broker,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        self.consumer = Consumer(conf)
        self.consumer.subscribe([self.topic])

    def consume(self):
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)  # Poll messages with a timeout

                if msg is None:
                    continue  # No message received, continue polling

                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                # Process the message
                data = json.loads(msg.value().decode('utf-8'))
                return data

        except KeyboardInterrupt:
            print("Stopping consumer...")
        finally:
            self.consumer.close()  # Ensure consumer is properly closed

    def consume_messages_bulk(self):
        consumed_msgs = []
        empty_count = 0  # Counter to track empty polls
        max_empty_polls = 5  # Stop after N consecutive empty polls

        try:
            while empty_count < max_empty_polls:
                msg = self.consumer.poll(timeout=1.0)  # Adjust timeout if needed

                if msg is None:
                    empty_count += 1  # No message received, increase empty count
                    continue

                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                data = json.loads(msg.value().decode('utf-8'))
                consumed_msgs.append(data)
                empty_count = 0  # Reset empty counter when a message is received
            return consumed_msgs
        finally:
            self.consumer.close()

    def close(self):
        """Close the consumer."""
        self.consumer.close()