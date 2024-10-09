import time
import psycopg2
from kafka import KafkaProducer
import json

# PostgreSQL connection settings
POSTGRESQL_CONFIG = {
    'host': '172.18.0.2',
    'port': '5432',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'postgres'
}

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
DAT_TOPIC = 'data_topic'
REQ_TOPIC = 'request_topic'

# Polling interval in seconds
POLLING_INTERVAL = 5

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data to JSON
)

def get_postgresql_connection():
    """Connect to PostgreSQL."""
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def fetch_new_records(cursor, last_id):
    """
    Fetch new records from the table where the ID is greater than the last processed ID.
    Replace 'your_table' with your actual table name.
    """
    query = f"""
        SELECT id, body, req_type
        FROM kafka_reqs
        WHERE id > %s
        ORDER BY id ASC;
    """
    cursor.execute(query, (last_id,))
    return cursor.fetchall()

def send_to_kafka(topic, message):
    """Send a message to the specified Kafka topic."""
    try:
        producer.send(topic, value=message)
        producer.flush()
        print(f"Sent to {topic}: {message}")
    except Exception as e:
        print(f"Error sending message to Kafka: {e}")

def poll_database():
    """Poll the database for new records and send them to Kafka."""
    last_id = 0  # Keep track of the last processed ID to avoid duplicates

    # Get PostgreSQL connection
    conn = get_postgresql_connection()
    if conn is None:
        return

    cursor = conn.cursor()

    try:
        while True:
            # Fetch new records
            records = fetch_new_records(cursor, last_id)

            # Process each record
            for record in records:
                record_id, record_body, req_type = record

                # Routing logic
                if req_type == 'Data':
                    topic = DAT_TOPIC
                elif req_type == 'Request':
                    topic = REQ_TOPIC
                
                send_to_kafka(topic, record_body)
                last_id = record_id  # Update the last processed ID

            # Sleep for a while before polling again
            time.sleep(POLLING_INTERVAL)
    except KeyboardInterrupt:
        print("=====================================")
        print("======= Polling stopped =============")
        print("=====================================")
    finally:
        # Close the database connection
        cursor.close()
        conn.close()

if __name__ == '__main__':
    poll_database()
