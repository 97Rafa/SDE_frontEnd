import threading
import time
import psycopg2
from kafka import KafkaProducer, KafkaConsumer
import json


# PostgreSQL connection settings
POSTGRESQL_CONFIG = {
    'host': '172.18.0.42',
    'port': '5432',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'postgres'
}

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
DAT_TOPIC = 'data_topic'
REQ_TOPIC = 'request_topic'
EST_TOPIC = 'estimation_topic'

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
                    send_to_kafka(topic, record_body)
                elif req_type == 'Request':
                    topic = REQ_TOPIC
                    send_to_kafka(topic, record_body)
                else:
                    print('Wrong req_type!')
                
                last_id = record_id  # Update the last processed ID

            # Sleep for a while before polling again
            time.sleep(POLLING_INTERVAL)
    except KeyboardInterrupt:
        print("\n=====================================")
        print("======= Polling stopped =============")
        print("=====================================")
    finally:
        # Close the database connection
        cursor.close()
        conn.close()


# Function to insert data into the new PostgreSQL table
def insert_into_new_table(conn, data):
    """
    Insert consumed data into the new PostgreSQL table.
    """
    try:
        query = f"INSERT INTO kafka_ests (body,status) VALUES('{data}', 'completed');"  # Customize columns
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        print(f"Inserted data into new table: {data}")
    except Exception as e:
        print(f"Error inserting into new table: {e}")
        conn.rollback()

# Kafka consumer to poll from the estimation_topic
def consume_from_estimation_topic():
    """
    Consume messages from estimation_topic and insert into the new table.
    """
    consumer = KafkaConsumer(
        EST_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='estimation-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # Get PostgreSQL connection
    conn = get_postgresql_connection()
    if conn is None:
        print("Connection not found")
        return
    try:
        while True:
            for message in consumer:
    
                estimation_data = json.dumps(message.value)

                # print(json.dumps(message.value))

                # Insert into PostgreSQL new table
                insert_into_new_table(conn, estimation_data)

                # Optionally print or log the data consumed
                print(f"Consumed data from {EST_TOPIC}: {estimation_data}")

    except KeyboardInterrupt:
        print("Cancelled")
    finally:
        # Close the database connection
        conn.close()


# Thread to poll the database for request and data topics
def poll_database_thread():
    print("Starting database polling thread...")
    poll_database()

# Thread to consume from estimation_topic and insert into new table
def consume_estimation_topic_thread():
    print("Starting Kafka consumer thread...")
    consume_from_estimation_topic()

# Main function to start threads
if __name__ == "__main__":
    # Create two threads: one for database polling and one for Kafka consumption
    db_thread = threading.Thread(target=poll_database_thread)
    kafka_thread = threading.Thread(target=consume_estimation_topic_thread)

    # Start both threads
    db_thread.start()
    kafka_thread.start()

    # Join threads to keep them running
    db_thread.join()
    kafka_thread.join()