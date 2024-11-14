import select
import json
import psycopg2
import psycopg2.extensions
from kafka import KafkaProducer

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

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data to JSON
)

def send_to_kafka(topic, message):
    """Send a message to the specified Kafka topic."""
    try:
        producer.send(topic, value=message)
        producer.flush()
        print(f"Sent to {topic}: {message}")
    except Exception as e:
        print(f"Error sending message to Kafka: {e}")

def setup_trigger(cursor, table_name):
    """Function to create a notify trigger on the specified table."""
    create_function_sql = f"""
    CREATE OR REPLACE FUNCTION notify_{table_name}_insert()
    RETURNS TRIGGER AS $$
    BEGIN
         -- Sends a notification with the payload (e.g., the row's ID or the entire row as JSON)
        PERFORM pg_notify('my_channel', row_to_json(NEW)::text);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """
    create_trigger_sql = f"""
    DROP TRIGGER IF EXISTS {table_name}_insert_trigger
    ON {table_name};
    CREATE TRIGGER {table_name}_insert_trigger
    AFTER INSERT ON {table_name}
    FOR EACH ROW EXECUTE FUNCTION notify_{table_name}_insert();
    """
    cursor.execute(create_function_sql)
    cursor.execute(create_trigger_sql)

# Connect to the PostgreSQL database
connection = psycopg2.connect(**POSTGRESQL_CONFIG)
connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
cursor = connection.cursor()

# Set up triggers on each table
setup_trigger(cursor, 'requests')
setup_trigger(cursor, 'datain')
setup_trigger(cursor, 'estimations')
print("Triggers created on tables requests, datain, and estimations.")

# Start listening for notifications
cursor.execute("LISTEN my_channel;")
print("Listening for notifications on 'my_channel'...")

try:
    while True:
        if select.select([connection], [], [], 5) == ([], [], []):
            print("Waiting for notifications...")
        else:
            connection.poll()
            while connection.notifies:
                notify = connection.notifies.pop(0)
                payload = json.loads(notify.payload)
                print(f"Received notification: {payload}")
                
                # # Send the payload to Kafka
                # producer.send('your_kafka_topic', value=payload)
                # print("Sent data to Kafka.")
               
except KeyboardInterrupt:
    print("\nStopped listening.")

# Clean up
cursor.close()
connection.close()
