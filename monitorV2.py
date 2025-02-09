import threading,time
import select
import json
from pprint import pprint
import psycopg2
import psycopg2.extensions
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError

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
POLLING_INTERVAL = 3

# Configuration for the Kafka producer
conf = {
    'bootstrap.servers': KAFKA_BROKER,  # Replace with your Kafka server address
}
try:
    producer = Producer(conf)
    print("✅ Kafka Producer connected successfully!")
except Exception as e:
    print(f"❌ Failed to connect Kafka Producer: {e}")


try:
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'my_group',
        'auto.offset.reset': 'earliest'
    })
    print("✅ Kafka Consumer connected successfully!")
except Exception as e:
    print(f"❌ Failed to connect Kafka Consumer: {e}")

def extract_message_value(msg):
    """Extracts and converts only the Kafka message value to JSON."""
    if msg is None or msg.value() is None:
        return None

    try:
        return json.loads(msg.value().decode('utf-8'))  # Convert string to JSON
    except json.JSONDecodeError:
        return {"error": "Invalid JSON format", "raw_value": msg.value().decode('utf-8')}



def send_to_kafka(topic, message):
    """Send a message to the specified Kafka topic."""
    try:
        producer.produce(topic, message.encode('utf-8'))
        producer.flush()
        print(f"Sent to {topic}: {message}")
    except Exception as e:
        print(f"Error sending message to Kafka: {e}")

def insert_trigger(cursor, table_name):
    """Function to create a notify trigger on the specified table."""
    create_ins_fnc = f"""
        CREATE OR REPLACE FUNCTION notify_{table_name}_insert()
        RETURNS TRIGGER AS $$
        BEGIN
            -- Sends a notification with the payload
            PERFORM pg_notify('my_channel',
                            json_build_object(
                                    'table', '{table_name}',
                                    'operation', 'INSERT',
                                    'data', row_to_json(NEW)
                                )::text);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    create_ins_trg = f"""
        DROP TRIGGER IF EXISTS {table_name}_insert_trigger
        ON {table_name};
        CREATE TRIGGER {table_name}_insert_trigger
        AFTER INSERT ON {table_name}
        FOR EACH ROW EXECUTE FUNCTION notify_{table_name}_insert();
        """
    cursor.execute(create_ins_fnc)
    cursor.execute(create_ins_trg)

def update_trigger(cursor):
    create_upd_fnc = f"""
        CREATE OR REPLACE FUNCTION notify_estimations_changes()
        RETURNS TRIGGER AS $$
        BEGIN
            -- Send notification with operation type
            PERFORM pg_notify(
                'my_channel',
                json_build_object(
                    'table', 'estimations',
                    'operation', 'UPDATE',
                    'data', row_to_json(NEW)
                )::text
            );
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    create_upd_trg = f"""
        DROP TRIGGER IF EXISTS notify_estimations_trigger
            ON estimations;
            CREATE TRIGGER notify_estimations_trigger
            AFTER UPDATE OF last_req ON estimations
            FOR EACH ROW EXECUTE FUNCTION notify_estimations_changes();
        """
    cursor.execute(create_upd_fnc)
    cursor.execute(create_upd_trg)

def update_estimation(cursor, kafkaJson, est_uid):
    update_function_sql = f"""
        UPDATE estimations 
        SET data = '{kafkaJson}'::jsonb,
            last_data = (SELECT CURRENT_TIMESTAMP)
        WHERE "synopsisUID" = {est_uid};
        """
    cursor.execute(update_function_sql)

done = False

def poll_db():
    # Connect to the PostgreSQL database
    connection = psycopg2.connect(**POSTGRESQL_CONFIG)
    connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connection.cursor()

    # Set up triggers on each table
    insert_trigger(cursor, 'requests')
    insert_trigger(cursor, 'datain')
    insert_trigger(cursor, 'estimations')
    update_trigger(cursor)
    print("Triggers created on tables requests, datain, and estimations.")

    # Start listening for notifications
    cursor.execute("LISTEN my_channel;")
    try:
        while not done:
            if select.select([connection], [], [], 5) == ([], [], []):
                print("Waiting for notifications... Press Enter to quit.")
                update_refresh = f"""
                                UPDATE estimations 
                                SET "toRefresh" = 
                                    CASE
                                        WHEN last_req > (last_data+timeout) OR last_data IS NULL THEN true
                                        ELSE false 
                                    END;
                                """
                cursor.execute(update_refresh)
                time.sleep(POLLING_INTERVAL)
            else:
                connection.poll()
                while connection.notifies:
                    notify = connection.notifies.pop(0)
                    payload = json.loads(notify.payload)
                    operation = payload.get("operation")
                    row_data = payload["data"]
                    table = payload["table"]

                    print("--------------------------------------------------")
                    print(f"Received notification from table {table}")
                    print("--------------------------------------------------")

                    if operation == 'INSERT':
                        print('This is an INSERT operation')
                        if table == "requests" or table == "estimations":
                            topic = REQ_TOPIC
                        elif table == "datain":
                            topic = DAT_TOPIC
                        else:
                            continue
                    elif operation == 'UPDATE':
                        print('This is an UPDATE operation')
                        if row_data['toRefresh']:
                            print('The estimation has expired, let me grab some fresh ones')
                            topic = REQ_TOPIC
                        else:
                            old_data=f"""
                                    SELECT data 
                                    FROM estimations 
                                    WHERE "synopsisUID" = {row_data['body']['uid']} 
                                    """
                            cursor.execute(old_data)
                            result = cursor.fetchall()
                            print("--------------------------------------------------")
                            print('The old estimation is still fresh:')
                            pprint(result)
                            print("--------------------------------------------------")
                            continue
                    
                    # Send the payload to Kafka
                    send_to_kafka(topic, json.dumps(row_data['body']))
                    print("--------------------------------------------------")
                    print("Sent data to Kafka.")                
                    print("--------------------------------------------------")
    finally:
        # Clean up
        cursor.close()
        connection.close()


def poll_kafka():
    # Connect to the PostgreSQL database
    connection = psycopg2.connect(**POSTGRESQL_CONFIG)
    connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connection.cursor()

    # Subscribe to the topic
    consumer.subscribe([EST_TOPIC])
    
    try:
        while not done:
            # Poll for a message (timeout of 1 second)
            message = consumer.poll(timeout=1.0)

            if message is None:
                # No message available within the timeout
                continue
            if message.error():
                # Error handling
                if message.error().code() == KafkaError._PARTITION_EOF:
                    print('End of partition reached: {}'.format(message))
                else:
                    raise KafkaException(message.error())
            else:
                # Message received successfully
                print('Received message: {}'.format(message.value().decode('utf-8')))
                # Extract 'uid' from the 'estimation'
                msg_value = extract_message_value(message)     
                est_uid = msg_value['uid']
                update_estimation(cursor, json.dumps(msg_value), est_uid)
    except KafkaException as e:
        print(e)
    finally:
        # Clean up
        cursor.close()
        connection.close()
        consumer.close()


# Main function to start threads
if __name__ == "__main__":
    # Create two threads
    db_thread = threading.Thread(target=poll_db, daemon=True)
    kafka_thread = threading.Thread(target=poll_kafka, daemon=True)

    # Start both threads
    db_thread.start()
    kafka_thread.start()

    print('===================')
    input('Press Enter to quit\n')

    done = True