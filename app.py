from flask import Flask, request, jsonify, make_response,json
from os import environ, path, makedirs
from datetime import timedelta, datetime
from models import db, Requests, Estimations, DataIn, create_tables
from sqlalchemy.dialects.postgresql import insert
from confluent_kafka import Producer, Consumer
from werkzeug.utils import secure_filename
import csv

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = environ.get('DB_URL')
UPLOAD_FOLDER = 'static/files'
makedirs(UPLOAD_FOLDER, exist_ok=True)  # Ensure the folder exists



# Configure Kafka broker
KAFKA_BROKER = environ.get('KAFKA_BROKER', 'kafka:9093')
DAT_TOPIC = 'data_topic'
REQ_TOPIC = 'request_topic'
EST_TOPIC = 'estimation_topic'

# Initialize the database
db.init_app(app)

# Create tables before starting the app
create_tables(app)


#Root route
@app.route('/', methods=['GET'])
def test():
    return "SDE Front End"



# =============================================
# ========== Kafka Direct =====================
# =============================================

# Configuration for the Kafka producer
conf = {
    'bootstrap.servers': KAFKA_BROKER,
}

# Create the Producer instance
producer = Producer(conf)

@app.route('/produce/<topic>', methods=['POST'])
def produce_message(topic):
    if topic not in [DAT_TOPIC, REQ_TOPIC]:
        return jsonify({'error': 'Invalid topic'}), 400
    try:
        data = request.json
        if not data:
            return jsonify({'error': 'Empty data'}), 400
        
        json_data = json.dumps(data) 
        # Produce a message
        producer.produce(topic, json_data.encode('utf-8'))

        # Wait for any outstanding messages to be delivered and delivery reports to be received
        producer.flush()
        return jsonify({'status': f'Message sent to topic: {topic}'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/consume/<topic>', methods=['GET'])
def consume_message(topic):
    
    # Configuration for the Kafka consumer
    conf = {
        'bootstrap.servers': KAFKA_BROKER,  
        'group.id': 'my_group',
        'auto.offset.reset': 'earliest'
    }

    # Create the Consumer instance
    consumer = Consumer(conf)

    # Subscribe to the topic
    consumer.subscribe([topic])

    consumed_msgs = []
    empty_count = 0  # Counter to track empty polls
    max_empty_polls = 5  # Stop after N consecutive empty polls

    try:
        while empty_count < max_empty_polls:
            msg = consumer.poll(timeout=1.0)  # Adjust timeout if needed

            if msg is None:
                empty_count += 1  # No message received, increase empty count
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            data = json.loads(msg.value().decode('utf-8'))
            consumed_msgs.append(data)
            empty_count = 0  # Reset empty counter when a message is received
        return jsonify({'messages': consumed_msgs}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        consumer.close()

# =============================================
# ========== Requests table ===================
# =============================================


#create a kafka request
@app.route('/requests', methods=['POST'])
def create_request():
    try:
        data = request.get_json()
        new_request = Requests(
            body=data['body']
            )
        db.session.add(new_request)
        db.session.commit()
        return make_response(jsonify({'message' : 'Kafka request created'}), 201)
    except Exception:
        return make_response(jsonify({'message' : 'Error creating request'}), 500)

#get all requests
@app.route('/requests', methods=['GET'])
def get_requests():
    try:
         # Query the database for all items
        requests = Requests.query.all()
        # Convert the result to a list of dictionaries
        requests_l = [request.toJson() for request in requests]
        return make_response(jsonify(requests_l), 200)
        
    except Exception:
        return make_response(jsonify({'message' : 'Error getting requests'}), 500)


# =============================================
# ========== Data table ===================
# =============================================
#create a Data input
@app.route('/dataIn', methods=['POST'])
def create_DataIn():
    try:
        data = request.get_json()
        new_dataIn = DataIn(
            body=data['body']
            )
        db.session.add(new_dataIn)
        db.session.commit()
        return make_response(jsonify({'message' : 'Data were inserted'}), 201)
    except Exception:
        return make_response(jsonify({'message' : 'Error inserting Data'}), 500)
    
@app.route('/dataIn/csv', methods=['POST'])
def create_DataIn_csv():
    try:
        # Ensure a file is part of the request
        if 'file' not in request.files:
            return make_response(jsonify({'message': 'No file part'}), 400)

        file = request.files['file']

        # If the user doesn't select a file or the file is empty
        if file.filename == '':
            return make_response(jsonify({'message': 'No selected file'}), 400)

        # Ensure it's a CSV file
        if not file.filename.endswith('.csv'):
            return make_response(jsonify({'message': 'File is not a CSV'}), 400)

        # Save the file to a secure location
        filename = secure_filename(file.filename)
        file_path = path.join(UPLOAD_FOLDER, filename)        
        file.save(file_path)

        # Read the CSV file
        with open(file_path, newline='') as csvfile:
            csvreader = csv.DictReader(csvfile)  # Automatically maps each row to a dictionary
            new_dataIn_list = []

            # Iterate over each row and add the data to the list
            for row in csvreader:
                # Ensure streamID and dataSetkey are present
                if 'StreamID' not in row or 'dataSetkey' not in row:
                    return make_response(jsonify({'message': 'CSV must contain streamID and dataSetkey columns'}), 400)

                streamID = row['StreamID']
                dataSetkey = row['dataSetkey']

                # Extract all other columns dynamically into "values"
                values = {key: row[key] for key in row.keys() if key not in ['streamID', 'dataSetkey']}

                body = {
                    'values': values,
                    'streamID': streamID,
                    'dataSetkey': dataSetkey
                }
                
                # Create the DataIn object
                new_dataIn = DataIn(body=body)

                # Add to the list of DataIn objects
                new_dataIn_list.append(new_dataIn)

            # Insert the data into the database
            db.session.add_all(new_dataIn_list)
            db.session.commit()

        return make_response(jsonify({'message': 'Data were inserted'}), 201)

    except Exception as e:
        return make_response(jsonify({'message': 'Error inserting Data', 'error': str(e)}), 500)

#get all data
@app.route('/dataIn', methods=['GET'])
def get_DataIn():
    try:
         # Query the database for all items
        dataIns = DataIn.query.all()
        # Convert the result to a list of dictionaries
        dataIn_l = [dataIn.toJson() for dataIn in dataIns]
        return make_response(jsonify(dataIn_l), 200)
        
    except Exception:
        return make_response(jsonify({'message' : 'Error getting Data'}), 500)


# =============================================
# ========== Estimations table ================
# =============================================
def parse_interval(interval_str):
    # This function converts a string like '5 minutes' into arguments for timedelta
    time_units = {'seconds': 0, 'minutes': 0}

    if 'minute' in interval_str:
        time_units['minutes'] = int(interval_str.split()[0])

    return time_units

#create an estimation
@app.route('/estimations', methods=['POST'])
def create_est_request():
    try:
        data = request.get_json()
        if not data:
            return make_response(jsonify({'message': 'Invalid JSON payload'}), 400)

        # Extract the 'body' key from the JSON payload
        req_body = data.get('body')
        if not req_body or not isinstance(req_body, dict):
            return make_response(jsonify({'message': "'body' must be a valid JSON object"}), 400)

        # Extract 'uid' from the 'body'
        req_uid = req_body.get('uid')
        if req_uid is None:
            return make_response(jsonify({'message': "'uid' is required in 'body'"}), 400)

        #timestamp for last_req
        now = datetime.now()
        # Parse the duration from the JSON if it's provided
        timeout = None
        if 'timeout' in data:
            timeout = data['timeout']  # This will be a string, e.g., '5 minutes'

        # Perform upsert: Insert new record or update 'last_req' if conflict occurs
        stmt = insert(Estimations).values(
            body=req_body,
            synopsisUID=req_uid,
            timeout=timedelta(**parse_interval(timeout)) if timeout else None,
            last_req=now
        ).on_conflict_do_update(
            index_elements=['synopsisUID'],  # Specify the column(s) to check for conflicts
            set_={'last_req': now}  # Update the 'last_req' column on conflict
        )

        # Execute the statement
        db.session.execute(stmt)
        
        db.session.commit()
        return make_response(jsonify({'message' : 'Estimation request created'}), 201)
    except Exception as e:
        return make_response(jsonify({'message' :{e}}), 500)

#get all estimations
@app.route('/estimations', methods=['GET'])
def get_estimations():
    try:
         # Query the database for all items
        estimations = Estimations.query.all()
        # Convert the result to a list of dictionaries
        estimations_l = [estimation.toJson() for estimation in estimations]
        return make_response(jsonify(estimations_l), 200)
        
    except Exception:
        return make_response(jsonify({'message' : 'Error getting estimations'}), 500)


if __name__ == "__main__":
    app.run(debug=True,host='0.0.0.0' ,port='4000')
