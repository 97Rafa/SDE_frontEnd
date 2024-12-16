from flask import Flask, request, jsonify, make_response,json
from os import environ
from datetime import timedelta, datetime
from models import db, Requests, Estimations, DataIn, create_tables
from kafkaClient import get_kafka_consumer, get_kafka_producer

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = environ.get('DB_URL')



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

producer = get_kafka_producer(KAFKA_BROKER)

@app.route('/produce/<topic>', methods=['POST'])
def produce_message(topic):
    if topic not in [DAT_TOPIC, REQ_TOPIC]:
        return jsonify({'error': 'Invalid topic'}), 400
    try:
        data = request.json
        if not data:
            return jsonify({'error': 'Empty data'}), 400

        producer.send(topic, value=data)
        producer.flush()
        return jsonify({'status': f'Message sent to topic: {topic}'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/consume/<topic>', methods=['GET'])
def consume_message(topic):
    try:
        consumer = get_kafka_consumer(topic, KAFKA_BROKER)
        messages = []

        # Poll the topic for a few seconds to collect messages
        for message in consumer:
            messages.append(message.value)
            if len(messages) >= 10:  # Limit the number of messages returned
                break

        return jsonify({'messages': messages}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500



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
def get_ests():
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


        new_estimation_req = Estimations(
            body=req_body, 
            synopsisUID=req_uid,
            timeout=timedelta(**parse_interval(timeout)) if timeout else None,  # Convert to timedelta
            last_req=now
            )
        db.session.add(new_estimation_req)
        db.session.commit()
        return make_response(jsonify({'message' : 'Estimation request created'}), 201)
    except Exception:
        return make_response(jsonify({'message' : 'Error asking for estimation'}), 500)

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

