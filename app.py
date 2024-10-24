from flask import Flask, request, jsonify, make_response
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.postgresql import JSON
from os import environ
from datetime import timedelta
from models import db, Requests, Estimations, create_tables

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = environ.get('DB_URL')

# Initialize the database
db.init_app(app)

# Create tables before starting the app
create_tables(app)


#create a test route
@app.route('/test', methods=['GET'])
def test():
    return make_response(jsonify({'message': 'test route'}), 200)

# =============================================
# ========== Requests table ===================
# =============================================


#create a kafka request
@app.route('/requests', methods=['POST'])
def create_kafka_req():
    try:
        data = request.get_json()
        new_kafka_req = Requests(
            req_type=data['req_type'],
            body=data['body']
            )
        db.session.add(new_kafka_req)
        db.session.commit()
        return make_response(jsonify({'message' : 'Kafka request created'}), 201)
    except Exception:
        return make_response(jsonify({'message' : 'Error creating request'}), 500)

#get all requests
@app.route('/requests', methods=['GET'])
def get_kafka_reqs():
    try:
         # Query the database for all items
        kafka_requests = Requests.query.all()
        # Convert the result to a list of dictionaries
        kafka_requests_l = [kafka_request.toJson() for kafka_request in kafka_requests]
        return make_response(jsonify(kafka_requests_l), 200)
        
    except Exception:
        return make_response(jsonify({'message' : 'Error getting requests'}), 500)

#get a request by id
@app.route('/requests/<int:id>', methods=['GET'])
def get_kafka_req(id):
    try:
        kafka_req = Requests.query.filter_by(id=id).first()
        if kafka_req:
            return make_response(jsonify({'Kafka Request' : kafka_req.toJson()}), 200)
        return make_response(jsonify({'message': 'Request not found'}), 404)
    except Exception:
        return make_response(jsonify({'message' : 'Error getting request'}), 500)
    
#delete a request by id
@app.route('/requests/<int:id>', methods=['DELETE'])
def delete_kafka_req(id):
    try:
        kafka_req = Requests.query.filter_by(id=id).first()
        if kafka_req:
            db.session.delete(kafka_req)
            db.session.commit()
            return make_response(jsonify({'message' : 'Request deleted'}), 200)
        return make_response(jsonify({'message': 'Request not found'}), 404)
    except Exception:
        return make_response(jsonify({'message' : 'Error deleting request'}), 500)

# =============================================
# ========== Estimations table ================
# =============================================
def parse_interval(interval_str):
    # This function converts a string like '5 minutes' into arguments for timedelta
    time_units = {'seconds': 0, 'minutes': 0}

    if 'minute' in interval_str:
        time_units['minutes'] = int(interval_str.split()[0])

    return time_units

#get all estimations
@app.route('/estimations', methods=['POST'])
def get_ests():
    try:
        data = request.get_json()

        # Parse the duration from the JSON if it's provided
        timeout = None
        if 'timeout' in data:
            timeout = data['timeout']  # This will be a string, e.g., '5 minutes'

        new_estimation_req = Estimations(
            synopsisID=data['synopsisID'], 
            timeout=timedelta(**parse_interval(timeout)) if timeout else None  # Convert to timedelta
            )
        db.session.add(new_estimation_req)
        db.session.commit()
        return make_response(jsonify({'message' : 'Estimation request created'}), 201)
    except Exception:
        return make_response(jsonify({'message' : 'Error asking for estimation'}), 500)


if __name__ == "__main__":
    app.run(debug=True,host='0.0.0.0' ,port='4000')

