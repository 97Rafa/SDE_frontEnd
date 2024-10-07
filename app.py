from flask import Flask, request, jsonify, make_response
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.postgresql import JSON
from os import environ
# from config import DB_CONFIG

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = environ.get('DB_URL')
db = SQLAlchemy(app)


class Kafka_Req(db.Model):
    __tablename__ = 'kafka_reqs'

    id = db.Column(db.Integer, primary_key=True)
    status = db.Column(db.String(30), nullable=False)
    req_type = db.Column(db.String(30), nullable=False)
    body = db.Column(JSON, nullable=False)

    def toJson(self):
        return {'id' : self.id, 'status' : self.status, 'req_type' : self.req_type ,'body' : self.body}
    

with app.app_context():
    db.create_all()

#create a test route
@app.route('/test', methods=['GET'])
def test():
    return make_response(jsonify({'message': 'test route'}), 200)

#create a kafka request
@app.route('/kafka_reqs', methods=['POST'])
def create_kafka_req():
    try:
        data = request.get_json()
        new_kafka_req = Kafka_Req(status=data['status'], req_type=data['req_type'], body=data['body'])
        db.session.add(new_kafka_req)
        db.session.commit()
        return make_response(jsonify({'message' : 'Kafka request created'}), 201)
    except Exception:
        return make_response(jsonify({'message' : 'Error creating request'}), 500)

#get all requests
@app.route('/kafka_reqs', methods=['GET'])
def get_kafka_reqs():
    try:
         # Query the database for all items
        kafka_requests = Kafka_Req.query.all()
        # Convert the result to a list of dictionaries
        kafka_requests_l = [kafka_request.toJson() for kafka_request in kafka_requests]
        return make_response(jsonify(kafka_requests_l), 200)
        
    except Exception:
        return make_response(jsonify({'message' : 'Error getting requests'}), 500)

#get a request by id
@app.route('/kafka_reqs/<int:id>', methods=['GET'])
def get_kafka_req(id):
    try:
        kafka_req = Kafka_Req.query.filter_by(id=id).first()
        if kafka_req:
            return make_response(jsonify({'Kafka Request' : kafka_req.toJson()}), 200)
        return make_response(jsonify({'message': 'Request not found'}), 404)
    except Exception:
        return make_response(jsonify({'message' : 'Error getting request'}), 500)
    
#delete a request by id
@app.route('/kafka_reqs/<int:id>', methods=['DELETE'])
def delete_kafka_req(id):
    try:
        kafka_req = Kafka_Req.query.filter_by(id=id).first()
        if kafka_req:
            db.session.delete(kafka_req)
            db.session.commit()
            return make_response(jsonify({'message' : 'Request deleted'}), 200)
        return make_response(jsonify({'message': 'Request not found'}), 404)
    except Exception:
        return make_response(jsonify({'message' : 'Error deleting request'}), 500)

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')

