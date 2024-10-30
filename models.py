# models.py
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.postgresql import JSON, INTERVAL

db = SQLAlchemy()

class Requests(db.Model):
    __tablename__ = 'requests'

    id = db.Column(db.Integer, primary_key=True)
    status = db.Column(db.String(30), nullable=False, default='pending')
    body = db.Column(JSON, nullable=False)

    def toJson(self):
        return {'id': self.id, 'status': self.status, 'body': self.body}

class DataIn(db.Model):
    __tablename__ = 'datain'

    id = db.Column(db.Integer, primary_key=True)
    status = db.Column(db.String(30), nullable=False, default='pending')
    body = db.Column(JSON, nullable=False)

    def toJson(self):
        return {'id': self.id, 'status': self.status, 'body': self.body}


class Estimations(db.Model):
    __tablename__ = 'estimations'

    id = db.Column(db.Integer, primary_key=True)
    synopsisID = db.Column(db.Integer, nullable=False)
    timeout = db.Column(INTERVAL, nullable=False)
    body = db.Column(JSON, nullable=True)
    last_req = db.Column(db.DateTime(timezone=True), nullable=True)
    last_data = db.Column(db.DateTime(timezone=True), nullable=True)
    toRefresh = db.Column(db.Boolean, nullable=False, default=True)

    def toJson(self):
        return {'id': self.id, 'synpopsisID': self.synopsisID, 
                'body': self.body, 'last_req': self.last_req, 'last_data': self.last_data, 'toRefresh': self.toRefresh}

def create_tables(app):
    """Function to create all tables"""
    with app.app_context():
        db.create_all()
