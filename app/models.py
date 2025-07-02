from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import TypeDecorator, TEXT
import json
from datetime import datetime, timedelta

Base = declarative_base()

class JsonType(TypeDecorator):
    """Stores dicts as JSON-encoded strings in TEXT."""
    impl = TEXT

    def process_bind_param(self, value, dialect):
        return json.dumps(value) if value is not None else None

    def process_result_value(self, value, dialect):
        return json.loads(value) if value is not None else None

class RequestM(Base):
    __tablename__ = 'requests'

    uid = Column(Integer, primary_key=True, index=True, autoincrement=True)
    streamID = Column(String(30), nullable=False)
    synopsisID = Column(Integer, nullable=False)
    requestID = Column(Integer, nullable=False)
    dataSetkey = Column(String(30), nullable=False)
    param = Column(Text)
    noOfP = Column(Integer, nullable=False)

    def get_param(self):
        return json.loads(self.param) if self.param else []

    def set_param(self, value):
        self.param = json.dumps(value)

    def toJson(self):
        return {'uid': self.uid, 'status': self.status, 'streamID': self.streamID, 'synopsisID': self.synopsisID,
                'requestID': self.requestID, 'dataSetkey': self.dataSetkey, 'param': self.param, 'noOfP': self.noOfP}

class EstimationM(Base):
    __tablename__ = 'estimations'

    id = Column(Integer, primary_key=True)
    synopsisUID = Column(Integer, nullable=False, unique=True)
    age = Column(Integer, nullable=False)
    body = Column(JsonType, nullable=False)
    fetchedEst = Column(JsonType, nullable=True)
    last_req = Column(DateTime(timezone=True), nullable=True)
    last_data = Column(DateTime(timezone=True), nullable=True)
    toRefresh = Column(Boolean, nullable=False, default=False)

    def toJson(self):
        return {'id': self.id, 'synopsisID': self.synopsisID, 
                'body': self.body, 'last_req': self.last_req, 'last_data': self.last_data, 'toRefresh': self.toRefresh}
