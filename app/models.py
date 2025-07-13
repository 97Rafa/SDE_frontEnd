from sqlalchemy import Column, Integer, DateTime, Boolean
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

# class RequestM(Base):
#     __tablename__ = 'requests'

#     uid = Column(Integer, primary_key=True, index=True, autoincrement=True)
#     streamID = Column(String(30), nullable=False)
#     synopsisID = Column(Integer, nullable=False)
#     requestID = Column(Integer, nullable=False)
#     dataSetkey = Column(String(30), nullable=False)
#     param = Column(String)
#     noOfP = Column(Integer, nullable=False)

#     def get_param(self):
#         if not self.param:
#             return []
#         try:
#             return json.loads(self.param)
#         except (json.JSONDecodeError, TypeError):
#             return []

#     def set_param(self, value):
#         self.param = json.dumps(value)

#     def toJson(self):
#         return {'uid': self.uid, 'streamID': self.streamID, 'synopsisID': self.synopsisID,
#                 'requestID': self.requestID, 'dataSetkey': self.dataSetkey, 'param': self.get_param(), 'noOfP': self.noOfP}

class EstimationM(Base):
    __tablename__ = 'estimations'
    uid = Column(Integer, primary_key=True, nullable=False, unique=True)
    age = Column(Integer, nullable=False)
    body = Column(JsonType, nullable=True)
    fetchedEst = Column(JsonType, nullable=True)
    last_req = Column(DateTime(timezone=True), nullable=True)
    last_data = Column(DateTime(timezone=True), nullable=True)
    toRefresh = Column(Boolean, nullable=False, default=False)

    def toJson(self):
        return {'uid': self.uid, 'age': self.age,
                'body': self.body, 'fetchedEst': self.fetchedEst, 'last_req': self.last_req, 'last_data': self.last_data, 'toRefresh': self.toRefresh}
