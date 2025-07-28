from sqlalchemy import Column, Integer, DateTime, Boolean, String
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


def get_expiration(minutes: int = 1) -> datetime:
    return datetime.now() + timedelta(minutes=minutes)

class Synopsis(Base):
    __tablename__ = 'synopsis'
    uid = Column(Integer, primary_key=True, nullable=False, unique=True)
    createdAt = Column(DateTime(timezone=True), nullable=False)
    details = Column(String, nullable=False)

class EstimationM(Base):
    __tablename__ = 'estimations'
    uid = Column(Integer, primary_key=True, nullable=False, unique=True)
    body = Column(JsonType, nullable=True)
    fetchedEst = Column(JsonType, nullable=True)
    last_req = Column(DateTime(timezone=True), nullable=True)
    last_data = Column(DateTime(timezone=True), nullable=True)

    def toJson(self):
        return {'uid': self.uid, 'age': self.age,
                'body': self.body, 'fetchedEst': self.fetchedEst, 'last_req': self.last_req, 'last_data': self.last_data}
