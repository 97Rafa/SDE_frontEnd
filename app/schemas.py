from pydantic import BaseModel, Field, field_validator
from typing import Optional, Dict, Any, List, Literal
from datetime import timedelta

class Request(BaseModel):
    streamID: str = Field(..., description="The name of the stream where the request will be asked")
    synopsisID: int = Field(...,ge=1, le=31, description="Synopsis type(e.g. 1=CountMin, 2=BloomFilter,...)")
    dataSetkey: str = Field(..., description="Hash Value")
    param: List[str] = Field(default_factory=list, description="Parameters of the request")
    noOfP: Optional[int] = Field(default=4, description="Job parallelism")

class RequestRead(BaseModel):
    uid: int = Field(description="Unique ID of the Request")
    body: Request

class DataIn(BaseModel):
    values: Dict[str, Any]
    streamID: str
    dataSetkey: str

class Estimation(BaseModel):
    synopsisUID : int = None
    age : timedelta
    body : RequestRead

    @field_validator('age', mode='before')
    def validate_duration(cls, value):
        if isinstance(value, timedelta):
            return value
        try:
            parts = [int(p) for p in value.split(":")]
            if len(parts) == 3:
                h, m, s = parts
            elif len(parts) == 2:
                h, m, s = 0, *parts
            elif len(parts) == 1:
                h, m, s = 0, 0, parts[0]
            else:
                raise ValueError
            return timedelta(hours=h, minutes=m, seconds=s)
        except Exception:
            raise ValueError("Duration must be in format HH:MM:SS or MM:SS")