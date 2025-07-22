from pydantic import BaseModel, Field, field_validator
from typing import Optional, Dict, Any, List, Literal
from datetime import timedelta
from enum import Enum
import uuid, random

class synopsisEn(Enum):
    countMin = 1
    bloomFilter = 2
    ams = 3

class operationEn(Enum):
    QUERYABLE = "Queryable"
    CONTINUOUS = "Continuous"

class SynopsisParamSchema(Enum):
    countMin = {
        "KeyField" : str, 
        "ValueField" : str,
        "OperationMode" : operationEn, 
        "epsilon" : int, 
        "cofidence" : int, 
        "seed" : int
    }

    bloomFilter = {
        "KeyField" : str, 
        "ValueField" : str,
        "OperationMode" : operationEn, 
        "numberOfElements": int, 
        "FalsePositive": int
    }
    ams = {
        "KeyField" : str, 
        "ValueField" : str,
        "OperationMode" : operationEn, 
        "Depth": int, 
        "Buckets": int
    }


SYNOPSIS_ID_PARAM = {
    1: SynopsisParamSchema.countMin,
    2: SynopsisParamSchema.bloomFilter,
    3: SynopsisParamSchema.ams
}

def generate_uid():
    return random.randint(1000, 9999)

class RequestBase(BaseModel):
    externalUID: Optional[str] = Field(default_factory=lambda: uuid.uuid4().hex, description="External UID")
    uid: Optional[int] = Field(default_factory=generate_uid, description="Random 4-digit ID")
    streamID: str = Field(description="The name of the stream where the request will be asked")
    synopsisID: synopsisEn = Field(description="Synopsis type(e.g. 1=CountMin, 2=BloomFilter,...)")
    dataSetkey: str = Field(description="Hash Value")
    noOfP: Optional[int] = Field(default=4, description="Job parallelism")
    requestID: Optional[int] = Field(default=None)

    class Config:
        use_enum_values = True 

class AddRequest(RequestBase):
    param: List[str] = Field(default_factory=list, description="Parameters of the request")

class DelRequest(RequestBase):
    uid: int = Field(description="4-digit ID")

class EstRequest(RequestBase):
    param: List[str] = Field(default_factory=list, description="Parameters of the request")
    age: timedelta = Field(default="00:01", description="How fresh the estimation will be")

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

class DataIn(BaseModel):
    values: Dict[str, Any] = Field(..., description="Values to be inserted in the Synopsis")
    streamID: str = Field(..., description="The name of the stream where the request will be asked")
    dataSetkey: str = Field(..., description="Hash Value")

# class Estimation(BaseModel):
#     uid : int = Field(description="Unique ID of the Estimation")
#     age: timedelta = Field(default="00:01", description="How fresh the estimation will be")
#     streamID: str = Field(..., description="The name of the stream where the request will be asked")
#     synopsisID: int = Field(...,ge=1, le=31, description="Synopsis type(e.g. 1=CountMin, 2=BloomFilter,...)")
#     dataSetkey: str = Field(..., description="Hash Value")
#     requestID: Optional[Literal[3,6]] = Field(default=3, description="Estimation Type(Default=3)")
#     param: List[str] = Field(default_factory=list, description="Parameters of the request")
#     noOfP: Optional[int] = Field(default=4, description="Job parallelism")

