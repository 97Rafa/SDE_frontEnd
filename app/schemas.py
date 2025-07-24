from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
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
    uid: int = Field(description="4-digit ID")
    param: List[str] = Field(default_factory=list, description="Parameters of the request")
    cache_max_age: Optional[int] = Field(default=1, description="How 'fresh' should the estimation be(in minutes)")

class DataIn(BaseModel):
    values: Dict[str, Any] = Field(..., description="Values to be inserted in the Synopsis")
    streamID: str = Field(..., description="The name of the stream where the request will be asked")
    dataSetkey: str = Field(..., description="Hash Value")