import random
import uuid
from enum import Enum
from typing import Any, ClassVar

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, model_validator


class synopsis_id_En(Enum):
    countMin = 1
    bloomFilter = 2
    ams = 3


class operation_mode_En(Enum):
    QUERYABLE = "Queryable"
    CONTINUOUS = "Continuous"


class RequestParamSchema(Enum):
    countMin = "countMin"
    bloomFilter = "bloomFilter"
    ams = "ams"


PARAM_SCHEMA: ClassVar[dict[RequestParamSchema, dict[str, Any]]] = {
    RequestParamSchema.countMin: {
        "KeyField": str,
        "ValueField": str,
        "OperationMode": operation_mode_En,
        "epsilon": int,
        "cofidence": int,
        "seed": int,
    },
    RequestParamSchema.bloomFilter: {
        "KeyField": str,
        "ValueField": str,
        "OperationMode": operation_mode_En,
        "numberOfElements": int,
        "FalsePositive": int,
    },
    RequestParamSchema.ams: {
        "KeyField": str,
        "ValueField": str,
        "OperationMode": operation_mode_En,
        "Depth": int,
        "Buckets": int,
    },
}


SYNOPSIS_ID_PARAM: ClassVar[dict[int, RequestParamSchema]] = {
    1: RequestParamSchema.countMin,
    2: RequestParamSchema.bloomFilter,
    3: RequestParamSchema.ams,
}


def generate_uid():
    return random.randint(1000, 9999)


def generate_external_uid():
    return uuid.uuid4().hex


class RequestBase(BaseModel):
    model_config = ConfigDict(extra="ignore", use_enum_values=True)

    @model_validator(mode="before")
    @classmethod
    def strip_server_owned_fields(cls, values):
        if isinstance(values, dict):
            server_owned_fields = (
                "externalUID",
                "external_uid",
                "requestID",
                "noOfP",
            )
            if cls.__name__ == "AddRequest":
                server_owned_fields += ("uid",)
            for key in server_owned_fields:
                values.pop(key, None)
        return values

    streamID: str = Field(
        description="The name of the stream where the request will be asked"
    )
    synopsisID: synopsis_id_En = Field(
        description="Synopsis type(e.g. 1=CountMin, 2=BloomFilter,...)"
    )
    dataSetkey: str = Field(description="Hash Value")

    uid: int | None = Field(default=None, description="Server-generated unique ID")
    externalUID: str | None = Field(
        default=None,
        alias="external_uid",
        validation_alias=AliasChoices("external_uid", "externalUID"),
        description="Server-generated External UID",
    )
    noOfP: int | None = Field(default=None, description="Job parallelism")
    requestID: int | None = Field(default=None)


class AddRequest(RequestBase):
    param: list[str] = Field(
        default_factory=list, description="Parameters of the request"
    )


# class SpecRequest(RequestBase):
#     uid: int = Field(description="4-digit ID")


class EstRequest(RequestBase):
    uid: int = Field(description="4-digit ID")
    param: list[str] = Field(
        default_factory=list, description="Parameters of the request"
    )
    cache_max_age: int | None = Field(
        default=1, description="How 'fresh' should the estimation be(in minutes)"
    )


class DataIn(BaseModel):
    values: dict[str, Any] = Field(
        ..., description="Values to be inserted in the Synopsis"
    )
    streamID: str = Field(
        ..., description="The name of the stream where the request will be asked"
    )
    dataSetkey: str = Field(..., description="Hash Value")
