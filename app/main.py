from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from app.schemas import RequestBase, AddRequest,DelRequest, DataIn, EstRequest, SYNOPSIS_ID_PARAM
from app.models import EstimationM, get_expiration
from . import database
from app.kafka_producer import produce
from app.kafka_consumer import consume
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from enum import Enum
# from aiokafka import AIOKafkaConsumer
import random, asyncio, json
from asyncio import TimeoutError as AsyncTimeoutError



# Initialize DB tables
database.init_db()

REQ_TOP='request_topic'
DAT_TOP='data_topic'
EST_TOP='estimation_topic'
LOG_TOP='logging_topic'
PARALELISM=4
KAFKA_BROKER= 'kafka:9093'

TOPICS = {REQ_TOP, DAT_TOP, EST_TOP, LOG_TOP}

RESPONSE_TIMEOUT = 5
ESTIMATION_TIMEOUT = 5

tags_metadata = [
    {
        "name": "DataIn",
        "description": "Add new Data in a Synopsis. It sends it through Kafka directly to SDE",
    },
    {
        "name": "AddSynopsis",
        "description": "Add a new Synopsis in SDE"
    },
    {
        "name": "Estimations",
        "description": "Ask SDE for an Estimation of a specific Synopse"
    },
    {
        "name": "DeleteSynopsis",
        "description": "Delete a Synopsis in SDE(Req ID = 2)"
    },
    {
        "name": "CreateSnapshot",
        "description": "Create a Synopsis Snapshot in SDE(Req ID = 100)"
    },
    {
        "name": "ListSnapshots",
        "description": "List Synopsis Snapshots the system(Req ID = 301)"
    },
    {
        "name": "LoadLatest",
        "description": "Load the latest Snapshot of a specific Synopsis(Req ID = 200)"
    },
    {
        "name": "LoadCustom",
        "description": "Load a specific Snapshot of a specific Synopsis(Req ID = 201)"
    },
    {
        "name": "CreateFromSnap",
        "description": "Create a Synopsis from Snapshot in the system(Req ID = 202)"
    },
    {
        "name": "Kafka Direct",
        "description": "Direct Kafka Communication(No DB)"
    },
]

# Dependency
def get_db():
    db = database.SessionLocal()
    try:
        yield db
    finally:
        db.close()


# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     # STARTUP
#     asyncio.create_task(start_monitoring_task(database.SessionLocal))
#     yield
#     # SHUTDOWN (if needed): cleanup resources here


# FastAPI app
app = FastAPI(openapi_tags=tags_metadata,
                title="SDE Front End🚀",
                description="Here will be the Description",
                summary="Service Interface for Synopses Data Engine",
                version="0.0.1")



@app.post("/produce/{topic}", tags=["Kafka Direct"])
async def produce_message(topic: str, msg: dict):
    if topic not in TOPICS:
        raise HTTPException(status_code=400, detail="Invalid topic")
    await produce(topic, msg)
    return {"status": "sent", "topic": topic}
    
@app.get("/consume/{topic}", tags=["Kafka Direct"])
async def get_messages(topic: str):
    if topic not in TOPICS:
        raise HTTPException(status_code=400, detail="Invalid topic")
    data = await consume(topic)
    return {"topic": topic, "messages": data}

@app.post("/dataIn/", tags=["DataIn"])
async def produce_message(data: DataIn):
    json_data = data.model_dump()
    await produce(DAT_TOP, json_data)
    return {"status": "Sent Data", "data": json_data}
    
@app.get("/dataIn/", tags=["DataIn"])
async def get_messages():
    data = await consume(DAT_TOP)
    return {"Data in Kafka": data}



# It consumes the logger topic after a request is sent to SDE to find a response 
async def wait_for_response(externalUID: str):
    timeout = RESPONSE_TIMEOUT
    try:
        log_msgs = await asyncio.wait_for(consume(LOG_TOP), timeout)
    except AsyncTimeoutError:
        raise HTTPException(status_code=504, detail="Timeout waiting for Kafka response")

    for value in log_msgs:
        if value.get("relatedRequestIdentifier") == externalUID:
            return {
                "externalUID": value.get("relatedRequestIdentifier"),
                "timestamp": value.get("timestamp"),
                "requestTypeID": value.get("requestTypeID"),
                "content": value.get("content")
            }

    raise HTTPException(status_code=404, detail="Matching response not found")


@app.post("/requests/add", tags=["AddSynopsis"])
async def create_addrequest(request: AddRequest):
    synopsis_id = request.synopsisID
    param_list = request.param
    request.requestID = 1

    if synopsis_id not in SYNOPSIS_ID_PARAM:
        raise HTTPException(status_code=400, detail="Invalid synopsisID")
    # matches synopsisID with the expected parameters
    schema = SYNOPSIS_ID_PARAM[synopsis_id].value

    if len(param_list) != len(schema):
        raise HTTPException(
            status_code=422,
            detail=f"Expected {len(schema)} parameters: {list(schema.keys())}"
        )

    # Validate params depending on synopsisID
    for (name, expected_type), value in zip(schema.items(), param_list):
        try:
            if isinstance(expected_type, type) and issubclass(expected_type, Enum):
                expected_type(value)
            else:
                expected_type(value)
        except Exception as e:
            raise HTTPException(
                status_code=422,
                detail=f"Invalid value for '{name}': expected {expected_type.__name__}, got '{value}'"
            )
    
    json_request = request.model_dump()
    await produce(REQ_TOP, json_request)
    # Wait for matching response
    try:
        response = await wait_for_response(request.externalUID)
        return {"response": response}
    except asyncio.TimeoutError:
        return {"error": "Timeout waiting for response"}

    

@app.post("/requests/delete", tags=["DeleteSynopsis"])
async def create_delrequest(request: DelRequest):
    request.requestID = 2    
    json_request = request.model_dump()
    await produce(REQ_TOP, json_request)
    # Wait for matching response
    try:
        response = await wait_for_response(request.externalUID)
        return {"response": response}
    except asyncio.TimeoutError:
        return {"error": "Timeout waiting for response"}


@app.post("/requests/createSnapshot", tags=["CreateSnapshot"])
async def create_snapshot(request: RequestBase):
    request.requestID = 100    
    json_request = request.model_dump()
    await produce(REQ_TOP, json_request)
    # Wait for matching response
    try:
        response = await wait_for_response(request.externalUID)
        return {"response": response}
    except asyncio.TimeoutError:
        return {"error": "Timeout waiting for response"}

@app.post("/requests/listSnapshots", tags=["ListSnapshots"])
async def list_snapshots(request: RequestBase):
    request.requestID = 301    
    json_request = request.model_dump()
    await produce(REQ_TOP, json_request)
    # Wait for matching response
    try:
        response = await wait_for_response(request.externalUID)
        return {"response": response}
    except asyncio.TimeoutError:
        return {"error": "Timeout waiting for response"}

@app.post("/requests/loadLatest", tags=["LoadLatest"])
async def load_latest(request: RequestBase):
    request.requestID = 200    
    json_request = request.model_dump()
    await produce(REQ_TOP, json_request)
    # Wait for matching response
    try:
        response = await wait_for_response(request.externalUID)
        return {"response": response}
    except asyncio.TimeoutError:
        return {"error": "Timeout waiting for response"}

@app.post("/requests/loadCustom", tags=["LoadCustom"])
async def load_custom(request: RequestBase):
    request.requestID = 201    
    json_request = request.model_dump()
    await produce(REQ_TOP, json_request)
    # Wait for matching response
    try:
        response = await wait_for_response(request.externalUID)
        return {"response": response}
    except asyncio.TimeoutError:
        return {"error": "Timeout waiting for response"}

@app.post("/requests/createFromSnap", tags=["CreateFromSnap"])
async def create_fromSnap(request: AddRequest,version_number: int = 0, new_uid: int = random.randint(90000, 100000)):
    request.requestID = 202 
    request.param = [version_number, new_uid]
    json_request = request.model_dump()
    await produce(REQ_TOP, json_request)
    # Wait for matching response
    try:
        response = await wait_for_response(request.externalUID)
        return {"response": response}
    except asyncio.TimeoutError:
        return {"error": "Timeout waiting for response"}
    

#It waits for Kafka estimation topic to give an estimation to the corresponding synopsis
async def wait_for_estimation(uid: int):
    timeout = ESTIMATION_TIMEOUT
    try:
        est_msgs = await asyncio.wait_for(consume(EST_TOP), timeout)
    except AsyncTimeoutError:
        raise HTTPException(status_code=504, detail="Timeout waiting for Kafka response")

    for value in est_msgs:
        if value.get("uid") == uid:
            return {
	            "uid": value.get("uid"),
                "synopsisID": value.get("synopsisID"),
	            "param": value.get("param"),
                "estimation": value.get("estimation")
            }

    raise HTTPException(status_code=404, detail="Matching estimation not found")

# It compares the time that has passed since the last data where added on the table
# with the max age of the estimation the request specifies. If max age is longer
# the old estimation from the table can still be used
def should_use_cached(estimation: EstimationM, max_age_minutes: int) -> bool:
    if estimation.last_data is None:
        return False
    return datetime.now() - estimation.last_data < timedelta(minutes=max_age_minutes)


@app.post("/estimations/", tags=["Estimations"])
async def create_estimation(request: EstRequest, db: Session = Depends(get_db)):
    request.requestID = 3
    #timestamp for last_req
    now = datetime.now()
    req_body = request.model_dump(include={"uid","streamID", "synopsisID","dataSetkey", "param", "requestID", "noOfP"})
    sUID=request.uid
   
    try:
        # check if the same estimation request has been sent before
        existing = db.query(EstimationM).filter(EstimationM.uid == sUID).first()

        if existing:
            # update existing request timestamp
            existing.last_req = now        
            db.commit()
            db.refresh(existing)

            use_cached = should_use_cached(existing, request.cache_max_age)
            if use_cached:
                return {"status": "Estimation updated", "Estimation": existing.fetchedEst, 
                        "Cached" : use_cached
                        }
            else:
                await produce(REQ_TOP, existing.body) #send request to Kafka
                # Wait for matching estimation
                estimation = await wait_for_estimation(request.uid)
                # Update the existing estimation with the new one and its timestamp
                existing.fetchedEst = estimation
                existing.last_data = datetime.now()
                db.commit()
                db.refresh(existing)
                return {"status": "Estimation updated", "Estimation": estimation, 
                        "Cached" : use_cached
                        }
        else:
            try:
                await produce(REQ_TOP, req_body) #send request to Kafka
                # Wait for matching estimation
                estimation = await wait_for_estimation(request.uid)
                # add new estimation request on DB
                db_estimation = EstimationM(
                    uid=sUID,
                    body=req_body,
                    last_req=now,
                    last_data=now,
                    fetchedEst=estimation
                )
                db.add(db_estimation)
                db.commit()
                db.refresh(db_estimation)

                return {"status": "Estimation request created", "Estimation": estimation}
            except asyncio.TimeoutError:
                return {"error": "Timeout waiting for estimation"}

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    


@app.get("/estimations/", tags=["Estimations"])
def read_estimations(db: Session = Depends(get_db)):
    return db.query(EstimationM).all()