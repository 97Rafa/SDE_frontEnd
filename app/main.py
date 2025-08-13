from fastapi import FastAPI, Depends, HTTPException, UploadFile, File, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from app.schemas import RequestBase, AddRequest,SpecRequest, DataIn, EstRequest, SYNOPSIS_ID_PARAM
from app.models import EstimationM, Synopsis
from . import database
from typing import List
from app.kafka_producer import produce
from app.kafka_consumer import consume
from datetime import datetime, timedelta
from pathlib import Path
from enum import Enum
import random, asyncio,time
import csv



# Initialize DB tables
database.init_db()

REQ_TOP='request_topic'
DAT_TOP='data_topic'
EST_TOP='estimation_topic'
LOG_TOP='logging_topic'
PARALELISM=4
KAFKA_BROKER= 'kafka1:9092'

TOPICS = {REQ_TOP, DAT_TOP, EST_TOP, LOG_TOP}

RESPONSE_TIMEOUT = 8
ESTIMATION_TIMEOUT = 5

tags_metadata = [
    {
        "name": "DataIn",
        "description": "Add new Data in a Synopsis. It sends it through Kafka directly to SDE",
    },
    {
        "name": "Add Synopsis",
        "description": "Add a new Synopsis in SDE"
    },
    {
        "name": "Estimations",
        "description": "Ask SDE for an Estimation of a specific Synopse"
    },
    {
        "name": "Delete Synopsis",
        "description": "Delete a Synopsis in SDE(Req ID = 2)"
    },
    {
        "name": "Create Snapshot",
        "description": "Create a Synopsis Snapshot in SDE(Req ID = 100)"
    },
    {
        "name": "List Snapshots",
        "description": "List Synopsis Snapshots the system(Req ID = 301)"
    },
    {
        "name": "Load Latest Snapshot",
        "description": "Load the latest Snapshot of a specific Synopsis(Req ID = 200)"
    },
    {
        "name": "Load Custom Snapshot",
        "description": "Load a specific Snapshot of a specific Synopsis(Req ID = 201)"
    },
    {
        "name": "Create Synopsis from Snapshot",
        "description": "Create a Synopsis from Snapshot in the system(Req ID = 202)"
    },
    {
        "name": "Kafka Direct",
        "description": "Direct Kafka Communication(No DB)"
    },
    {
        "name": "Initiate SM",
        "description": "Update Storage Manager Credentials(Req ID = 101)"
    },
    {
        "name": "List Synopsis",
        "description": "List the synopsis in the system right now"
    },
]

# Dependency
def get_db():
    db = database.SessionLocal()
    try:
        yield db
    finally:
        db.close()



# FastAPI app
app = FastAPI(openapi_tags=tags_metadata,
                title="SDE Front End🚀",
                description="Here will be the Description",
                summary="Service Interface for Synopses Data Engine",
                version="0.0.1")

@app.post("/produce/{topic}", tags=["Kafka Direct"])
async def produce_message(topic: str, msg: dict):
    if topic not in TOPICS:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid topic")
    await produce(topic, msg)
    return {"status": "sent", "topic": topic}
    
@app.get("/consume/{topic}", tags=["Kafka Direct"])
async def get_messages(topic: str):
    if topic not in TOPICS:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid topic")
    data = await consume(topic)
    return {"topic": topic, "messages": data}

@app.post("/dataIn/", tags=["DataIn"])
async def produce_data(data: DataIn):
    json_data = data.model_dump()
    await produce(DAT_TOP, json_data)
    return {"status": "Sent Data", "data": json_data}
    
@app.get("/dataIn/", tags=["DataIn"])
async def get_data():
    data = await consume(DAT_TOP)
    return {"Data in Kafka": data}


UPLOAD_FOLDER = Path("uploads")
UPLOAD_FOLDER.mkdir(parents=True, exist_ok=True)

@app.post("/dataIn/csv", tags=["DataIn"])
async def create_datain_csv(file: UploadFile = File(...)):
    # Check if file is csv
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Uploaded file must be a CSV."
        )

    file_path = UPLOAD_FOLDER / file.filename
    try:
        # Save file
        with open(file_path, "wb") as buffer:
            buffer.write(await file.read())

        # Parse CSV
        with open(file_path, newline="", encoding="utf-8") as csvfile:
            csvreader = csv.DictReader(csvfile)
            required_columns = {"StreamID", "dataSetkey"}

            if not required_columns.issubset(csvreader.fieldnames or []):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"CSV must contain columns: {', '.join(required_columns)}"
                )
            records = 0
            for i, row in enumerate(csvreader, start=1):
                try:
                    data_in = DataIn(
                        streamID=row["StreamID"],
                        dataSetkey=row["dataSetkey"],
                        values={k: v for k, v in row.items() if k not in ["StreamID", "dataSetkey"]}
                    )
                    await produce(DAT_TOP, data_in.model_dump())
                    records += 1
                except Exception as e:
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                        detail=f"Row {i} failed: {str(e)}"                    )
    
        return {"status":status.HTTP_200_OK, "message": f"Sent {records} messages to '{DAT_TOP}'"}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the CSV."
        )


# # It consumes the logger topic after a request is sent to SDE to find a response 
async def wait_for_response(externalUID: str)-> dict:
    timeout = RESPONSE_TIMEOUT
    start_time = time.monotonic()

    while True:
        remaining = timeout - (time.monotonic() - start_time)
        if remaining <= 0:
            raise HTTPException(status_code=status.HTTP_504_GATEWAY_TIMEOUT, detail="Timeout waiting for Kafka response")

        try:
            log_msgs = await asyncio.wait_for(consume(LOG_TOP), timeout=remaining)
            if not log_msgs:
                await asyncio.sleep(0.1)
                continue

            for value in log_msgs:
                if value.get("relatedRequestIdentifier") == externalUID:
                    return {
                        "externalUID": value.get("relatedRequestIdentifier"),
                        "timestamp": value.get("timestamp"),
                        "requestTypeID": value.get("requestTypeID"),
                        "content": value.get("content")
                    }

        except asyncio.TimeoutError:
            continue

        await asyncio.sleep(0.1)  # prevent tight loop


@app.post("/requests/storeInit", tags=["Initiate SM"])
async def smanager_init():
    json_request = {
                        "externalUID": "1000",
                        "uid":1,
                        "requestID": 101,
                        "dataSetkey": "Forex",
                        "noOfP": 4,
                        "param": [
                            "aws"
                        ]
                    }
    await produce(REQ_TOP, json_request)
    # Wait for matching response
    try:
        response = await wait_for_response("1000")
        return {"response": response}
    except HTTPException as e:
        return {"Error": e.status_code, "Detail" : e.detail}

@app.post("/requests/add", tags=["Add Synopsis"])
async def create_addrequest(request: AddRequest, db: Session = Depends(get_db)):
    synopsis_id = request.synopsisID
    param_list = request.param
    request.requestID = 1

    if synopsis_id not in SYNOPSIS_ID_PARAM:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid synopsisID")
    # matches synopsisID with the expected parameters
    schema = SYNOPSIS_ID_PARAM[synopsis_id].value

    if len(param_list) != len(schema):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
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
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Invalid value for '{name}': expected {expected_type.__name__}, got '{value}'"
            )
    
    json_request = request.model_dump()
    await produce(REQ_TOP, json_request)
    # Wait for matching response
    try:
        response = await wait_for_response(request.externalUID)
        content=response.get('content')
        timestamp = response.get('timestamp')
        synopsis = Synopsis(
                    uid=request.uid,
                    createdAt=datetime.strptime(timestamp, "%d-%m-%Y %H:%M:%S"),
                    details=content[0]
                    )
        db.add(synopsis)
        db.commit()
        db.refresh(synopsis)
        return {"response": response}
    except HTTPException as e:
        return {"Error": e.status_code, "Detail" : e.detail}
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    

@app.post("/requests/delete", tags=["Delete Synopsis"])
async def create_delrequest(request: SpecRequest, db: Session = Depends(get_db)):
    request.requestID = 2
    # Look on Synopsis table to delete it from there too
    synopse_to_delete = db.query(Synopsis).filter(Synopsis.uid == request.uid).first()
    if synopse_to_delete is not None:
        db.delete(synopse_to_delete)
        db.commit()
    json_request = request.model_dump()
    await produce(REQ_TOP, json_request)
    # Wait for matching response
    try:
        response = await wait_for_response(request.externalUID)
        return {"response": response}
    except HTTPException as e:
        return {"Error": e.status_code, "Detail" : e.detail}


@app.post("/requests/createSnapshot", tags=["Create Snapshot"])
async def create_snapshot(request: SpecRequest):
    request.requestID = 100    
    json_request = request.model_dump()
    await produce(REQ_TOP, json_request)
    # Wait for matching response
    try:
        response = await wait_for_response(request.externalUID)
        return {"response": response}
    except HTTPException as e:
        return {"Error": e.status_code, "Detail" : e.detail}

@app.post("/requests/listSnapshots", tags=["List Snapshots"])
async def list_snapshots(request: SpecRequest):
    request.requestID = 301    
    json_request = request.model_dump()
    await produce(REQ_TOP, json_request)
    # Wait for matching response
    try:
        response = await wait_for_response(request.externalUID)
        return {"response": response}
    except HTTPException as e:
        return {"Error": e.status_code, "Detail" : e.detail}

@app.post("/requests/loadLatest", tags=["Load Latest Snapshot"])
async def load_latest(request: SpecRequest):
    request.requestID = 200    
    json_request = request.model_dump()
    await produce(REQ_TOP, json_request)
    # Wait for matching response
    try:
        response = await wait_for_response(request.externalUID)
        return {"response": response}
    except HTTPException as e:
        return {"Error": e.status_code, "Detail" : e.detail}

@app.post("/requests/loadCustom", tags=["Load Custom Snapshot"])
async def load_custom(request: SpecRequest):
    request.requestID = 201    
    json_request = request.model_dump()
    await produce(REQ_TOP, json_request)
    # Wait for matching response
    try:
        response = await wait_for_response(request.externalUID)
        return {"response": response}
    except HTTPException as e:
        return {"Error": e.status_code, "Detail" : e.detail}

@app.post("/requests/createFromSnap", tags=["Create Synopsis from Snapshot"])
async def create_fromSnap(request: AddRequest,version_number: int = 0, new_uid: int = random.randint(90000, 100000), db: Session = Depends(get_db)):
    request.requestID = 202 
    request.param = [version_number, new_uid]
    json_request = request.model_dump()
    await produce(REQ_TOP, json_request)
    # Wait for matching response
    try:
        response = await wait_for_response(request.externalUID)
        content=response.get('content')
        timestamp = response.get('timestamp')
        synopsis = Synopsis(
                    uid=request.uid,
                    createdAt=datetime.strptime(timestamp, "%d-%m-%Y %H:%M:%S"),
                    details=content[0]
                    )
        db.add(synopsis)
        db.commit()
        db.refresh(synopsis)
        return {"response": response}
    except HTTPException as e:
        return {"Error": e.status_code, "Detail" : e.detail}
    

#It waits for Kafka estimation topic to give an estimation to the corresponding synopsis
async def wait_for_estimation(uid: int) -> dict:
    timeout = RESPONSE_TIMEOUT
    start_time = time.monotonic()

    while True:
        remaining = timeout - (time.monotonic() - start_time)
        if remaining <= 0:
            raise HTTPException(status_code=status.HTTP_504_GATEWAY_TIMEOUT, detail="Timeout waiting for Kafka response")

        try:
            est_msgs = await asyncio.wait_for(consume(EST_TOP), timeout)
            if not est_msgs:
                await asyncio.sleep(0.1)
                continue

            for value in est_msgs:
                if value.get("uid") == uid:
                    return {
                        "uid": value.get("uid"),
                        "synopsisID": value.get("synopsisID"),
                        "param": value.get("param"),
                        "estimation": value.get("estimation")
                    }

        except asyncio.TimeoutError:
            continue

        await asyncio.sleep(0.1)  # prevent tight loop

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
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    


@app.get("/estimations/", tags=["Estimations"])
def read_estimations(db: Session = Depends(get_db)):
    return db.query(EstimationM).all()

@app.get("/synopsis/", tags=["List Synopsis"])
def list_synopsis(db: Session = Depends(get_db)):
    return db.query(Synopsis).all()