from fastapi import FastAPI, Depends, HTTPException
# from fastapi.exceptions import RequestValidationError
# from fastapi.responses import JSONResponse

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from app.schemas import Request, DataIn, Estimation
from app.models import RequestM, EstimationM
from . import database
from app.kafka_producer import produce, serializer
from app.kafka_consumer import consume
from datetime import datetime
# from app.monitor import start_monitoring_task
from contextlib import asynccontextmanager
import json


# Initialize DB tables
database.init_db()

REQ_TOP='request_topic'
DAT_TOP='data_topic'
EST_TOP='estimation_topic'
LOG_TOP='logging_topic'

TOPICS = {REQ_TOP, DAT_TOP, EST_TOP, LOG_TOP}

tags_metadata = [
    {
        "name": "DataIn",
        "description": "Add new Data in a Synopsis. It sends it through Kafka directly to SDE",
    },
    {
        "name": "Requests",
        "description": "Get all the requests from the db"
    },
    {
        "name": "AddSynopsis",
        "description": "Add a new Synopsis in SDE"
    },
    {
        "name": "estimations",
        "description": "Ask SDE for an Estimation of a specific Synopse"
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


#############
# Endpoints #
#############

###### KAFKA ###########
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
#######################

###### DATA ###########
@app.post("/DataIn/", tags=["DataIn"])
async def produce_message(data: DataIn):
    json_data = data.model_dump()
    await produce('data_topic', json_data)
    return {"status": "Sent Data", "data": json_data}
    
@app.get("/DataIn/", tags=["DataIn"])
async def get_messages():
    data = await consume('data_topic')
    return {"Data in Kafka": data}


#######################


###### REQUESTS #######
@app.post("/requests/add", tags=["AddSynopsis"])
def create_addrequest(req: Request, db: Session = Depends(get_db)):
    if req.noOfP is not None:
        paral = req.noOfP
    else:
        paral = 4
    db_request = RequestM(streamID = req.streamID,
                            synopsisID = req.synopsisID,
                            requestID = 1,
                            dataSetkey = req.dataSetkey,
                            noOfP= paral)
    
    # Parse param List into the DB
    db_request.set_param(req.param)
    db.add(db_request)
    db.commit()
    db.refresh(db_request)
    return {"Sent to request_topic":db_request.toJson()}

@app.post("/requests/delete", tags=["DeleteSynopsis"])
def create_delrequest(req: Request, db: Session = Depends(get_db)):
    if req.noOfP is not None:
        paral = req.noOfP
    else:
        paral = 4
    db_request = RequestM(streamID = req.streamID,
                            synopsisID = req.synopsisID,
                            requestID = 2,
                            dataSetkey = req.dataSetkey,
                            noOfP= paral)
    
    # Parse param List into the DB
    db_request.set_param(req.param)
    db.add(db_request)
    db.commit()
    db.refresh(db_request)
    return {"Sent to request_topic":db_request.toJson()}

@app.post("/requests/update", tags=["UpdateSynopsis"])
def create_updateRequest():
    return {"Oopsie": "not Implemented"}


@app.get("/requests/",tags=["Requests"])
def read_requests(db: Session = Depends(get_db)):
    return db.query(RequestM).all()
#######################

#### ESTIMATIONS ########
@app.post("/estimations/", tags=["estimations"])
def create_estimation(est: Estimation, db: Session = Depends(get_db)):
    #timestamp for last_req
    now = datetime.now()
    age_sec = int(est.age.total_seconds())
    req_body = est.model_dump_json(include={"uid","streamID", "synopsisID","dataSetKey", "param", "requestID", "noOfP"})
    sUID=est.uid
   
    try:
        existing = db.query(EstimationM).filter(EstimationM.uid == sUID).first()

        if existing:
            # update existing request
            existing.last_req = now
            existing.age = age_sec
            db.commit()
            db.refresh(existing)
            return {"status": "Estimation updated", "payload": existing}
        else:
            # add new estimation request
            db_estimation = EstimationM(
                uid=sUID,
                body=req_body,
                age=age_sec,
                last_req=now
            )
            db.add(db_estimation)
            db.commit()
            db.refresh(db_estimation)
            return {"status": "Estimation created", "payload": db_estimation.toJson()}
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    


@app.get("/estimations/", tags=["estimations"])
def read_estimations(db: Session = Depends(get_db)):
    return db.query(EstimationM).all()
#######################
