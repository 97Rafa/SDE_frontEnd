from fastapi import FastAPI, Depends, HTTPException
# from fastapi.exceptions import RequestValidationError
# from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from .schemas import Request, DataIn, Estimation
from .models import RequestM, EstimationM
from . import database
from .kafka_producer import send_message
from datetime import datetime
from typing import List

# Initialize DB tables
database.init_db()

tags_metadata = [
    {
        "name": "dataIn",
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
        "name": "AddRandSynopsis",
        "description": "Add a new Synopsis in SDE with Random Partitioning"
    },
    {
        "name": "AddContSynopsis",
        "description": "Add a new Continuous Synopis in SDE"
    },
    {
        "name": "UpdateSynopsis",
        "description": "Update a Synopsis state"
    },
    {
        "name": "estimations",
        "description": "Ask SDE for an Estimation of a specific Synopse"
    },
]


# FastAPI app
app = FastAPI(openapi_tags=tags_metadata,
                title="SDE Front End🚀",
                description="Here will be the Description",
                summary="Service Interface for Synopses Data Engine",
                version="0.0.1")

# Dependency
def get_db():
    db = database.SessionLocal()
    try:
        yield db
    finally:
        db.close()


#############
# Endpoints #
#############

######  DATA ##########
@app.post("/dataIn/", tags=["dataIn"])
async def send_data(message: DataIn):
    await send_message("data_topic", message.model_dump())
    return {"status": "Message sent", "payload": message.model_dump()}
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
    return db_request.toJson()

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
    return db_request.toJson()

@app.post("/requests/addRand", tags=["AddRandSynopsis"])
def create_addRandrequest(req: Request, db: Session = Depends(get_db)):
    if req.noOfP is not None:
        paral = req.noOfP
    else:
        paral = 4
    db_request = RequestM(streamID = req.streamID,
                            synopsisID = req.synopsisID,
                            requestID = 4,
                            dataSetkey = req.dataSetkey,
                            noOfP= paral)
    
    # Parse param List into the DB
    db_request.set_param(req.param)
    db.add(db_request)
    db.commit()
    db.refresh(db_request)
    return db_request.toJson()

@app.post("/requests/addCont", tags=["AddContSynopsis"])
def create_addContrequest(req: Request, db: Session = Depends(get_db)):
    if req.noOfP is not None:
        paral = req.noOfP
    else:
        paral = 4
    db_request = RequestM(streamID = req.streamID,
                            synopsisID = req.synopsisID,
                            requestID = 5,
                            dataSetkey = req.dataSetkey,
                            noOfP= paral)
    
    # Parse param List into the DB
    db_request.set_param(req.param)
    db.add(db_request)
    db.commit()
    db.refresh(db_request)
    return db_request.toJson()

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
    body_data = jsonable_encoder(est.body)
    age_sec = int(est.age.total_seconds())
    sUID=body_data.get('uid')
    try:
        existing = db.query(EstimationM).filter(EstimationM.synopsisUID == sUID).first()

        if existing:
            # update existing request
            existing.last_req = now
            existing.age = age_sec
            db.commit()
            db.refresh(existing)
            return {"status": "Estimation updated", "payload": body_data}
        else:
            # add new estimation request
            db_estimation = EstimationM(
                body=body_data,
                synopsisUID=sUID,
                age=age_sec,
                last_req=now
            )
            db.add(db_estimation)
            db.commit()
            db.refresh(db_estimation)
            return {"status": "Estimation created", "payload": body_data}
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    


@app.get("/estimations/", tags=["estimations"])
def read_estimations(db: Session = Depends(get_db)):
    return db.query(EstimationM).all()
#######################
