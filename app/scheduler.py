from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
from .database import SessionLocal
from .models import EstimationM

def check_estimations_to_refresh():
    db = SessionLocal()
    print(f"[{datetime.now()}] Running scheduler check")
    try:
        now = datetime.now()
        estimations = db.query(EstimationM).filter(
            EstimationM.toRefresh == False,
            EstimationM.last_req != None
        ).all()

        for est in estimations:
            age_limit = est.last_req + timedelta(seconds=est.age)
            if now >= age_limit:
                est.toRefresh = True

        db.commit()
    finally:
        db.close()

scheduler = BackgroundScheduler()
scheduler.add_job(check_estimations_to_refresh, "interval", minutes=1)
scheduler.start()
