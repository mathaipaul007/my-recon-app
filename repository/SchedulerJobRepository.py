from sqlalchemy.orm import Session
from db.session import SessionLocal
from dbmodels.SchedulerJob import SchedulerJob
from sqlalchemy import select, func

def get_scheduled_jobs():
    db: Session = SessionLocal()

    try:
        stmt = (
            select(SchedulerJob)
            .where(SchedulerJob.status == "A")
        )

        users = db.execute(stmt).scalars().all()
        return users

    finally:
        db.close()
