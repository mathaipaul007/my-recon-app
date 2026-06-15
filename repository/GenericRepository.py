from sqlalchemy.orm import Session
from db.session import SessionLocal

def saveBulkData(bulkData):
    session = SessionLocal()
    session.bulk_save_objects(bulkData)
    session.commit()
    session.close()
