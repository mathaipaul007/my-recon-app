from sqlalchemy.orm import Session
from db.session import SessionLocal
from dbmodels.UserRoles import UserRoles
from sqlalchemy import select, func, update

def get_userrole():
    db: Session = SessionLocal()
    try:
        stmt = select(UserRoles.role.label("key"), UserRoles.descr.label("value"))
        return db.execute(stmt).mappings().all()
    finally:
        db.close()
