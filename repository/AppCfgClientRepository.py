from sqlalchemy.orm import Session
from db.session import SessionLocal
from dbmodels.AppCfgClients import AppCfgClients
from sqlalchemy import select, func, update

def get_app_cfg_by_name(clientId : str, key : str, name : str):
    db: Session = SessionLocal()
    try:
        stmt = select(AppCfgClients).where(AppCfgClients.client_id == clientId,
            AppCfgClients.cfg_key == key, AppCfgClients.cfg_name == name)
        appcfg = db.scalars(stmt).first()

        return appcfg
    finally:
        db.close()

def get_app_cfg(clientId : str, key : str):
    db: Session = SessionLocal()
    try:
        stmt = select(AppCfgClients.cfg_name.label("key"), AppCfgClients.cfg_value.label("value")).where(AppCfgClients.client_id == clientId,
            AppCfgClients.cfg_key == key)
        
        return db.execute(stmt).mappings().all()

    finally:
        db.close()

def update_app_cfg(clientId : str, key : str, name : str, value: str):
    db: Session = SessionLocal()

    try:
        dbappcfg = db.query(AppCfgClients).filter(AppCfgClients.client_id == clientId,
            AppCfgClients.cfg_key == key, AppCfgClients.cfg_name == name).first()
        
        if not dbappcfg:
            raise Exception("User not found")
        
        dbappcfg.cfg_value = value

        db.commit()
        db.refresh(dbappcfg)

        return dbappcfg

    finally:
        db.close()