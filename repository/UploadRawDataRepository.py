from sqlalchemy.orm import Session
from db.session import SessionLocal
from dbmodels.UploadRawData import UploadRawData
from dbmodels.FileUploads import FileUploads
from sqlalchemy import select, func, and_, update
import json

def save_raw_data(fileid, file_type, clientId, jsonData):
    db: Session = SessionLocal()
    records = [UploadRawData(file_id=fileid, data_type=file_type, json_data=json.loads(row)) for row in jsonData]
    try:
        db.bulk_save_objects(records)
        db.commit()
    finally:
        db.close()

def update_cleared_status(ids):
    db: Session = SessionLocal()
    try:
        stmt = (
            update(UploadRawData)
            .where(UploadRawData.id.in_(ids))
            .values(cleared_status='C')
        )
        db.execute(stmt)
        db.commit()
    finally:
        db.close()

def reset_all_status():
    db: Session = SessionLocal()
    try:
        stmt = (
            update(UploadRawData)
            .values(cleared_status='U')
        )
        db.execute(stmt)
        db.commit()
    finally:
        db.close()

def getConditions(clientId: str, filterDict: dict):
    conditions = []
    conditions.append(FileUploads.client_id == clientId)

    for k,v in filterDict.items():
        if k == "fileType":
            conditions.append(FileUploads.recon_file_type == v)
        if k == "settleNo" and len(v) > 0:
            conditions.append(UploadRawData.json_data["settlement_no"].astext == v)
            
    return conditions

def get_raw_data_count(clientId: str, filterDict: dict):
    db: Session = SessionLocal()
    try:
        conditions = getConditions(clientId, filterDict)
        stmt = (
            select(func.count())
            .select_from(UploadRawData)
            .join(FileUploads, UploadRawData.file_id == FileUploads.id)
            .where(*conditions)
        )
        total = db.execute(stmt).scalar_one()
        return total
    finally:
        db.close()

def get_rawdata(offset: int, limit: int, clientId: str, filterDict: dict):
    db: Session = SessionLocal()
    try:
        conditions = getConditions(clientId, filterDict)
        stmt = (
            select(UploadRawData)
            .join(FileUploads, UploadRawData.file_id == FileUploads.id)
            .where(*conditions)
            .order_by(UploadRawData.id.desc())
            .offset(offset)
            .limit(limit)
        )
        users = db.execute(stmt).scalars().all()
        return users
    finally:
        db.close()

def get_raw_all_data(clientId : str, fileType : str):
    db : Session = SessionLocal()

    try:
        stmt = (
            select(UploadRawData)
            .join(FileUploads, UploadRawData.file_id == FileUploads.id)
            .where(
                and_(
                    FileUploads.client_id == clientId,
                    FileUploads.recon_file_type == fileType
                )
            )
            .order_by(UploadRawData.id.desc())
        )

        users = db.execute(stmt).scalars().all()
        return users
    
    finally:
        db.close()

def get_rawdata_byfilter(clientId: str, filetype: str, filter):
    db: Session = SessionLocal()
    conditions = []
    try:
        for key, value in filter.items():
            if (key == "from_date"):
                conditions.append(UploadRawData.json_data[key].as_string() >= value)
            elif (key == "to_date"):
                conditions.append(UploadRawData.json_data[key].as_string() <= value)
            else:
                conditions.append(UploadRawData.json_data[key].as_string() == value)
        
        stmt = (
            select(UploadRawData)
            .join(FileUploads, UploadRawData.file_id == FileUploads.id)
            .where(FileUploads.client_id == clientId, FileUploads.recon_file_type == filetype, and_(*conditions))
        )
        result = db.execute(stmt).scalars().all()
        return result
    finally:
        db.close()

def get_all_rawdata(clientId : str, filetype : str):
    db: Session = SessionLocal()
    try:
        stmt = (
            select(UploadRawData)
            .join(FileUploads, UploadRawData.file_id == FileUploads.id)
            .where(FileUploads.client_id == clientId, FileUploads.recon_file_type == filetype)
        )
        result = db.execute(stmt).scalars().all()
        return result
    finally:
        db.close()