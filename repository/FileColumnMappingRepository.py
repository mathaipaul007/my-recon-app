from sqlalchemy.orm import Session
from db.session import SessionLocal
from dbmodels.FileColumnMapping import FileColumnMapping
from sqlalchemy import select

def get_filecolumnmapping(clientid, filetype):
    db: Session = SessionLocal()
    try:
        stmt = select(FileColumnMapping).where(FileColumnMapping.client_id == clientid,
            FileColumnMapping.recon_file_type == filetype)
        mapping = db.scalars(stmt).first()

        return mapping
    finally:
        db.close()