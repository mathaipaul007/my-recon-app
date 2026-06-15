from sqlalchemy.orm import Session
from db.session import SessionLocal
from dbmodels.ReconReportBatch import ReconReportBatch
from dbmodels.ClientUser import ClientUser
from dbmodels.ReconReport import ReconReport
from sqlalchemy import select, update, func, and_, delete
from dto.ReconReportBatchDTO import ReconReportBatchDTO
from sqlalchemy.exc import SQLAlchemyError

def save_bulk_data(reconReportList):
    db: Session = SessionLocal()
    
    try:
        db.bulk_save_objects(reconReportList)
        db.commit()
    finally:
        db.close()

def clear_all_report():
    db: Session = SessionLocal()
    
    try:
        stmt = (delete(ReconReport))
        db.execute(stmt)
        db.commit()
    finally:
        db.close()

def get_recon_report_json(clientid, reportid, reportType):
    db: Session = SessionLocal()
    try:
        stmt = (
            select(ReconReport.details)
            .join(ReconReportBatch, ReconReportBatch.batchid == ReconReport.batchid)
            .where(and_(ReconReportBatch.batchid == reportid,
                        ReconReport.report_type == reportType,
                        ReconReportBatch.client_id == clientid
            ))
        )
        reportJson = db.execute(stmt).scalars().all()
        return reportJson
    finally:
        db.close()
