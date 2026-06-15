from sqlalchemy.orm import Session
from db.session import SessionLocal
from dbmodels.ReconReportBatch import ReconReportBatch
from dbmodels.ClientUser import ClientUser
from dbmodels.User import User
from sqlalchemy import select, update, func, and_, delete
from dto.ReconReportBatchDTO import ReconReportBatchDTO
from sqlalchemy.exc import SQLAlchemyError

def get_reportbatch_count(clientId : str):
    db: Session = SessionLocal()
    try:
        stmt = select(func.count()).select_from(ReconReportBatch).where(ReconReportBatch.client_id == clientId)
        total = db.execute(stmt).scalar_one()

        return total
    finally:
        db.close()

def get_reconreportbatch(clientId : str, offset: int, limit: int):
    db: Session = SessionLocal()

    try:
        stmt = (
            select(ReconReportBatch, User.username)
            .join(ClientUser, ReconReportBatch.client_id == ClientUser.client_id)
            .join(User, ReconReportBatch.generate_user_id == User.id)
            .where(
                and_(
                    ReconReportBatch.client_id == clientId,
                    ClientUser.user_id == User.id
                )
            )
            .order_by(ReconReportBatch.batchid.desc())
            .offset(offset)
            .limit(limit)
        )

        reconreport = db.execute(stmt).all()
        return reconreport
    
    finally:
        db.close()

def get_pending_reconbatch(clientId : str):
    db: Session = SessionLocal()

    try:
        stmt = (
            select(ReconReportBatch)
            .where(
                ReconReportBatch.client_id == clientId,
                ReconReportBatch.report_status == 'not_started'
            )
            .limit(1)
        )

        batch = db.execute(stmt).scalar_one_or_none()

        return batch
    
    finally:
        db.close()

def save_reconreport_data(reconReportBatchDTO : ReconReportBatchDTO):
    db: Session = SessionLocal()

    reconreport_data = ReconReportBatch(**reconReportBatchDTO.model_dump())
    try:
        db.add(reconreport_data)
        db.commit()
        db.refresh(reconreport_data)
        return reconreport_data
    
    finally:
        db.close()

def clear_all_report_batch():
    db: Session = SessionLocal()

    try:
        stmt = (delete(ReconReportBatch))
        db.execute(stmt)
        db.commit()
    finally:
        db.close()

def update_by_recon_status(recon_obj):

    db: Session = SessionLocal()
    try:
        stmt = (
            update(ReconReportBatch)
            .where(
                ReconReportBatch.batchid == recon_obj.batchid,
                ReconReportBatch.client_id == recon_obj.client_id
            )
            .values(
                matched=recon_obj.matched,
                unmatched=recon_obj.unmatched,
                report_status= recon_obj.report_status
            )
        )

        result = db.execute(stmt)
        db.commit()

        return result.rowcount
    
    except SQLAlchemyError as e:
        db.rollback()
        raise

    finally:
        db.close()