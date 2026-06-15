from database import SessionLocal
from sqlalchemy import select, update, and_
import models

def getFileColumnMapping(clientid, filetype):
    db: Session = next(get_db())
    stmt = select(models.FileColumnMapping).where(models.FileColumnMapping.client_id == clientid,
        models.FileColumnMapping.recon_file_type == filetype)
    mapping = db.scalars(stmt).first()
    return mapping

def getCustomRule(clientid):
    db: Session = next(get_db())
    stmt = select(models.AppCfgClients.cfg_value.label("value")).where(
        models.AppCfgClients.client_id == clientid,
        models.AppCfgClients.cfg_key == "RECON_CUSTOM_RULE"
    )
    rows = db.execute(stmt).all()
    return rows[0][0]

def saveBulkData(bulkData):
    session = SessionLocal()
    session.bulk_save_objects(bulkData)
    session.commit()
    session.close()

def updateReconStatus(recon_obj):
    session = SessionLocal()
    stmt = update(models.ReconReportBatch).where(
            models.ReconReportBatch.batchid == recon_obj.batchid,
            models.ReconReportBatch.client_id == recon_obj.client_id
        ) .values(
            matched=recon_obj.matched,
            unmatched=recon_obj.unmatched,
            report_status=recon_obj.report_status
        )
    session.execute(stmt)
    session.commit()
