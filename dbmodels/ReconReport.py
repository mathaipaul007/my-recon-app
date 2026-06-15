from sqlalchemy import Column, Integer, ForeignKey, JSON, String
from db.base import Base

class ReconReport(Base):
    __tablename__ = "recon_report"

    id = Column(Integer, primary_key=True, autoincrement=True)
    batchid = Column(Integer, ForeignKey("recon_report_batch.batchid"), nullable=False)
    report_type = Column(String(100), nullable=False)
    details = Column(JSON)


    #def __str__(self):
    #    return f"UploadRawData(batchid={self.batchid},ruleid={self.ruleid},details={self.details})"