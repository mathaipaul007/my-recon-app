from sqlalchemy import Column, Integer, ForeignKey, JSON, PrimaryKeyConstraint, String, func, DateTime
from db.base import Base

#, autoincrement=True

class ReconReportBatch(Base):
    __tablename__ = "recon_report_batch"

    batchid = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(50), ForeignKey("recon_clients.id"), nullable=False)
    report_query = Column(JSON)
    matched = Column(Integer, default=0)
    unmatched = Column(Integer, default=0)
    report_status = Column(String(50), nullable=False)
    generate_user_id = Column(Integer, ForeignKey("recon_users.id"), nullable=False)
    generate_timestamp=Column(DateTime, default=func.now())