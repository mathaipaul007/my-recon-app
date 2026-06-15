from sqlalchemy import Column, Integer, String, Date, LargeBinary, DateTime, func, JSON, ForeignKey, PrimaryKeyConstraint, Text
from db.base import Base


class SchedulerJob(Base):
    __tablename__ = "scheduler_job"

    id = Column(Integer, primary_key=True, index=True)
    client_id = Column(String(50), ForeignKey("recon_clients.id"), nullable=False)
    hour = Column(Integer, nullable=False)
    min = Column(Integer, nullable=False)
    status = Column(String(1), default="A", nullable=False)
    handler = Column(String(255), nullable=False)