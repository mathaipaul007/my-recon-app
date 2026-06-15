from sqlalchemy import Column, Integer, String, Date, LargeBinary, DateTime, func, JSON, ForeignKey, PrimaryKeyConstraint, Text
from db.base import Base

class Client(Base):
    __tablename__ = "recon_clients"

    id = Column(String(50), primary_key=True)
    name = Column(String(100), nullable=False)
    descr = Column(String(200), nullable=False)
    status = Column(String(50), nullable=False)
    created_date = Column(DateTime, default=func.now())
