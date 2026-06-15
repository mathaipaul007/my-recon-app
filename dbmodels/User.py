from sqlalchemy import Column, Integer, String, Date, LargeBinary, DateTime, func, JSON, ForeignKey, PrimaryKeyConstraint, Text
from db.base import Base


class User(Base):
    __tablename__ = "recon_users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(100), nullable=False)
    usermail = Column(String(100), nullable=False)
    password = Column(String(255), nullable=False)
    eff_status = Column(String(1), default="A")
    admin_user = Column(String(1), default="N")
    created_date = Column(DateTime, default=func.now())