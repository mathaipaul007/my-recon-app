from sqlalchemy import Column, Integer, String, Date, LargeBinary, DateTime, func, JSON, ForeignKey, PrimaryKeyConstraint, Text
from db.base import Base


class UserRoles(Base):
    __tablename__ = "recon_user_role"

    role = Column(String(100), primary_key=True)
    descr = Column(String(200), nullable=False)