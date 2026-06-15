from sqlalchemy import Column, Integer, String, DateTime, func, ForeignKey
from db.base import Base

class ClientUser(Base):
    __tablename__ = "recon_client_users"

    id = Column(Integer, primary_key=True, index=True)
    client_id = Column(String(50), ForeignKey("recon_clients.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("recon_users.id"), nullable=False)
    role = Column(String(100), ForeignKey("recon_user_role.role"), nullable=False)
    status = Column(String(1), default="A")
    created_date = Column(DateTime, default=func.now())
