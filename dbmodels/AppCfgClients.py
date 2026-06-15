from sqlalchemy import Column, Integer, String, ForeignKey, Text
from db.base import Base

class AppCfgClients(Base):
    __tablename__ = "application_cfg_clients"
    id = Column(Integer, primary_key=True, index=True)
    client_id = Column(String(50), ForeignKey("recon_clients.id"), nullable=False)
    cfg_key = Column(String(100), nullable=False)
    cfg_name = Column(String(100), nullable=False)
    cfg_value = Column(Text, nullable=False)
    file_desc = Column(String(200), nullable=False)
