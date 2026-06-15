from sqlalchemy import Column, Integer, String, ForeignKey, JSON
from db.base import Base

class FileColumnMapping(Base):
    __tablename__ = "file_column_mapping"

    id = Column(Integer, primary_key=True, index=True)
    client_id = Column(String(50), ForeignKey("recon_clients.id"), nullable=False)
    header_row = Column(Integer, default=1, nullable=False)
    sheet_idx = Column(Integer, default=0, nullable=False)
    recon_file_type = Column(String(100), nullable=False)
    json_data = Column(JSON)
    mandatory_column = Column(JSON)
