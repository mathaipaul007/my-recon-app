from sqlalchemy import Column, Integer, String, Date, LargeBinary, DateTime, func, JSON, ForeignKey, PrimaryKeyConstraint, Text
from db.base import Base

class FileUploads(Base):
    __tablename__ = "file_uploads"

    id = Column(Integer, primary_key=True, index=True)
    client_id = Column(String(50), ForeignKey("recon_clients.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("recon_users.id"), nullable=False)
    recon_file_type = Column(String(100), nullable=False)
    recon_file_desc = Column(String(200), nullable=False)
    uploaded_timestamp = Column(DateTime, default=func.now())
    upload_file_name = Column(String(255), nullable=False)
    upload_file_data = Column(LargeBinary, nullable=False)
    upload_file_type = Column(String(250), nullable=True)
    upload_file_status = Column(String(50), nullable=False)
