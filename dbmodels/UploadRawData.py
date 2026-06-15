from sqlalchemy import Column, Integer, ForeignKey, String, CHAR
from sqlalchemy.dialects.postgresql import JSONB
from db.base import Base


class UploadRawData(Base):
    __tablename__ = "upload_raw_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    file_id = Column(Integer, ForeignKey("file_uploads.id"), nullable=False)
    data_type = Column(String(100), nullable=False)
    cleared_status = Column(CHAR(1), nullable=False, default="U")
    json_data = Column(JSONB)

    def __str__(self):
        return f"UploadRawData(name={self.json_data})"