from typing import Optional, List
from pydantic import BaseModel, Field


class FileUploadDTO(BaseModel):
    id: Optional[int] = None
    client_id: str
    user_id: int
    recon_file_type: str
    recon_file_desc: str
    upload_file_name: str
    upload_file_data: bytes
    upload_file_type: str
    upload_file_status: str

    class Config:
        from_attributes = True


class FileUploadNoBlobDTO(BaseModel):
    id: int
    client_id: str
    user_id: int
    recon_file_type: str
    recon_file_desc: str
    upload_file_name: str
    upload_file_type: str
    upload_file_status: str

    class Config:
        from_attributes = True


class PaginatedFileUploadResponse(BaseModel):
    total: int
    page: int
    size: int
    data: List[FileUploadNoBlobDTO]