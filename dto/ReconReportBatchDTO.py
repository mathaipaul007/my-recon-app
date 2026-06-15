from pydantic import BaseModel
from typing import List
from datetime import datetime
from typing import Optional, Dict, Any

class ReconReportBatchDTO(BaseModel):
    batchid: int
    client_id: str
    report_query: Optional[Dict[str, Any]] = None
    matched: int = 0
    unmatched: int = 0
    report_status: str
    generate_user_id: int
    class Config:
        from_attributes = True

class ReportBatchDTO(BaseModel):
    id: int
    report_query: Dict[str, Any]
    report_status: str
    matched: int = 0
    unmatched: int = 0
    generated_by: str

    class Config:
        from_attributes = True

class PaginatedReportBatchResponse(BaseModel):
    total: int
    page: int
    size: int
    data: List[ReportBatchDTO]
