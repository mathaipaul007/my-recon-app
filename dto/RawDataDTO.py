from typing import Optional
from pydantic import BaseModel, field
from typing import List

class RawDataDTO(BaseModel):
    id: int
    from_date : Optional[str] = None
    to_date : Optional[str] = None
    account : Optional[str] = None
    bankname : Optional[str] = None
    branchname : Optional[str] = None
    amount : Optional[float] = None
    class Config:
        from_attributes = True
        extra="ignore"

class PaginatedRawDataResponse(BaseModel):
    total: int
    page: int
    size: int
    data: List[RawDataDTO]
