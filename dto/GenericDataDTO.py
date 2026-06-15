from typing import Optional
from pydantic import BaseModel, Field
from typing import List, Any, Dict

class GenericDataDTO(BaseModel):
    id: int
    json_data: Dict[str, Any]
    class Config:
        from_attributes = True

class PaginatedGenericDataResponse(BaseModel):
    total: int
    page: int
    size: int
    data: List[GenericDataDTO]
