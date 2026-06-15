from pydantic import BaseModel
from typing import List


class ClientDTO(BaseModel):
    id: int
    name: str
    descr: str
    status: str

    class Config:
        from_attributes = True


class PaginatedClientResponse(BaseModel):
    total: int
    page: int
    size: int
    data: List[ClientDTO]