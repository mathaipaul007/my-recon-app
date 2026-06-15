from pydantic import BaseModel
from typing import List
from datetime import datetime

class UserDTO(BaseModel):
    id: int
    username: str
    usermail: str
    eff_status: str
    admin_user: str

    class Config:
        from_attributes = True

class PaginatedUserResponse(BaseModel):
    total: int
    page: int
    size: int
    data: List[UserDTO]

class UserLoginDTO(BaseModel):
    username: str
    password: str
    client_id: str
