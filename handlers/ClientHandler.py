from sqlalchemy.orm import Session
from db.session import SessionLocal
from dbmodels.Client import Client
from dto.ClientDTO import ClientDTO, PaginatedClientResponse
from repository.ClientRepository import get_client_count, get_client, new_dbclient
from security.jwt import hash_password

def read_clients(page: int, size: int):
    skip = (page - 1) * size

    total = get_client_count()
    users = get_client(skip, size)

    response = PaginatedClientResponse(
        total=total,
        page=page,
        size=size,
        data=[ClientDTO.model_validate(u) for u in users]
    )

    return response.model_dump()

def create_client(body):
    user = Client(**body)
    new_dbclient(user)
    return "success"

def update_client(body):
    return "success"
