from sqlalchemy.orm import import Session
from db.session import SessionLocal
from dbmodels.User import User
from dto.UserDTO import PaginatedUserResponse, UserDTO
from repository.UserRepository import get_users_count, get_users, new_dbuser, update_dbuser
from security.jwt import hash_password

def read_users(page: int, size: int):
    skip = (page - 1) * size

    total = get_users_count()
    users = get_users(skip, size)

    response = PaginatedUserResponse(
        total=total,
        page=page,
        size=size,
        data=[UserDTO.model_validate(u) for u in users]
    )

    return response.model_dump()

def create_user(body):
    user = User(**body)
    user.password = hash_password(user.password)
    new_dbuser(user)
    return "success"

def update_user(body):
    update_dbuser(body)
    return "success"
