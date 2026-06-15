from sqlalchemy.orm import Session
from db.session import SessionLocal
from dbmodels.ClientUser import ClientUser
from dto.UserDTO import PaginatedUserResponse, UserDTO
from repository.ClientUserRepository import get_clientuser_count, get_clientuser, new_clientuser
from connexion import request

def read_client_user(page: int, size: int):
    usersession = request.context["token_info"]
    clientid = usersession['clientid']
    skip = (page - 1) * size

    total = get_clientuser_count(clientid)
    users = get_clientuser(clientid, skip, size)
    print(users)

    response = PaginatedUserResponse(
        total=total,
        page=page,
        size=size,
        data=[UserDTO.model_validate(u) for u in users]
    )

    return response.model_dump()


def create_client_user(body):

    user = ClientUser(**body)
    new_clientuser(user)
    return "success"


def delete_client_user(body):
    return "success"