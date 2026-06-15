from repository.UserRoleRepository import get_userrole
from dto.KeyValueDTO import KeyValueDTO
from connexion import request

def get_user_role() -> list[KeyValueDTO]:
    rows = get_userrole()

    return [
        {"key": row["key"], "value": row["value"]}
        for row in rows
    ]
