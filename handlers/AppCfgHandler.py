from repository.AppCfgClientRepository import get_app_cfg
from dto.KeyValueDTO import KeyValueDTO
from connexion import request

def get_cfg_values(cfgkey : str) -> list[KeyValueDTO]:
    usersession = request.context["token_info"]
    clientid = usersession['clientid']

    rows = get_app_cfg(clientid, cfgkey)

    return [
        {"key": row["key"], "value": row["value"]}
        for row in rows
    ]
