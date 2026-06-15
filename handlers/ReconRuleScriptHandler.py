from sqlalchemy.orm import Session
from db.session import SessionLocal
from dbmodels.ReconRuleScriptCfg import ReconRuleScriptCfg
from dto.ReconRuleScriptCfgDTO import ReconRuleScriptCfgDTO, PaginatedReconRuleScriptCfgResponse
from repository.ReconRuleScriptCfgRepository import get_recon_rule_count, get_recon_rule, new_recon_rule, update_recon_rule
from security.jwt import hash_password
from connexion import request

def read_recon_rules(**kwargs):
    usersession = request.context["token_info"]
    clientid = usersession['clientid']

    bodyDict = kwargs.get("body", {})

    filters = {}
    page: int = int(bodyDict['page'])
    size: int = int(bodyDict['size'])

    for k,v in bodyDict.items():
        if k not in ["page","size"]:
            filters[k] = v

    skip = (page - 1) * size

    total = get_recon_rule_count(clientid,filters)
    rules = get_recon_rule(clientid,skip,size, filters)

    response = PaginatedReconRuleScriptCfgResponse(
        total=total,
        page=page,
        size=size,
        data=[ReconRuleScriptCfgDTO.model_validate(u) for u in rules]
    )

    return response.model_dump()


def save_script(body):
    usersession = request.context["token_info"]
    clientid = usersession['clientid']
    body["client_id"] = clientid
    print(body)
    reconScript = ReconRuleScriptCfg(**body)
    if(reconScript.id is None or reconScript.id == ''):
        new_recon_rule(reconScript)
    else:
        update_recon_rule(reconScript)
    return "success"