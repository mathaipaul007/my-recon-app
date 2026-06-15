from sqlalchemy.orm import Session
from db.session import SessionLocal
from repository.ReconRuleWorkflowRepository import get_recon_wrkflw, new_recon_wrkflw, save_raw_data, get_recon_wrkflw_rules, remove_rules
from security.jwt import hash_password
from connexion import request
from dbmodels.ReconRuleWorkflow import ReconWorkflow, ReconRuleWorkflow
from dto.ReconRuleWorkflowRuleDTO import ReconWorkflowRuleDTO
from typing import List

def get_recon_workflow():
    usersession = request.context["token_info"]
    clientid = usersession['clientid']

    rows = get_recon_wrkflw(clientid)

    return [
        {"key": row.id, "value": row.workflowname}
        for row in rows
    ]


def get_recon_workflow_rules(workflowId : int):
    usersession = request.context["token_info"]
    clientid = usersession['clientid']
    rows = get_recon_wrkflw_rules(clientid, workflowId)

    ruleList = [ReconWorkflowRuleDTO.model_validate(x).model_dump(mode="json") for x in rows]
    return ruleList


def create_new(body):
    usersession = request.context["token_info"]
    clientid = usersession['clientid']
    body["client_id"] = clientid

    wrkflw = ReconWorkflow(**body)
    new_recon_wrkflw(wrkflw)

    return "success"