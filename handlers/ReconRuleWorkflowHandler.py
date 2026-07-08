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

def save_rules(body):
    #ruleList : List[ReconWorkflowRuleDTO]
    #ruleList = [ReconWorkflowRuleDTO.model_validate(x) for x in body]
    ruleList : ReconRuleWorkflow = []

    workflowId = body["workflowId"]
    ruleData = body["ruleData"]
    print(workflowId)
    print(ruleData)

    for x in ruleData:
        rrw = ReconRuleWorkflow()
        for k,v in x.items():
            setattr(rrw, k, v)

        workflowId=rrw.workflowid
        ruleList.append(rrw)
    print("Removing the rules for workflow ",workflowId)
    remove_rules(workflowId)
    save_raw_data(ruleList)
    return "success"
