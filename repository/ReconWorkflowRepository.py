from sqlalchemy.orm import Session
from db.session import SessionLocal
from dbmodels.User import User
from dbmodels.Client import Client
from dbmodels.ReconRuleWorkflow import ReconWorkflow, ReconRuleWorkflow
from dbmodels.ReconRuleScriptCfg import ReconRuleScriptCfg
from sqlalchemy import select, func, delete

def get_recon_wrkflw(clientId : str):
    db: Session = SessionLocal()
    try:
        stmt = (
            select(ReconWorkflow)
            .join(Client, ReconWorkflow.client_id == Client.id)
        )
        
        wrkflw = db.execute(stmt).scalars().all()
        return wrkflw
    finally:
        db.close()

def get_recon_wrkflw_rules(clientId : str, workflowId : int):
    db: Session = SessionLocal()
    try:
        stmt = (
            select(
                ReconRuleScriptCfg.id.label("ruleId"),
                ReconRuleScriptCfg.rulename.label("ruleName"),
                ReconRuleScriptCfg.descr.label("descr")
            )
            .join(ReconRuleWorkflow, ReconRuleWorkflow.ruleid == ReconRuleScriptCfg.id)
            .join(ReconWorkflow, ReconWorkflow.id == ReconRuleWorkflow.workflowid)
            .join(Client, ReconWorkflow.client_id == Client.id)
            .where(ReconRuleWorkflow.workflowid == workflowId)
            .order_by(ReconRuleWorkflow.run_seq)
        )

        rules = db.execute(stmt).all()
        return rules
    finally:
        db.close()

def new_recon_wrkflw(reconWorkflow: ReconWorkflow):
    db: Session = SessionLocal()
    try:
        db.add(reconWorkflow)
        db.commit()
        db.refresh(reconWorkflow)
        return reconWorkflow
    finally:
        db.close()

def remove_rules(workflowId):
    db: Session = SessionLocal()
    try:
        stmt = delete(ReconRuleWorkflow).where(ReconRuleWorkflow.workflowid == workflowId)
        db.execute(stmt)
        db.commit()
    finally:
        db.close()

def save_raw_data(records):
    db: Session = SessionLocal()
    try:
        db.bulk_save_objects(records)
        db.commit()
    finally:
        db.close()