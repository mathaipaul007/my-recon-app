from sqlalchemy.orm import Session
from db.session import SessionLocal
from dbmodels.User import User
from dbmodels.Client import Client

from dbmodels.ReconRuleScriptCfg import ReconRuleScriptCfg

from dto.ClientDTO import ClientDTO, PaginatedClientResponse
from sqlalchemy import select, func, update

def getConditions(clientId: str, filterDict: dict):
    conditions = []
    conditions.append(ReconRuleScriptCfg.client_id == clientId)

    for k, v in filterDict.items():
        if k == "rulename":
            conditions.append(ReconRuleScriptCfg.rulename == v)

    return conditions

def get_recon_rule_count(clientId: str, filterDict: dict):
    db: Session = SessionLocal()
    try:
        conditions = getConditions(clientId, filterDict)
        
        stmt = select(func.count(ReconRuleScriptCfg.id)).join(Client, ReconRuleScriptCfg.client_id == Client.id).where(*conditions)
        total = db.execute(stmt).scalar_one()

        return total
    finally:
        db.close()

def get_recon_rule(clientId: str, offset: int, limit: int, filterDict: dict):
    db: Session = SessionLocal()

    try:
        conditions = getConditions(clientId, filterDict)
        stmt = (
            select(ReconRuleScriptCfg)
            .join(Client, ReconRuleScriptCfg.client_id == Client.id)
            .where(*conditions)
            .order_by(Client.id.desc())
            .offset(offset)
            .limit(limit)
        )

        users = db.execute(stmt).scalars().all()
        return users

    finally:
        db.close()

def new_recon_rule(reconRuleScriptCfg: ReconRuleScriptCfg):
    db: Session = SessionLocal()

    try:
        db.add(reconRuleScriptCfg)
        db.commit()
        db.refresh(reconRuleScriptCfg)
        return reconRuleScriptCfg

    finally:
        db.close()

def update_recon_rule(reconRuleScriptCfg: ReconRuleScriptCfg):
    db: Session = SessionLocal()

    try:
        stmt = (
            update(ReconRuleScriptCfg)
            .where(ReconRuleScriptCfg.id == int(reconRuleScriptCfg.id))
            .values(
                rulename = reconRuleScriptCfg.rulename,
                descr = reconRuleScriptCfg.descr,
                script_type = reconRuleScriptCfg.script_type,
                script = reconRuleScriptCfg.script
            )
        )

        db.execute(stmt)
        db.commit()
        updated = db.get(ReconRuleScriptCfg, int(reconRuleScriptCfg.id))
        return updated
    finally:
        db.close()