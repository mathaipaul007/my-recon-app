from sqlalchemy import Column, Integer, String, Date, LargeBinary, DateTime, func, JSON, ForeignKey, PrimaryKeyConstraint, Text
from db.base import Base


class ReconWorkflow(Base):
    __tablename__ = "recon_workflow"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(50), ForeignKey("recon_clients.id"), nullable=False)
    workflowname = Column(String(100), nullable=False)


class ReconRuleWorkflow(Base):
    __tablename__ = "recon_rule_workflow"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_seq = Column(Integer)
    ruleid = Column(Integer, ForeignKey("recon_rule_script_cfg.id"), nullable=False)
    workflowid = Column(Integer, ForeignKey("recon_workflow.id"), nullable=False)