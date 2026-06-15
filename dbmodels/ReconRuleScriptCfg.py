from sqlalchemy import Column, Integer, String, Date, LargeBinary, DateTime, func, JSON, ForeignKey, PrimaryKeyConstraint, Text
from db.base import Base


class ReconRuleScriptCfg(Base):
    __tablename__ = "recon_rule_script_cfg"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(50), ForeignKey("recon_clients.id"), nullable=False)
    rulename = Column(String(100), nullable=False)
    descr = Column(String(200), nullable=False)
    script_type = Column(String(200), nullable=False)
    script = Column(Text)
    status = Column(String(1), default="A", nullable=False)
    created_date = Column(DateTime, default=func.now())