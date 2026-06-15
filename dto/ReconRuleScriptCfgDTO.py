from typing import Optional
from pydantic import BaseModel, Field
from typing import List

class ReconRuleScriptCfgDTO(BaseModel):
    id: int
    client_id: str
    rulename: str
    descr: Optional[str] = None
    script_type: str
    script: str
    status: Optional[str] = None

    class Config:
        from_attributes = True

class PaginatedReconRuleScriptCfgResponse(BaseModel):
    total: int
    page: int
    size: int
    data: List[ReconRuleScriptCfgDTO]
