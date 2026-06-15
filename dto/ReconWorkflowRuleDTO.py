from pydantic import BaseModel
from typing import List

class ReconWorkflowRuleDTO(BaseModel):
    ruleId : int
    ruleName : str
    descr : str

    class Config:
        from_attributes = True
