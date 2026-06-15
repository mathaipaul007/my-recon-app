from pydantic import BaseModel

class KeyValueDTO(BaseModel):
    key : str
    value : str

    class Config:
        from_attributes = True
