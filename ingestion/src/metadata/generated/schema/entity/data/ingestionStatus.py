
from pydantic import BaseModel, Extra, Field

class IngestionStatus(BaseModel):
    class Config:
        extra = Extra.forbid

    systemname: str = Field(
        ..., description='ingestion system name'
    )