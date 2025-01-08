
from pydantic import BaseModel, Extra, Field

class IngestionStatus(BaseModel):
    class Config:
        extra = Extra.forbid

    system_name: str = Field(
        ..., description='ingestion system name'
    )