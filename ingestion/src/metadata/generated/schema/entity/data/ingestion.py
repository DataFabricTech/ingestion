from pydantic import BaseModel, Extra, Field


class IngestionCheck(BaseModel):
    class Config:
        extra = Extra.forbid

    system_id: str = Field(
        ..., description='ingestion system name'
    )