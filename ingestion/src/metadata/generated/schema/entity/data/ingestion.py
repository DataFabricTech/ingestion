from pydantic import BaseModel, Extra, Field


class IngestionCheck(BaseModel):
    class Config:
        extra = Extra.forbid

    system_id: str = Field(
        ..., description='ingestion system name'
    )


class IngestionStatus(BaseModel):

    system_id: str = Field(..., description='system_id')
    status: str = Field(..., description='수집 상태')
    err_description: str = Field(None, description='에러 내용')
    user: str = Field(None, description='api 호출자')