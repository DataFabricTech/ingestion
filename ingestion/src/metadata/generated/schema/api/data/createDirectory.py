
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Extra, Field, constr

from ...entity.data import directory
from ...type import basic, entityReference


class CreateDirectoryRequest(BaseModel):
    class Config:
        extra = Extra.forbid

    name: directory.EntityName = Field(
        ..., description='Name that identifies this Container model.'
    )
    # displayName: Optional[str] = Field(
    #     None, description='Display Name that identifies this Container model.'
    # )
    description: Optional[basic.Markdown] = Field(
        None, description='Description of the Container instance.'
    )
    service: basic.FullyQualifiedEntityName = Field(
        ...,
        description='Link to the storage service where this container is hosted in.',
    )
    numberOfFiles: Optional[float] = Field(
        None, description='The number of objects/files this container has.'
    )
    size: Optional[float] = Field(
        None, description='The total size in KB this container has.'
    )
    owner: Optional[str] = Field(
        None, description='Owner of this database'
    )
    sourceHash: Optional[constr(min_length=1, max_length=32)] = Field(
        None, description='Source hash of the entity'
    )
    systemType: Optional[str] = Field(
        None, description='system type'
    )
    last_modified: Optional[str] = Field(None, description="modified")

