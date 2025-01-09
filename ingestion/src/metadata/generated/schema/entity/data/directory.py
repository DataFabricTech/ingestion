
from typing import List, Optional

from pydantic import BaseModel, Extra, Field, constr

from ...type import (
    basic,
    entityReference,
)

class EntityName(BaseModel):
    __root__: constr(regex=r'^((?!::).)*$', min_length=1, max_length=128) = Field(
        ...,
        description='Name of a directory.',
    )

class Directory(BaseModel):
    class Config:
        extra = Extra.forbid

    id: basic.Uuid = Field(
        ..., description='Unique identifier that identifies this directry instance.'
    )
    name: EntityName = Field(..., description='Name of a directory.')

    fullyQualifiedName: Optional[basic.FullyQualifiedEntityName] = Field(
        None,
        description="",
    )
    href: Optional[basic.Href] = Field(
        None, description='Link to the resource corresponding to this entity.'
    )
    owner: Optional[entityReference.EntityReference] = Field(
        None, description='Owner of this directory.'
    )
    numberOfFiles: Optional[float] = Field(
        None, description='The number of files this directory has.'
    )
    size: Optional[float] = Field(
        None, description='The total size in KB this directory has.'
    )
    sourceHash: Optional[constr(min_length=1, max_length=32)] = Field(
        None, description='Source hash of the entity'
    )
    systemType: Optional[str] = Field(
        None, description='system type'
    )