
from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Extra, Field

from ...entity.services import filesystemService
from ...type import basic, entityReference, tagLabel


class CreateFilesystemServiceRequest(BaseModel):
    class Config:
        extra = Extra.forbid

    name: basic.EntityName = Field(
        ..., description='Name that identifies the this entity instance uniquely'
    )
    displayName: Optional[str] = Field(
        None,
        description='Display Name that identifies this storage service. It could be title or label from the source services.',
    )
    description: Optional[basic.Markdown] = Field(
        None, description='Description of storage service entity.'
    )
    serviceType: filesystemService.FilesystemServiceType
    connection: Optional[filesystemService.FilesystemConnection] = None
    tags: Optional[List[tagLabel.TagLabel]] = Field(
        None, description='Tags for this Object Store Service.'
    )
    owner: Optional[entityReference.EntityReference] = Field(
        None, description='Owner of this object store service.'
    )
    dataProducts: Optional[List[basic.FullyQualifiedEntityName]] = Field(
        None,
        description='List of fully qualified names of data products this entity is part of.',
    )
    domain: Optional[str] = Field(
        None,
        description='Fully qualified name of the domain the Storage Service belongs to.',
    )
    systemType: Optional[str] = Field(
        None, description='system type'
    )
