

from __future__ import annotations

from enum import Enum
from typing import Optional, Union

from pydantic import BaseModel, Extra, Field

from ..type import filterPattern

class FilesystemMetadataConfigType(Enum):
    FilesystemMetadata = 'FilesystemMetadata'



class FilesystemServiceMetadataPipeline(BaseModel):
    class Config:
        extra = Extra.forbid

    type: Optional[FilesystemMetadataConfigType] = Field(
        FilesystemMetadataConfigType.FilesystemMetadata, description='Pipeline type'
    )
    filesystemFilterPattern: Optional[filterPattern.FilterPattern] = Field(
        None, description='Regex to only fetch file name that matches the pattern.'
    )
