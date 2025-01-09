from enum import Enum
from typing import Optional
from pydantic import BaseModel, Extra, Field


class SshType(Enum):
    Linux = 'Linux'

class SshConnection(BaseModel):
    class Config:
        extra = Extra.forbid

    def __getitem__(self, item):
        return getattr(self, item)

    type: Optional[SshType] = Field(
        SshType.Linux, description='Service Type', title='Service Type'
    )

    hostname: str = Field(..., description='the server to connect to')
    port: Optional[int] = Field(22, description='the server port to connect to')
    username: str = Field(..., description='the username to authenticate as (defaults to the current local username)')
    password: str = Field(..., description='Used for password authentication; is also used for private key decryption if passphrase is not given.')
