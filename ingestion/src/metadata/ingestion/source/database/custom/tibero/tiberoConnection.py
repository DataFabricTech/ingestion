from enum import Enum
from typing import Optional

from pydantic import BaseModel, Extra, Field

from metadata.ingestion.models.custom_pydantic import CustomSecretStr
from metadata.generated.schema.entity.services.connections import connectionBasicType
class TiberoType(Enum):
    Tibero = 'Tibero'

class TiberoScheme(Enum):
    Tibero_pyodbc = 'tibero+pyodbc'

class TiberoConnection(BaseModel):
    class Config:
        extra = Extra.forbid

    scheme: Optional[TiberoScheme] = Field(
        TiberoScheme.Tibero_pyodbc,
        description='SQLAlchemy driver scheme options.',
        title='Connection Scheme',
    )
    username: str = Field(
        ...,
        description='Username to connect to Tibero. This user should have privileges to read all the metadata in Tibero.',
        title='Username',
    )
    password: Optional[CustomSecretStr] = Field(
        None, description='Password to connect to Tibero.', title='Password'
    )
    odbcDsnName: Optional[str] = Field(
        None, description='ODBC DSN Name.', title='ODBC DSN Name'
    )
    connectionArguments: Optional[connectionBasicType.ConnectionArguments] = Field(
        None, title='Connection Arguments'
    )
    supportsMetadataExtraction: Optional[
        connectionBasicType.SupportsMetadataExtraction
    ] = Field(None, title='Supports Metadata Extraction')
    supportsDBTExtraction: Optional[connectionBasicType.SupportsDBTExtraction] = None
    supportsProfiler: Optional[connectionBasicType.SupportsProfiler] = Field(
        None, title='Supports Profiler'
    )
    sampleDataStorageConfig: Optional[
        connectionBasicType.SampleDataStorageConfig
    ] = Field(None, title='S3 Config for Sample Data')