from enum import Enum
from typing import Optional

from pydantic import BaseModel, Extra, Field

from metadata.ingestion.models.custom_pydantic import CustomSecretStr
from metadata.generated.schema.entity.services.connections import connectionBasicType
class AltibaseType(Enum):
    Altibase = 'Altibase'

class AltibaseODBCName(BaseModel):
    altibaseODBCName: str = Field(
        ...,
        description='The Altibase Service name is the ODBC alias that you give when you remotely connect to your database.',
        title='Oracle Service Name',
    )

class AltibaseScheme(Enum):
    altibase_pyodbc = 'altibase+pyodbc'

class AltibaseConnection(BaseModel):
    class Config:
        extra = Extra.forbid

    scheme: Optional[AltibaseScheme] = Field(
        AltibaseScheme.altibase_pyodbc,
        description='SQLAlchemy driver scheme options.',
        title='Connection Scheme',
    )
    username: str = Field(
        ...,
        description='Username to connect to Altibase. This user should have privileges to read all the metadata in Altibase.',
        title='Username',
    )
    password: Optional[CustomSecretStr] = Field(
        None, description='Password to connect to Altibase.', title='Password'
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