from src.services.database.database_source import DatabaseSource
from src.metadata.generated.schema.metadataIngestion.workflow import SourceConfig
from src.metadata.generated.schema.metadataIngestion.databaseServiceMetadataPipeline import DatabaseServiceMetadataPipeline
from src.metadata.generated.schema.entity.services.databaseService import DatabaseConnection
from src.metadata.generated.schema.entity.services.connections.database.mssqlConnection import MssqlConnection

class MssqlSource(DatabaseSource):

    def getSourceConfig(self) -> SourceConfig:
        return SourceConfig(config=DatabaseServiceMetadataPipeline(**self.source_filter))

    def getServiceConnection(self) -> DatabaseConnection:

        serviceConnection = DatabaseConnection(config=MssqlConnection(**{
            "type": "Mssql",
            "scheme": "mssql+pymssql",
            "username": self.source_user,
            "password": self.source_password,
            "hostPort": self.source_hostport,
            "database": self.source_database
        }))

        return serviceConnection