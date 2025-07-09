from services.database.database_source import DatabaseSource
from metadata.generated.schema.metadataIngestion.workflow import SourceConfig
from metadata.generated.schema.metadataIngestion.databaseServiceMetadataPipeline import DatabaseServiceMetadataPipeline
from metadata.generated.schema.entity.services.databaseService import DatabaseConnection
from metadata.generated.schema.entity.services.connections.database.mssqlConnection import MssqlConnection

class MssqlSource(DatabaseSource):

    def getSourceConfig(self) -> SourceConfig:
        return SourceConfig(config=DatabaseServiceMetadataPipeline(**self.source_filter))

    def getServiceConnection(self) -> DatabaseConnection:

        serviceConnection = DatabaseConnection(config=MssqlConnection(**{
            "type": "Mssql",
            "scheme": "mssql+pymssql",
            "username": self.source_user,
            "password": self.source_password,
            "hostPort": f"{self.source_host}:{self.source_port}",
            "database": self.source_database
        }))

        return serviceConnection