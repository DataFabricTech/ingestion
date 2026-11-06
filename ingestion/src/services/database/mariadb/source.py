from services.database.database_source import DatabaseSource
class MariadbSource(DatabaseSource):

    def getSourceConfig(self):
        from metadata.generated.schema.metadataIngestion.workflow import SourceConfig
        from metadata.generated.schema.metadataIngestion.databaseServiceMetadataPipeline import DatabaseServiceMetadataPipeline
        sourceConfig = SourceConfig(config=DatabaseServiceMetadataPipeline(**self.source_filter))
        return sourceConfig

    def getServiceConnection(self):
        from metadata.generated.schema.entity.services.databaseService import DatabaseConnection
        from metadata.generated.schema.entity.services.connections.database.mariaDBConnection import \
            MariaDBConnection

        serviceConnection = DatabaseConnection(config=MariaDBConnection(**{
            "type": "MariaDB",
            "scheme": "mysql+pymysql",
            "username": self.source_user,
            "password": self.source_password,
            "hostPort": f"{self.source_host}:{self.source_port}",
        }))

        return serviceConnection
