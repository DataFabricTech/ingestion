from services.database.database_source import DatabaseSource
class TrinoSource(DatabaseSource):

    def getSourceConfig(self):
        from metadata.generated.schema.metadataIngestion.workflow import SourceConfig

        from metadata.generated.schema.metadataIngestion.databaseServiceMetadataPipeline import DatabaseServiceMetadataPipeline
        sourceConfig = SourceConfig(config=DatabaseServiceMetadataPipeline(**self.source_filter))

        return sourceConfig

    def getServiceConnection(self):
        from metadata.generated.schema.entity.services.databaseService import DatabaseConnection
        from metadata.generated.schema.entity.services.connections.database.trinoConnection import \
            TrinoConnection

        serviceConnection = DatabaseConnection(config=TrinoConnection(**{
            "type": "Trino",
            "scheme": "trino",
            "username": self.source_user,
            "authType": {"password": self.source_password},
            "hostPort": f"{self.source_host}:{self.source_port}",
            "catalog": self.source_database,
        }))
        return serviceConnection
