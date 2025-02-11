from src.services.database.database_source import DatabaseSource
class OracleSource(DatabaseSource):

    def getSourceConfig(self):
        from src.metadata.generated.schema.metadataIngestion.workflow import SourceConfig
        from src.metadata.generated.schema.metadataIngestion.databaseServiceMetadataPipeline import DatabaseServiceMetadataPipeline
        sourceConfig = SourceConfig(config=DatabaseServiceMetadataPipeline(**self.source_filter))
        return sourceConfig

    def getServiceConnection(self):
        from src.metadata.generated.schema.entity.services.databaseService import DatabaseConnection
        from src.metadata.generated.schema.entity.services.connections.database.oracleConnection import \
            OracleConnection

        serviceConnection = DatabaseConnection(config=OracleConnection(**{
            "type": "Oracle",
            "scheme": "oracle+cx_oracle",
            "username": self.source_user,
            "password": self.source_password,
            "hostPort": self.source_hostport,
            "oracleConnectionType": {"oracleServiceName": self.source_database},
        }))


        return serviceConnection
