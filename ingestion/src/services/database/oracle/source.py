from services.database.database_source import DatabaseSource
class OracleSource(DatabaseSource):

    def getSourceConfig(self):
        from metadata.generated.schema.metadataIngestion.workflow import SourceConfig
        from metadata.generated.schema.metadataIngestion.databaseServiceMetadataPipeline import DatabaseServiceMetadataPipeline
        sourceConfig = SourceConfig(config=DatabaseServiceMetadataPipeline(**self.source_filter))
        return sourceConfig

    def getServiceConnection(self):
        from metadata.generated.schema.entity.services.databaseService import DatabaseConnection
        from metadata.generated.schema.entity.services.connections.database.oracleConnection import \
            OracleConnection

        serviceConnection = DatabaseConnection(config=OracleConnection(**{
            "type": "Oracle",
            "scheme": "oracle+cx_oracle",
            "username": self.source_user,
            "password": self.source_password,
            "hostPort": f"{self.source_host}:{self.source_port}",
            "oracleConnectionType": {"oracleServiceName": self.source_database},
        }))


        return serviceConnection
