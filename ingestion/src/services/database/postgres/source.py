from services.database.database_source import DatabaseSource
class PostgresSource(DatabaseSource):

    def getSourceConfig(self):
        from metadata.generated.schema.metadataIngestion.workflow import SourceConfig

        from metadata.generated.schema.metadataIngestion.databaseServiceMetadataPipeline import DatabaseServiceMetadataPipeline
        sourceConfig = SourceConfig(config=DatabaseServiceMetadataPipeline(**self.source_filter))

        #profile phy
        # from metadata.generated.schema.metadataIngestion.databaseServiceProfilerPipeline import DatabaseServiceProfilerPipeline
        # sourceConfig = SourceConfig(config=DatabaseServiceProfilerPipeline(**self.source_filter))

        return sourceConfig

    def getServiceConnection(self):
        from metadata.generated.schema.entity.services.databaseService import DatabaseConnection
        from metadata.generated.schema.entity.services.connections.database.postgresConnection import \
            PostgresConnection

        serviceConnection = DatabaseConnection(config=PostgresConnection(**{
            "type": "Postgres",
            "scheme": "postgresql+psycopg2",
            "username": self.source_user,
            "authType": {"password": self.source_password},
            "hostPort": self.source_hostport,
            "database": self.source_database,
        }))
        return serviceConnection
