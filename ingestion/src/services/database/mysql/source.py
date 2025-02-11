from services.database.database_source import DatabaseSource
class MysqlSource(DatabaseSource):

    def getSourceConfig(self):
        from metadata.generated.schema.metadataIngestion.workflow import SourceConfig
        from metadata.generated.schema.metadataIngestion.databaseServiceMetadataPipeline import DatabaseServiceMetadataPipeline
        sourceConfig = SourceConfig(config=DatabaseServiceMetadataPipeline(**self.source_filter))
        return sourceConfig

    def getServiceConnection(self):
        from metadata.generated.schema.entity.services.databaseService import DatabaseConnection
        from metadata.generated.schema.entity.services.connections.database.mysqlConnection import \
            MysqlConnection

        serviceConnection = DatabaseConnection(config=MysqlConnection(**{
            "type": "Mysql",
            "scheme": "mysql+pymysql",
            "username": self.source_user,
            "authType": {"password": self.source_password},
            "hostPort": self.source_hostport,
        }))

        return serviceConnection
