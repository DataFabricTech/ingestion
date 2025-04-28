from services.database.database_source import DatabaseSource
class TiberoSource(DatabaseSource):

    def getSourceConfig(self):
        from metadata.generated.schema.metadataIngestion.workflow import SourceConfig
        from metadata.generated.schema.metadataIngestion.databaseServiceMetadataPipeline import DatabaseServiceMetadataPipeline
        sourceConfig = SourceConfig(config=DatabaseServiceMetadataPipeline(**self.source_filter))
        return sourceConfig

    def getServiceConnection(self):
        from metadata.generated.schema.entity.services.databaseService import DatabaseConnection
        from metadata.generated.schema.entity.services.connections.database.customDatabaseConnection import \
            CustomDatabaseConnection

        serviceConnection = DatabaseConnection(config=CustomDatabaseConnection(**{
            "type": "CustomDatabase",
            "sourcePythonClass": "metadata.ingestion.source.database.custom.tibero.metadata.TiberoSource",
            "connectionOptions": {
                "scheme": "tibero+pyodbc",
                "username": self.source_user,
                "password": self.source_password,
                "odbcDsnName": self.system_id.replace("-",""),
                "supportsProfiler": False,
                "supportsDBTExtraction": True,
                "supportsMetadataExtraction": True
            },
        }))
        return serviceConnection
