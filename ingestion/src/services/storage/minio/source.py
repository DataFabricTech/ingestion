import json

from metadata.generated.schema.entity.services.storageService import StorageConnection
from metadata.generated.schema.metadataIngestion.workflow import SourceConfig
from services.storage.storage_source import StorageSource

class MinioSource(StorageSource):

    def getSourceConfig(self) -> SourceConfig:
        from metadata.generated.schema.metadataIngestion.workflow import SourceConfig

        filter_config = {k: v for k, v in self.source_filter.items() if k != 'classifications_rules'}

        if filter_config['type'] == 'StorageMetadata':
            from metadata.generated.schema.metadataIngestion.storageServiceMetadataPipeline import StorageServiceMetadataPipeline
            sourceConfig = SourceConfig(config=StorageServiceMetadataPipeline(**filter_config))
        elif filter_config['type'] == 'StorageProfiler':
            from metadata.generated.schema.metadataIngestion.storageServiceProfilerPipeline import StorageServiceProfilerPipeline
            sourceConfig = SourceConfig(config=StorageServiceProfilerPipeline(**filter_config))

        return sourceConfig

    def getServiceConnection(self) -> StorageConnection:
        from metadata.generated.schema.entity.services.storageService import StorageConnection
        from metadata.generated.schema.entity.services.connections.storage.minioConnection import MinioConnection
        from metadata.generated.schema.entity.services.connections.connectionBasicType import ConnectionArguments

        conn_args = {}
        classifications_rules = self.source_filter.get('classifications_rules')
        if classifications_rules:
            conn_args['classifications_rules'] = json.dumps(classifications_rules, ensure_ascii=False)

        serviceConnection = StorageConnection(config=MinioConnection(**{
            "type": "MinIO",
            "minioConfig": {
                "accessKeyId": self.source_user,
                "secretKey": self.source_password,
                "endPointURL": f"http://{self.source_host}:{self.source_port}"},
            "connectionArguments": ConnectionArguments(__root__=conn_args) if conn_args else None,
        }))

        return serviceConnection
