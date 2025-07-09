import boto3

from metadata.generated.schema.entity.services.storageService import StorageConnection
from metadata.generated.schema.metadataIngestion.workflow import SourceConfig
from services.storage.storage_source import StorageSource
class MinioSource(StorageSource):

    def getSourceConfig(self) -> SourceConfig:
        from metadata.generated.schema.metadataIngestion.workflow import SourceConfig
        from metadata.generated.schema.metadataIngestion.storageServiceMetadataPipeline import StorageServiceMetadataPipeline
        sourceConfig = SourceConfig(config=StorageServiceMetadataPipeline(**self.source_filter))
        return sourceConfig

    def getServiceConnection(self) -> StorageConnection:
        from metadata.generated.schema.entity.services.storageService import StorageConnection
        from metadata.generated.schema.entity.services.connections.storage.minioConnection import MinioConnection

        serviceConnection = StorageConnection(config=MinioConnection(**{
            "type": "MinIO",
            "minioConfig": {
                "accessKeyId": self.source_user,
                "secretKey": self.source_password,
                "endPointURL": f"http://{self.source_host}:{self.source_port}"}
        }))

        return serviceConnection