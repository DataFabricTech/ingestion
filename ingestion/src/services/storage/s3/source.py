import boto3

from src.metadata.generated.schema.entity.services.storageService import StorageConnection
from src.metadata.generated.schema.metadataIngestion.workflow import SourceConfig
from src.services.storage.storage_source import StorageSource
class S3Source(StorageSource):

    def getSourceConfig(self) -> SourceConfig:
        from metadata.generated.schema.metadataIngestion.workflow import SourceConfig
        from metadata.generated.schema.metadataIngestion.storageServiceMetadataPipeline import StorageServiceMetadataPipeline
        sourceConfig = SourceConfig(config=StorageServiceMetadataPipeline(**self.source_filter))
        return sourceConfig

    def getServiceConnection(self) -> StorageConnection:
        from metadata.generated.schema.entity.services.storageService import StorageConnection
        from metadata.generated.schema.entity.services.connections.storage.s3Connection import S3Connection

        serviceConnection = StorageConnection(config=S3Connection(**{
            "type": "S3",
            "awsConfig": {
                "awsAccessKeyId": self.source_user,
                "awsSecretAccessKey": self.source_password,
                "awsRegion": "ap-northeast-2",
                # "awsRegion": None,
                "endPointURL": "http://<INTERNAL_HOST>:9002"
            }
        }))

        return serviceConnection