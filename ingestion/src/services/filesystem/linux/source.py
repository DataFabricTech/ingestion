
from metadata.generated.schema.entity.services.filesystemService import FilesystemConnection
from metadata.generated.schema.metadataIngestion.workflow import SourceConfig
from services.filesystem.filesystem_source import FilesystemSource

class LinuxSource(FilesystemSource):

    def getSourceConfig(self) -> SourceConfig:
        from metadata.generated.schema.metadataIngestion.workflow import SourceConfig
        from metadata.generated.schema.metadataIngestion.filesystemServiceMetadataPipeline import FilesystemServiceMetadataPipeline
        sourceConfig = SourceConfig(config=FilesystemServiceMetadataPipeline(**self.source_filter))
        return sourceConfig


    def getServiceConnection(self) -> FilesystemConnection:
        from metadata.generated.schema.entity.services.filesystemService import FilesystemConnection
        from metadata.generated.schema.entity.services.connections.ssh.sshConnection import SshConnection

        serviceConnection = FilesystemConnection(config=SshConnection(**{
            "type": "Linux",
            "hostname": f"{self.source_host}:{self.source_port}",
            "username": self.source_user,
            "password": self.source_password
        }))

        return serviceConnection
