from metadata.generated.schema.entity.services.filesystemService import (
    FilesystemServiceType,
    FilesystemConnection
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source,
    SourceConfig
)

from src.services.common.source import CommonSource

class FilesystemSource(CommonSource):

    def _getSource(self) -> Source:
        serviceConnection = self.getServiceConnection()
        sourceConfig = self.getSourceConfig()
        serviceTypeStr = self.service_type.value if type(self.service_type) == FilesystemServiceType else 'Custom'
        serviceNameStr = self.system_id

        source = Source(type=serviceTypeStr,
                        serviceName=serviceNameStr,  # 없으면 오류 발생
                        serviceConnection=serviceConnection,
                        sourceConfig=sourceConfig,
                        systemType=serviceTypeStr)

        return source

    def getSourceConfig(self) -> SourceConfig:
        return None

    def getServiceConnection(self) -> FilesystemConnection:
        return None