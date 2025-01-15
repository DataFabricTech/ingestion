from metadata.generated.schema.entity.services.storageService import StorageServiceType
from metadata.generated.schema.metadataIngestion.workflow import Source
from metadata.generated.schema.metadataIngestion.workflow import SourceConfig
from metadata.generated.schema.entity.services.storageService import StorageConnection

from services.common.source import CommonSource
class StorageSource(CommonSource):

    def _getSource(self) -> Source:
        serviceConnection = self.getServiceConnection()
        sourceConfig = self.getSourceConfig()
        serviceTypeStr = self.service_type.value if type(self.service_type) == StorageServiceType else 'Custom'
        serviceNameStr = self.system_id

        source = Source(type=serviceTypeStr,
                        serviceName=serviceNameStr,  # 없으면 오류 발생
                        serviceConnection=serviceConnection,
                        sourceConfig=sourceConfig,
                        systemType=serviceTypeStr)

        return source

    def getSourceConfig(self) -> SourceConfig:
        return None

    def getServiceConnection(self) -> StorageConnection:
        return None