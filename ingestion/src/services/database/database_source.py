from src.metadata.generated.schema.entity.services.databaseService import DatabaseServiceType
from src.metadata.generated.schema.metadataIngestion.workflow import Source
from src.metadata.generated.schema.metadataIngestion.workflow import SourceConfig
from src.metadata.generated.schema.entity.services.databaseService import DatabaseConnection

from src.services.common.source import CommonSource
class DatabaseSource(CommonSource):

    def _getSource(self) -> Source:
        serviceConnection = self.getServiceConnection()
        sourceConfig = self.getSourceConfig()
        serviceTypeStr = self.service_type.value if type(self.service_type) == DatabaseServiceType else 'Custom'
        serviceNameStr = self.system_id

        source = Source(type=serviceTypeStr,
                        serviceName=serviceNameStr,  # 없으면 오류 발생
                        serviceConnection=serviceConnection,
                        sourceConfig=sourceConfig,
                        systemType=serviceTypeStr)

        return source

    def getSourceConfig(self) -> SourceConfig:
        return None

    def getServiceConnection(self) -> DatabaseConnection:
        return None
