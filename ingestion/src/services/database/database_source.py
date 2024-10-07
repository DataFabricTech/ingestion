from metadata.generated.schema.entity.services.databaseService import DatabaseServiceType
from metadata.generated.schema.metadataIngestion.workflow import Source
from metadata.generated.schema.metadataIngestion.workflow import SourceConfig
from metadata.generated.schema.entity.services.databaseService import DatabaseConnection

from services.common.source import CommonSource
class DatabaseSource(CommonSource):

    def _getSource(self) -> Source:
        serviceConnection = self.getServiceConnection()
        sourceConfig = self.getSourceConfig()
        serviceTypeStr = self.service_type.value if type(self.service_type) == DatabaseServiceType else 'Custom'
        serviceNameStr = self.service_type.value if type(self.service_type) == DatabaseServiceType else self.service_type

        source = Source(type=serviceTypeStr,
                        serviceName='-'.join(['Test', serviceNameStr]),  # 없으면 오류 발생
                        serviceConnection=serviceConnection,
                        sourceConfig=sourceConfig,
                        systemType=serviceNameStr)

        return source

    def getSourceConfig(self) -> SourceConfig:
        return None

    def getServiceConnection(self) -> DatabaseConnection:
        return None
