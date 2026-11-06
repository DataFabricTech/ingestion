from metadata.generated.schema.entity.services.searchService import SearchServiceType
from metadata.generated.schema.metadataIngestion.workflow import Source
from metadata.generated.schema.metadataIngestion.workflow import SourceConfig
from metadata.generated.schema.entity.services.searchService import SearchConnection

from services.common.source import CommonSource


class SearchSource(CommonSource):

    def _getSource(self) -> Source:
        serviceConnection = self.getServiceConnection()
        sourceConfig = self.getSourceConfig()
        serviceTypeStr = (
            self.service_type.value
            if type(self.service_type) == SearchServiceType
            else "Custom"
        )
        serviceNameStr = self.system_id

        source = Source(
            type=serviceTypeStr,
            serviceName=serviceNameStr,
            serviceConnection=serviceConnection,
            sourceConfig=sourceConfig,
            systemType=serviceTypeStr,
        )
        return source

    def getSourceConfig(self) -> SourceConfig:
        return None

    def getServiceConnection(self) -> SearchConnection:
        return None
