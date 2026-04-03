from metadata.generated.schema.entity.services.searchService import SearchConnection
from metadata.generated.schema.entity.services.connections.search.elasticSearchConnection import (
    ElasticsearchConnection,
)
from metadata.generated.schema.metadataIngestion.workflow import SourceConfig
from metadata.generated.schema.metadataIngestion.searchServiceMetadataPipeline import (
    SearchServiceMetadataPipeline,
)

from services.search.search_source import SearchSource


class ElasticsearchSource(SearchSource):
    """Elasticsearch 메타데이터 수집용 래퍼 소스."""

    def getSourceConfig(self) -> SourceConfig:
        sourceConfig = SourceConfig(
            config=SearchServiceMetadataPipeline(**self.source_filter)
        )
        return sourceConfig

    def getServiceConnection(self) -> SearchConnection:
        port = self.source_port if self.source_port is not None else 9200
        host = self.source_host if self.source_host else "localhost"
        host_port = f"http://{host}:{port}"

        connection_config = {
            "type": "ElasticSearch",
            "hostPort": host_port,
        }
        if self.source_user and self.source_password:
            connection_config["authType"] = {
                "username": self.source_user,
                "password": self.source_password,
            }

        serviceConnection = SearchConnection(
            config=ElasticsearchConnection(**connection_config)
        )
        return serviceConnection
