from metadata.generated.schema.entity.services.searchService import SearchConnection
from metadata.generated.schema.entity.services.connections.search.elasticSearchConnection import (
    ElasticsearchConnection,
)
from metadata.generated.schema.metadataIngestion.workflow import SourceConfig
from metadata.generated.schema.metadataIngestion.searchServiceMetadataPipeline import (
    SearchServiceMetadataPipeline,
)
from metadata.ingestion.connections.builders import init_empty_connection_arguments
from metadata.ingestion.source.search.elasticsearch.jks_ssl import (
    METADATA_ES_JKS_CONNECTION_KEY,
)
from metadata.utils.logger import ingestion_logger

from services.search.search_source import SearchSource

logger = ingestion_logger()


class ElasticsearchSource(SearchSource):
    """Elasticsearch 메타데이터 수집용 래퍼 소스."""

    def getSourceConfig(self) -> SourceConfig:
        sourceConfig = SourceConfig(
            config=SearchServiceMetadataPipeline(**self.source_filter)
        )
        return sourceConfig

    def getServiceConnection(self) -> SearchConnection:
        protocol = (self.es_protocol or "http").strip().lower()
        if protocol == "http":
            return self._get_http_service_connection()
        if protocol == "https":
            return self._get_https_service_connection()
        logger.warning(
            "지원하지 않는 es_protocol=%r 입니다. http 로 연결합니다.",
            self.es_protocol,
        )
        return self._get_http_service_connection()

    def _host_port(self, scheme: str) -> str:
        port = self.source_port if self.source_port is not None else 9200
        host = self.source_host if self.source_host else "localhost"
        return f"{scheme}://{host}:{port}"

    def _build_elasticsearch_connection_config(self, scheme: str) -> dict:
        connection_config = {
            "type": "ElasticSearch",
            "hostPort": self._host_port(scheme),
        }
        if self.source_user and self.source_password:
            connection_config["authType"] = {
                "username": self.source_user,
                "password": self.source_password,
            }
        return connection_config

    def _get_http_service_connection(self) -> SearchConnection:
        return SearchConnection(
            config=ElasticsearchConnection(
                **self._build_elasticsearch_connection_config("http")
            )
        )

    def _get_https_service_connection(self) -> SearchConnection:
        cfg = self._build_elasticsearch_connection_config("https")
        if self.es_jks_path:
            args = init_empty_connection_arguments()
            args.__root__[METADATA_ES_JKS_CONNECTION_KEY] = {
                "keystorePath": self.es_jks_path,
                "password": self.es_ssl_password or "",
                "alias": "es-https",
            }
            cfg["connectionArguments"] = args
        return SearchConnection(config=ElasticsearchConnection(**cfg))
