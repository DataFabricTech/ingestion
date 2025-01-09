from abc import ABC, abstractmethod
from typing import Any, Iterable, List, Optional, Set, Tuple, Union

from metadata.generated.schema.api.data.createDirectory import CreateDirectoryRequest
from metadata.generated.schema.entity.data.directory import Directory

from metadata.generated.schema.entity.services.filesystemService import (
    FilesystemConnection,
    FilesystemService
)

from metadata.generated.schema.metadataIngestion.filesystemServiceMetadataPipeline import (
    FilesystemServiceMetadataPipeline
)

from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)

from metadata.ingestion.api.models import Either
from metadata.ingestion.api.steps import Source
from metadata.ingestion.api.topology_runner import TopologyRunnerMixin

from metadata.ingestion.models.topology import (
    NodeStage,
    ServiceTopology,
    TopologyContext,
    TopologyNode,
)

from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.connections import get_connection, get_test_connection_fn

from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()

class FilesystemServiceTopology(ServiceTopology):

    root = TopologyNode(
        producer="get_services",
        stages=[
            NodeStage(
                type_=FilesystemService,
                context="filesystem_service",
                processor="yield_create_request_filesystem_service",
                overwrite=False,
                must_return=True,
                cache_entities=True
            )
        ],
        children=["directory"]
    )

    directory = TopologyNode(
        producer="get_directory",
        stages=[
            NodeStage(
                type_=Directory,
                context="directory",
                processor="yield_create_directory_requests",
                consumer=["filesystem_service"],
                nullable=True,
                use_cache=True
            )
        ]
    )


class FilesystemServiceSource(TopologyRunnerMixin, Source, ABC):

    source_config: FilesystemServiceMetadataPipeline
    config: WorkflowSource
    metadata: OpenMetadata

    service_connection = FilesystemConnection.__fields__["config"].type_

    topology = FilesystemServiceTopology()
    context = TopologyContext.create(topology)

    filesystem_source_state: Set = set()

    def __init__(
            self,
            config: WorkflowSource,
            metadata: OpenMetadata,
    ):
        super().__init__()
        self.config = config
        self.metadata = metadata
        self.service_connection = self.config.serviceConnection.__root__.config
        self.source_config: FilesystemServiceMetadataPipeline = (
            self.config.sourceConfig.config
        )
        self.connection = get_connection(self.service_connection)

        # Flag the connection for the test connection
        self.connection_obj = self.connection
        # self.test_connection()

        # Try to get the global manifest
        # self.global_manifest: Optional[
        #     ManifestMetadataConfig
        # ] = self.get_manifest_file()

    def get_services(self) -> Iterable[WorkflowSource]:
        yield self.config

    def yield_create_request_filesystem_service(self, config: WorkflowSource):
        yield Either(
            right=self.metadata.get_create_service_from_source(
                entity=FilesystemService, config=config
            )
        )

    @abstractmethod
    def yield_create_directory_requests(
            self, container_details: Any
    ) -> Iterable[Either[CreateDirectoryRequest]]:
        """Generate the create container requests based on the received details"""


    @abstractmethod
    def get_directory(self) -> Iterable[Any]:
        """
            Retrieve all directory
        """

    def close(self):
        """By default, nothing needs to be closed"""

    def prepare(self):
        """By default, nothing needs to be taken care of when loading the source"""

    def test_connection(self) -> None:
        test_connection_fn = get_test_connection_fn(self.service_connection)
        test_connection_fn(self.metadata, self.connection_obj, self.service_connection)