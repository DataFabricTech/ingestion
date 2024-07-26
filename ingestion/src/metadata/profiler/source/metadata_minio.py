#  Copyright 2021 Collate
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
OpenMetadata source for the profiler
"""
import traceback
from typing import Iterable, Optional, cast

from pydantic import BaseModel

from metadata.generated.schema.entity.data.container import Container, FileFormat
from metadata.generated.schema.entity.services.ingestionPipelines.status import (
    StackTraceError,
)
from metadata.generated.schema.entity.services.storageService import StorageService
from metadata.generated.schema.metadataIngestion.storageServiceProfilerPipeline import StorageServiceProfilerPipeline
from metadata.generated.schema.metadataIngestion.workflow import (
    OpenMetadataWorkflowConfig,
)
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.parser import parse_workflow_config_gracefully
from metadata.ingestion.api.step import Step
from metadata.ingestion.api.steps import Source
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.profiler.source.storage.minio.profiler_source import MinIOProfilerSource
from metadata.profiler.source.profiler_source_factory import profiler_source_factory
from metadata.utils import fqn
from metadata.utils.filters import filter_by_container
from metadata.utils.logger import profiler_logger

logger = profiler_logger()

#CONTAINER_FIELDS = ["tableProfilerConfig", "dataModel", "customMetrics"]
CONTAINER_FIELDS = ["dataModel"]
TAGS_FIELD = ["tags"]


class MinIoProfilerSourceAndEntity(BaseModel):
    """Return class for the Profiler Source"""

    class Config:
        arbitrary_types_allowed = True
        extra = "forbid"

    profiler_source: MinIOProfilerSource
    entity: Container

    def __str__(self):
        """Return the information of the table being profiler"""
        return f"Entity[Container] [{self.entity.name.__root__}]"


class MetadataSourceForMinio(Source):
    """
    This source lists and filters the entities that need
    to be processed by the profiler workflow.

    Note that in order to manage the following steps we need
    to test the connection against the Service Source.
    We do this here as well.
    """

    def init_steps(self):
        super().__init__()

    @property
    def name(self) -> str:
        return "MetadataSourceForMinio Service"

    # pylint: disable=super-init-not-called
    def __init__(
            self,
            config: OpenMetadataWorkflowConfig,
            metadata: OpenMetadata,
    ):
        self.init_steps()

        self.config = config
        self.metadata = metadata
        self.test_connection()

        self.source_config: StorageServiceProfilerPipeline = cast(
            StorageServiceProfilerPipeline, self.config.source.sourceConfig.config
        )

        # Used to satisfy type checked
        if not self._validate_service_name():
            raise ValueError(
                f"Service name `{self.config.source.serviceName}` does not exist. "
                "Make sure you have run the ingestion for the service specified in the profiler workflow. "
                "If so, make sure the profiler service name matches the service name specified during ingestion "
                "and that your ingestion token (settings > bots) is still valid."
            )

        logger.info(
            f"Starting profiler for service {self.config.source.serviceName}"
            f":{self.config.source.type.lower()}"
        )

    def _validate_service_name(self):
        """Validate service name exists in Server"""
        return self.metadata.get_by_name(
            entity=StorageService, fqn=self.config.source.serviceName
        )

    def prepare(self):
        """Nothing to prepare"""

    def test_connection(self) -> None:
        """
        Our source is the ometa client. Validate the
        health check before moving forward
        """
        self.metadata.health_check()

    def _iter(self, *_, **__) -> Iterable[Either[MinIoProfilerSourceAndEntity]]:
        global_profiler_config = self.metadata.get_profiler_config_settings()
        supported_file_formats = [
            FileFormat.csv,
            FileFormat.tsv,
            FileFormat.xls,
            FileFormat.xlsx
        ]
        for container in self.get_container_entities():
            # Skip unsupported file formats
            if any(format in container.fileFormats for format in supported_file_formats) is False:
                logger.info(f"Skipping container {container.name.__root__} due to unsupported file formats")
                continue

            try:
                profiler_source = profiler_source_factory.create(
                    self.config.source.type.lower(),
                    self.config,
                    self.metadata,
                    global_profiler_config
                )
                yield Either(
                    right=MinIoProfilerSourceAndEntity(
                        profiler_source=profiler_source,
                        entity=container,
                    )
                )
            except Exception as exc:
                yield Either(
                    left=StackTraceError(
                        name=container.fullyQualifiedName.__root__,
                        error=f"Error listing source and entities for container due to [{exc}]",
                        stackTrace=traceback.format_exc(),
                    )
                )

    @classmethod
    def create(
            cls,
            config_dict: dict,
            metadata: OpenMetadata,
            pipeline_name: Optional[str] = None,
    ) -> "Step":
        config = parse_workflow_config_gracefully(config_dict)
        return cls(config=config, metadata=metadata)

    def get_container_entities(self):
        """List all container in service"""
        containers = [
            self.filter_containers(container)
            for container in self.metadata.list_all_entities(
                entity=Container,
                fields=CONTAINER_FIELDS
                if not self.source_config.processPiiSensitive
                else CONTAINER_FIELDS + TAGS_FIELD,
                params={"service": self.config.source.serviceName},
            )
            if self.filter_containers(container)
        ]
        if not containers:
            raise ValueError(
                "FilterPattern returned 0 result. At least 1 container must be returned by the filter pattern."
                f"\n\t- includes: {self.source_config.containerFilterPattern.includes if self.source_config.containerFilterPattern else None}"  # pylint: disable=line-too-long
                f"\n\t- excludes: {self.source_config.containerFilterPattern.excludes if self.source_config.containerFilterPattern else None}"
                # pylint: disable=line-too-long
            )
        return containers

    def filter_containers(self, container: Container) -> Optional[Container]:
        """Returns filtered container entities"""
        container_fqn = fqn.build(
            self.metadata,
            entity_type=Container,
            service_name=self.config.source.serviceName,
            parent_container=container.parent,
            container_name=container.name.__root__,
        )
        if filter_by_container(
                self.source_config.containerFilterPattern,
                container_fqn
                if self.source_config.useFqnForFiltering
                else container.name.__root__,
        ):
            self.status.filter(container.name.__root__, "Name pattern not allowed")
            return None
        return container

    def close(self) -> None:
        """Nothing to close"""
