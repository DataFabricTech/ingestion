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
Base source for the profiler used to instantiate a profiler runner with
its interface
"""
from copy import deepcopy
from typing import List, Optional, cast

from metadata.generated.schema.configuration.profilerConfiguration import (
    ProfilerConfiguration,
)
from metadata.generated.schema.entity.data.container import Container, FileFormat
from metadata.generated.schema.entity.data.table import ColumnProfilerConfig
from metadata.generated.schema.entity.services.storageService import StorageService, StorageConnection
from metadata.generated.schema.metadataIngestion.storageServiceProfilerPipeline import StorageServiceProfilerPipeline
from metadata.generated.schema.metadataIngestion.workflow import (
    OpenMetadataWorkflowConfig,
)
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.profiler.api.models import ProfilerProcessorConfig, TableConfig
from metadata.profiler.interface.document.profiler_interface import DocumentProfilerInterface
from metadata.profiler.interface.profiler_interface import ProfilerInterface
from metadata.profiler.metrics.registry import Metrics
from metadata.profiler.processor.core import Profiler
from metadata.profiler.processor.default import DefaultProfiler, get_default_metrics_for_storage_service
from metadata.profiler.processor.document_core import DocProfiler
from metadata.profiler.source.profiler_source_interface import ProfilerSourceInterface


class MinIOProfilerSource(ProfilerSourceInterface):
    """
    Base class for the profiler source
    """

    def __init__(
            self,
            config: OpenMetadataWorkflowConfig,
            ometa_client: OpenMetadata,
            global_profiler_configuration: ProfilerConfiguration,
    ):
        self.service_conn_config = self._copy_service_config(config)
        self.source_config = config.source.sourceConfig.config
        self.source_config = cast(
            StorageServiceProfilerPipeline, self.source_config
        )  # satisfy type checker
        self.profiler_config = ProfilerProcessorConfig.parse_obj(
            config.processor.dict().get("config")
        )
        self.ometa_client = ometa_client
        self.profiler_interface_type: str = config.source.serviceConnection.__root__.config.__class__.__name__
        # self.sqa_metadata = self._set_sqa_metadata()
        self._interface = None
        self.global_profiler_configuration = global_profiler_configuration

    def _copy_service_config(self, config: OpenMetadataWorkflowConfig) -> StorageConnection:
        """Make a copy of the service config and update the storage name

        Args:
            storage (_type_): a storage entity

        Returns:
            StorageService.__config__
        """
        config_copy = deepcopy(
            config.source.serviceConnection.__root__.config  # type: ignore
        )

        # we know we'll only be working with StorageConnection, we cast the type to satisfy type checker
        config_copy = cast(StorageConnection, config_copy)

        return config_copy


    @property
    def interface(
            self,
    ) -> Optional[ProfilerInterface]:
        """Get the interface"""
        return self._interface

    @interface.setter
    def interface(self, interface):
        """Set the interface"""
        self._interface = interface

    @staticmethod
    def get_config_for_container(entity: Container,
                                 profiler_config: ProfilerProcessorConfig) -> Optional[TableConfig]:
        """Get config for a specific entity

        Args:
            entity: container entity
        """
        for table_config in profiler_config.tableConfig or []:
            if (
                    table_config.fullyQualifiedName.__root__
                    == entity.fullyQualifiedName.__root__
            ):
                return table_config
        return None

    def _get_include_columns(
            self, entity: Container, entity_config: Optional[TableConfig]
    ) -> Optional[List[ColumnProfilerConfig]]:
        """get included columns"""
        if entity_config and entity_config.columnConfig:
            return entity_config.columnConfig.includeColumns

        if entity.tableProfilerConfig:
            return entity.tableProfilerConfig.includeColumns

        return None

    def _get_exclude_columns(
            self, entity: Container, entity_config: Optional[TableConfig]
    ) -> Optional[List[str]]:
        """get excluded columns"""
        if entity_config and entity_config.columnConfig:
            return entity_config.columnConfig.excludeColumns

        if entity.tableProfilerConfig:
            return entity.tableProfilerConfig.excludeColumns

        return None

    def create_profiler_interface(
            self,
            entity,
            config,
            profiler_config,
            schema_entity,
            database_entity,
            db_service,
    ) -> ProfilerInterface:
        pass

    def create_storage_profiler_interface(
            self,
            entity: Container,
            config: Optional[TableConfig],
            profiler_config: Optional[ProfilerProcessorConfig],
            storage_service: Optional[StorageService],
    ) -> ProfilerInterface:
        """Create sqlalchemy profiler interface"""
        from metadata.profiler.interface.profiler_interface_factory import (  # pylint: disable=import-outside-toplevel
            profiler_interface_factory,
        )

        profiler_interface: ProfilerInterface = profiler_interface_factory.create(
            self.profiler_interface_type,
            entity,
            None,                       # DatabaseSchema
            None,                       # Database
            storage_service,            # DatabaseService, StorageService
            config,                     # TableConfig
            profiler_config,            # ProfilerProcessorConfig
            self.source_config,         # DatabaseServiceProfilerPipeline, StorageServiceProfilerPipeline
            self.service_conn_config,   # Service Connection Config
            self.ometa_client           # Service Connection
        )  # type: ignore

        self.interface = profiler_interface
        return self.interface

    def create_document_profiler_interface(
            self,
            entity: Container,
            config: Optional[TableConfig],
            profiler_config: Optional[ProfilerProcessorConfig],
            storage_service: Optional[StorageService],
    ) -> ProfilerInterface:
        profiler_interface: ProfilerInterface = DocumentProfilerInterface.create(
            entity,
            None,  # DatabaseSchema
            None,  # Database
            storage_service,  # DatabaseService, StorageService
            config,  # TableConfig
            profiler_config,  # ProfilerProcessorConfig
            self.source_config,  # DatabaseServiceProfilerPipeline, StorageServiceProfilerPipeline
            self.service_conn_config,  # Service Connection Config
            self.ometa_client  # Service Connection
        )  # type: ignore

        self.interface = profiler_interface
        return self.interface

    def _get_context_entities(
            self, entity: Container
    ) -> StorageService:
        storage_service = None

        if entity.service:
            storage_service_list = self.ometa_client.es_search_from_fqn(
                entity_type=StorageService,
                fqn_search_string=entity.service.fullyQualifiedName,
            )
            if storage_service_list:
                storage_service = storage_service_list[0]

        return storage_service

    def get_profiler_runner(
            self, entity: Container, profiler_config: ProfilerProcessorConfig
    ) -> Profiler:
        """
        Returns the runner for the profiler
        """
        storage_service = self._get_context_entities(entity=entity)
        config = self.get_config_for_container(entity, profiler_config)

        # JBLIM : For MinIO Document Data
        if (entity.fileFormats is not None and
                entity.fileFormats[0] in [FileFormat.hwpx, FileFormat.hwp, FileFormat.doc, FileFormat.docx]):
            profiler_interface = self.create_document_profiler_interface(
                entity,
                config,
                profiler_config,
                storage_service,
            )
            return DocProfiler(
                source_config=self.source_config,
                profiler_interface=profiler_interface,
            )

        profiler_interface = self.create_storage_profiler_interface(
            entity,
            config,
            profiler_config,
            storage_service,
        )

        if not profiler_config.profiler:
            return DefaultProfiler(
                profiler_interface=profiler_interface,
                include_columns=self._get_include_columns(entity, config),
                exclude_columns=self._get_exclude_columns(entity, config),
                global_profiler_configuration=self.global_profiler_configuration,
            )

        metrics = (
            [Metrics.get(name) for name in profiler_config.profiler.metrics]
            if profiler_config.profiler.metrics
            else get_default_metrics_for_storage_service(
                table=profiler_interface.table,
                ometa_client=self.ometa_client,
                storage_service=storage_service,
            )
        )

        return Profiler(
            *metrics,  # type: ignore
            profiler_interface=profiler_interface,
            include_columns=self._get_include_columns(entity, config),
            exclude_columns=self._get_exclude_columns(entity, config),
            global_profiler_configuration=self.global_profiler_configuration,
        )
