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
Workflow definition for the profiler
"""
from metadata.generated.schema.metadataIngestion.workflow import (
    OpenMetadataWorkflowConfig,
)
from metadata.glossary.processor import GlossaryProcessorForDatabaseService
from metadata.ingestion.api.steps import Processor, Sink
from metadata.ingestion.source.connections import get_connection, get_test_connection_fn
from metadata.pii.processor import PIIProcessor
from metadata.profiler.processor.processor import ProfilerProcessor
from metadata.profiler.processor.minio_processor import MinioProfilerProcessor
from metadata.profiler.source.metadata import OpenMetadataSource
from metadata.profiler.source.metadata_ext import OpenMetadataSourceExt
from metadata.profiler.source.metadata_minio import MetadataSourceForMinio
from metadata.utils.importer import import_sink_class
from metadata.utils.logger import profiler_logger
from metadata.workflow.ingestion import IngestionWorkflow

logger = profiler_logger()


class ProfilerWorkflow(IngestionWorkflow):
    """
    Profiler ingestion workflow implementation

    We check the source connection test when initializing
    this workflow. No need to do anything here if this does not pass
    """

    def __init__(self, config: OpenMetadataWorkflowConfig):
        super().__init__(config)

        # Validate that we can properly reach the source database
        self.test_connection()

    def _get_source_class(self):
        # jblim : ProfilerType 을 이용해 MinIO 데이터를 처리할 수 있는 SourceClass 를 가져올 수 있도록 함.
        if self.config.source.sourceConfig.config.type.value == "StorageProfiler":
            return MetadataSourceForMinio
        if self.config.source.serviceName:
            return OpenMetadataSource
        logger.info(
            "Database Service name not provided, we will scan all the tables"
            "available within data source and locate table entity in OpenMetadata"
            "to ingest profiler data."
        )
        return OpenMetadataSourceExt

    def set_steps(self):
        source_class = self._get_source_class()
        self.source = source_class.create(self.config.dict(), self.metadata)

        profiler_processor = self._get_profiler_processor()
        pii_processor = self._get_pii_processor()
        glossary_processor = self._get_glossary_processor()
        sink = self._get_sink()
        self.steps = (profiler_processor, pii_processor, glossary_processor, sink)
        # self.steps = (profiler_processor, pii_processor, sink)

    def test_connection(self):
        service_config = self.config.source.serviceConnection.__root__.config
        conn = get_connection(service_config)

        test_connection_fn = get_test_connection_fn(service_config)
        test_connection_fn(self.metadata, conn, service_config)

    def _get_sink(self) -> Sink:
        sink_type = self.config.sink.type
        sink_class = import_sink_class(sink_type=sink_type)
        sink_config = self.config.sink.dict().get("config", {})
        sink: Sink = sink_class.create(sink_config, self.metadata)
        logger.debug(f"Sink type:{self.config.sink.type}, {sink_class} configured")

        return sink

    def _get_profiler_processor(self) -> Processor:
        if self.config.source.sourceConfig.config.type.value == "StorageProfiler":
            return MinioProfilerProcessor.create(self.config.dict(), self.metadata)
        return ProfilerProcessor.create(self.config.dict(), self.metadata)

    def _get_pii_processor(self) -> Processor:
        return PIIProcessor.create(self.config.dict(), self.metadata)

    def _get_glossary_processor(self) -> Processor:
        return GlossaryProcessorForDatabaseService.create(self.config.dict(), self.metadata)
