# Copyright 2024 Mobigen
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Notice!
# This software is based on https://open-metadata.org and has been modified accordingly.


"""
Processor util to fetch pii sensitive columns
"""
import traceback
from typing import List, Optional

from metadata.generated.schema.entity.data.container import FileFormat, Container
from metadata.generated.schema.entity.data.table import Column, TableData
from metadata.generated.schema.entity.services.ingestionPipelines.status import (
    StackTraceError,
)
from metadata.generated.schema.metadataIngestion.storageServiceProfilerPipeline import ProfilerConfigType
from metadata.generated.schema.metadataIngestion.workflow import (
    MetadataWorkflowConfig,
)
from metadata.generated.schema.type.tagLabel import (
    LabelType,
    State,
    TagLabel,
    TagSource, TagFQN,
)
from metadata.glossary.matcher.glossary_terms_matcher import GlossaryTermsMatcher
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.parser import parse_workflow_config_gracefully
from metadata.ingestion.api.step import Step
from metadata.ingestion.api.steps import Processor
from metadata.ingestion.models.table_metadata import ColumnTag
from metadata.ingestion.server.server_api import ServerInterface
from metadata.profiler.api.models import ProfilerResponse
from metadata.utils.logger import profiler_logger

logger = profiler_logger()


class GlossaryProcessorForDatabaseService(Processor):
    """
    데이터 사전을 이용해 메타데이터(프로파일링) 처리
    """

    def __init__(
            self,
            config: MetadataWorkflowConfig,
            metadata: ServerInterface,
    ):
        logger.info("Glossary Processor Initialized")
        super().__init__()
        self.config = config
        self.metadata = metadata

        logger.info("Loading Glossary Terms")
        self.glossary_matcher = GlossaryTermsMatcher(metadata)

    @property
    def name(self) -> str:
        return "Glossary Processor"

    @classmethod
    def create(
            cls,
            config_dict: dict,
            metadata: ServerInterface,
            pipeline_name: Optional[str] = None,
    ) -> "Step":
        config = parse_workflow_config_gracefully(config_dict)
        return cls(config=config, metadata=metadata)

    def close(self) -> None:
        """Nothing to close"""
        # self.glossary_matcher = None

    @staticmethod
    def build_column_tag(tag_fqn: str, column_fqn: str) -> ColumnTag:
        """
        Build the tag and run the PATCH
        """
        tag_label = TagLabel(
            tagFQN=TagFQN(__root__=tag_fqn),
            source=TagSource.Glossary,
            state=State.Suggested,
            labelType=LabelType.Automated,
        )

        return ColumnTag(column_fqn=column_fqn, tag_label=tag_label)

    def process_column(
            self,
            idx: int,
            column: Column,
            table_data: Optional[TableData],
    ) -> Optional[List[ColumnTag]]:
        """
        테이블 이름과 컬럼 이름을 데이터 사전에서 검색하여 사전 태그 추가
        """

        # glossary terms 정보가 이미 있는 경우
        # column_has_glossary = False
        # for tag in column.tags:
        #     if tag.source.value == TagSource.Glossary.value:
        #         column_has_glossary = True
        #         break
        # if column_has_glossary is True:
        #     return None

        # Glossary Terms - column name.
        terms = self.glossary_matcher.match(column.name.__root__)
        if terms is not None:
            column_tags = []
            for term in terms:
                is_existing_tag = False
                if column.tags is not None:
                    for tag in column.tags:
                        if tag.tagFQN.__root__ == term.__root__:
                            is_existing_tag = True
                            break
                if is_existing_tag:
                    continue
                tag_fqn = term.__root__
                column_fqn = column.fullyQualifiedName.__root__
                column_tag = self.build_column_tag(tag_fqn, column_fqn)
                column_tags.append(column_tag)
            return column_tags

        return None

    def _run(
            self,
            record: ProfilerResponse,
    ) -> Either[ProfilerResponse]:
        """
        Main entrypoint for the glossary.

        데이터 사전 정보를 이용해 테이블, 컬럼 정보를 업데이트.
        """

        logger.info("Glossary Processor")

        column_tags = []

        #  JBLIM : 문서 파일을 위한 데이터 사전 기능은 아직..
        if isinstance(record.table, Container):
            logger.info("For Storage(Container)")
            if (record.table.fileFormats is not None and len(record.table.fileFormats) > 0 and
                    record.table.fileFormats[0] in [FileFormat.docx, FileFormat.doc, FileFormat.hwpx, FileFormat.hwp]):
                logger.info("Unstructured File Format(Document) Pass...")
                return Either(right=record)

        if self.config.source.sourceConfig.config.type == ProfilerConfigType.StorageProfiler:
            logger.info("Structured File Format")
            columns = record.table.dataModel.columns
        else:
            logger.info("Database")
            columns = record.table.columns

        for idx, column in enumerate(columns):
            try:
                col_tags = self.process_column(
                    idx=idx,
                    column=column,
                    table_data=record.sample_data,
                )
                if col_tags:
                    column_tags.extend(col_tags)
            except Exception as err:
                self.status.failed(
                    StackTraceError(
                        name=record.table.fullyQualifiedName.__root__,
                        error=f"Error computing PII tags for [{column}] - [{err}]",
                        stackTrace=traceback.format_exc(),
                    )
                )

        record.column_tags = column_tags
        return Either(right=record)
