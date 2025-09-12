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
Main Profile definition and queries to execute
"""
from __future__ import annotations

import traceback
from typing import Optional, Tuple

from metadata.generated.schema.metadataIngestion.storageServiceProfilerPipeline import StorageServiceProfilerPipeline
from metadata.generated.schema.entity.data.container import FileFormat
from metadata.profiler.api.models import ProfilerResponse
from metadata.utils.logger import profiler_logger

from metadata.ml.summarization import Summarization

logger = profiler_logger()


class DocProfiler:
    """
    Document Profiler.
    """

    def __init__(self, source_config: StorageServiceProfilerPipeline, profiler_interface, file_format: FileFormat):
        self.summarizer = Summarization()
        self.source_config = source_config
        self.profiler_interface = profiler_interface
        self.file_format = file_format

    def process(self) -> ProfilerResponse:

        # sample : 이미지 변환, summary
        if self.source_config.generateSampleData:
            sample_data, sample_image_data = self.generate_sample_data()

            if sample_data is not None:
                ############## sample_data는 1000자 일테니 summary용 데이터를 가지고와야할듯.
                str_summary = self.summarizer.summarize(sample_data)
            else:
                str_summary = None
        else:
            sample_data = None
            str_summary = None

        profile_response = ProfilerResponse(
            table=self.profiler_interface.table_entity,
            unstructured_sample_data=sample_data,
            unstructured_summary=str_summary,
            image_sample_data=sample_image_data
        )

        return profile_response

    def generate_sample_data(self) -> Tuple[Optional[str], Optional[str]]:
        """
        Fetch and ingest sample data
        """
        try:
            logger.debug(
                "Fetching sample data for "
                f"{self.profiler_interface.table_entity.fullyQualifiedName.__root__}..."  # type: ignore
            )
            sample_data, sample_image_data = self.profiler_interface.fetch_sample_data(get_chunk_size=-1)
            return sample_data, sample_image_data
        except Exception as err:
            logger.debug(traceback.format_exc())
            logger.warning(f"Error fetching sample data: {err}")
            return None

    def close(self):
        pass
