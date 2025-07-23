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
from typing import Optional

from metadata.generated.schema.metadataIngestion.storageServiceProfilerPipeline import StorageServiceProfilerPipeline
from metadata.profiler.api.models import ProfilerResponse
from metadata.utils.logger import profiler_logger

logger = profiler_logger()


class DocProfiler:
    """
    Document Profiler.
    """

    def __init__(self, source_config: StorageServiceProfilerPipeline, profiler_interface):
        self.source_config = source_config
        self.profiler_interface = profiler_interface

    def process(self) -> ProfilerResponse:

        if self.source_config.generateSampleData:
            sample_data = self.generate_sample_data()
        else:
            sample_data = None

        profile_response = ProfilerResponse(
            table=self.profiler_interface.table_entity,
            unstructured_sample_data=sample_data,
        )

        return profile_response

    def generate_sample_data(self) -> Optional[str]:
        """
        Fetch and ingest sample data
        """
        try:
            logger.debug(
                "Fetching sample data for "
                f"{self.profiler_interface.table_entity.fullyQualifiedName.__root__}..."  # type: ignore
            )
            sample_data = self.profiler_interface.fetch_sample_data(sample_count=self.source_config.sampleDataCount)
            return sample_data
        except Exception as err:
            logger.debug(traceback.format_exc())
            logger.warning(f"Error fetching sample data: {err}")
            return None

    def close(self):
        pass
