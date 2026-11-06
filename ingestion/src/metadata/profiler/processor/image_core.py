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


class ImageProfiler:
    """
    Image Profiler.
    """

    def __init__(self, source_config: StorageServiceProfilerPipeline, profiler_interface, detected_objects_model):
        self.source_config = source_config
        self.profiler_interface = profiler_interface
        self.detected_objects_model = detected_objects_model

    def process(self) -> ProfilerResponse:

        image_text = None
        resize_image = None
        detected_objects = None
        if self.source_config.generateSampleData:
            image_text, resize_image, detected_objects, detected_objects_image = self.generate_sample_data()

        profile_response = ProfilerResponse(
            table=self.profiler_interface.table_entity,
            image_sample_data=resize_image,
            image_text=image_text,
            image_detected_objects=detected_objects,
            image_detected_objects_image=detected_objects_image
        )

        return profile_response

    def generate_sample_data(self) -> Optional[str]:
        """
        Fetch and ingest sample data
        """
        image_text = None
        resize_image = None
        detected_objects = None
        detected_objects_image = None

        try:
            logger.debug(
                "Fetching sample data for "
                f"{self.profiler_interface.table_entity.fullyQualifiedName.__root__}..."  # type: ignore
            )

            image_text, resize_image, detected_objects, detected_objects_image = self.profiler_interface.fetch_sample_data(detected_objects_model=self.detected_objects_model)

        except Exception as err:
            logger.debug(traceback.format_exc())
            logger.warning(f"Error fetching sample data: {err}")

        return image_text, resize_image, detected_objects, detected_objects_image

    def close(self):
        pass
