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
#  pylint: disable=arguments-differ
import os
import uuid
from typing import Any, Optional, Dict, List

from sqlalchemy import Column

from metadata.generated.schema.tests.customMetric import CustomMetric
from metadata.generated.schema.entity.data.container import FileFormat
from metadata.profiler.interface.profiler_interface import ProfilerInterface
from metadata.profiler.metrics.registry import Metrics
from metadata.profiler.processor.runner import QueryRunner
from metadata.readers.file.s3 import S3Reader
from metadata.utils.local_dir import ensure_directory_exists
from metadata.utils.logger import profiler_interface_registry_logger
from metadata.utils.s3_utils import get_normalized_key
from metadata.utils.image.pil_extractor import PilMetadataExtractor
from metadata.utils.image.pdf_extractor import PdfMetadataExtractor
from metadata.utils.image.yolo_detector import YoloDetector

from utils.process_config import config

logger = profiler_interface_registry_logger()


class ImageProfilerInterface(ProfilerInterface):
    """
    Interface to interact with registry supporting
    sqlalchemy.
    """

    # pylint: disable=too-many-arguments
    def __init__(
            self,
            service_connection_config,
            ometa_client,
            entity,
            storage_config,
            profile_sample_config,
            source_config,
            sample_query,
            table_partition_config,
            thread_count: int = 5,
            timeout_seconds: int = 43200,
            sample_data_count: int = 1000,
            **kwargs,
    ):
        """Instantiate Pandas Interface object"""

        super().__init__(
            service_connection_config,
            ometa_client,
            entity,
            storage_config,
            profile_sample_config,
            source_config,
            sample_query,
            table_partition_config,
            thread_count,
            timeout_seconds,
            sample_data_count,
            **kwargs,
        )
        self.client = self.connection.client
        # self.sample_data = self.minio_doc_sample(
        #     service_connection_config=self.service_connection_config,
        #     client=self.client,
        #     container=self.table_entity,
        #     profile_sample_config = profile_sample_config,
        # )

    def _get_file(self, bucket_name: str, key: str) -> Optional[str]:
        """
        Read the file from the bucket
        """
        local_dir_path = config.document_tmp_dir
        # 다운로드 디렉토리 확인 및 생성
        ensure_directory_exists(local_dir_path)

        file_extension = key.split('.')[-1]
        local_file_path = f"{local_dir_path}/{uuid.uuid4().hex}.{file_extension}"

        try:
            s3reader = S3Reader(self.client)
            s3reader.download(key, local_file_path=local_file_path, bucket_name=bucket_name, verbose=True)
            return local_file_path
        except Exception as e:
            logger.error(e)
            return None

    def fetch_sample_data(self, **kwargs) -> Optional[str]:
        """
        Fetch sample data from minio - image
        """
        local_file_path = ""
        detected_objects_model = kwargs.get('detected_objects_model')
        try:
            bucket_name = self.table_entity.fullPath.replace("s3://", "").split("/")[0]
            data_path = str(self.table_entity.prefix).strip('/')
            normalized_key = get_normalized_key(client=self.client, bucket_name=bucket_name, key=data_path)

            logger.debug("get_file start")
            local_file_path = self._get_file(bucket_name, normalized_key)
            logger.debug("get_file end")
            if local_file_path is None:
                return None, None, None, None

            image_text = None
            resize_image = None
            detected_objects = None
            if self.table_entity.fileFormats[0] in [FileFormat.jpeg, FileFormat.jpg, FileFormat.png,
                                                    FileFormat.bmp, FileFormat.gif, FileFormat.webp,
                                                    FileFormat.cr2, FileFormat.nef, FileFormat.tiff, FileFormat.tif]:
                image_text, resize_image = self.get_pil_sample(local_file_path)

            elif self.table_entity.fileFormats[0] in [FileFormat.pdf]:
                resize_image = self.get_pdf_sample(local_file_path)

            else:
                logger.warn("Unsupported file type")

            # 이미지 객체 탐지
            try:
                detected_objects, detected_objects_image = self._get_detected_objects(local_file_path, detected_objects_model)
            except Exception as e:
                logger.debug(f"image file detected fail [{e}], file {local_file_path}")


            return image_text, resize_image, detected_objects, detected_objects_image
        except Exception as e:
            logger.error(e)
            raise e
        finally:
            if len(local_file_path) > 0:
                os.remove(local_file_path)

    def get_pil_sample(self, local_file_path):
        extractor = PilMetadataExtractor(local_file_path)
        image_text, resize_image = extractor.get_sample_data()
        return image_text, resize_image

    def get_pdf_sample(self, local_file_path):
        extractor = PdfMetadataExtractor(local_file_path)
        sample_text = extractor.get_sample_data(local_file_path)
        return sample_text

    def _get_detected_objects(self, local_file_path: str, detected_objects_model) -> Optional[List]:
        """
        이미지 객체 탐지
        """
        detector = YoloDetector(local_file_path, detected_objects_model)
        logger.debug("detected start")
        detected_objects, detected_objects_image = detector.detected_objects()
        logger.debug("detected end")
        return detected_objects, detected_objects_image

    def _get_sampler(self):
        pass

    @property
    def table(self):
        pass

    def _compute_table_metrics(self, metrics: List[Metrics], runner, *args, **kwargs):
        pass

    def _compute_static_metrics(self, metrics: List[Metrics], runner, *args, **kwargs) -> Dict[str, Any]:
        pass

    def _compute_query_metrics(self, metric: Metrics, runner, *args, **kwargs):
        pass

    def _compute_window_metrics(self, metrics: List[Metrics], runner: QueryRunner, *args, **kwargs):
        pass

    def _compute_system_metrics(self, metrics: Metrics, runner, *args, **kwargs):
        pass

    def _compute_custom_metrics(self, metrics: List[CustomMetric], runner, *args, **kwargs):
        pass

    def get_all_metrics(self, metric_funcs) -> dict:
        pass

    def get_composed_metrics(self, column: Column, metric: Metrics, column_results: Dict) -> dict:
        pass

    def get_hybrid_metrics(self, column: Column, metric: Metrics, column_results: Dict, **kwargs) -> dict:
        pass

    def close(self):
        pass

    def get_columns(self):
        pass
