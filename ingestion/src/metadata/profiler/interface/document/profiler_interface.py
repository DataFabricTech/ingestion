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

#  pylint: disable=arguments-differ
import os
import uuid
from typing import Any, Optional, Dict, List

from sqlalchemy import Column

from metadata.generated.schema.tests.customMetric import CustomMetric
from metadata.profiler.interface.profiler_interface import ProfilerInterface
from metadata.profiler.metrics.registry import Metrics
from metadata.profiler.processor.runner import QueryRunner
from metadata.readers.file.s3 import S3Reader
from metadata.utils.local_dir import ensure_directory_exists
from metadata.utils.logger import profiler_interface_registry_logger
from metadata.utils.word.hwp_extractor import HwpMetadataExtractor
from metadata.utils.word.ms_word_extractor import MsWordMetadataExtractor

logger = profiler_interface_registry_logger()


class DocumentProfilerInterface(ProfilerInterface):
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

    def _get_document_data(self, bucket_name: str, key: str) -> Optional[str]:
        """
        Read the word document from the bucket
        """
        local_dir_path = os.environ.get('AIRFLOW_HOME') + "/tmp"
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
        Fetch sample data from minio document(doc, hwp)
        """
        local_file_path = ""
        try:
            bucket_name = self.table_entity.fullPath.replace("s3://", "").split("/")[0]
            path = str(self.table_entity.prefix).strip('/')
            local_file_path = self._get_document_data(bucket_name, path)
            if local_file_path is None:
                return None
            file_extension = path.split('.')[-1]
            if file_extension == "hwp" or file_extension == "hwpx":
                return self.get_hwp_sample(local_file_path)
            elif file_extension == "docx" or file_extension == "doc":
                return self.get_word_sample(local_file_path)
            else:
                logger.warn("Unsupported file type")
                return None
        except Exception as e:
            logger.error(e)
        finally:
            os.remove(local_file_path)

    def get_hwp_sample(self, local_file_path):
        hwp_extractor = HwpMetadataExtractor(local_file_path)
        sample_data = hwp_extractor.get_sample_data(1000)
        return sample_data

    def get_word_sample(self, local_file_path):
        word_extractor = MsWordMetadataExtractor(local_file_path)
        sample_text = word_extractor.get_sample_data(1000)
        return sample_text

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
