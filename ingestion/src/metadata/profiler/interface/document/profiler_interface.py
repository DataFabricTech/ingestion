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
from typing import Any, Optional, Dict, List, Tuple

from sqlalchemy import Column

from metadata.generated.schema.tests.customMetric import CustomMetric
from metadata.generated.schema.entity.data.container import FileFormat
from metadata.profiler.interface.profiler_interface import ProfilerInterface
from metadata.profiler.metrics.registry import Metrics
from metadata.profiler.processor.runner import QueryRunner
from metadata.readers.file.s3 import S3Reader
from metadata.utils.local_dir import ensure_directory_exists
from metadata.utils.logger import profiler_interface_registry_logger
from metadata.utils.word.hwp_extractor import HwpMetadataExtractor
from metadata.utils.word.ms_word_extractor import MsWordMetadataExtractor
from metadata.utils.word.txt_extractor import TxtMetadataExtractor
from metadata.utils.word.xml_extractor import XmlMetadataExtractor
from metadata.utils.word.json_extractor import JsonMetadataExtractor
from metadata.utils.image.pdf_extractor import PdfMetadataExtractor
from metadata.utils.s3_utils import get_normalized_key

from utils.process_config import config

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

    def fetch_sample_data(self, **kwargs) -> Tuple[Optional[str], Optional[str]]:
        """
        Fetch sample data from minio document
        """
        get_chunk_size = kwargs.get('chunk_size', 1000)
        local_file_path = ""
        try:
            bucket_name = self.table_entity.fullPath.replace("s3://", "").split("/")[0]
            data_path = str(self.table_entity.prefix).strip('/')
            normalized_key = get_normalized_key(client=self.client, bucket_name=bucket_name, key=data_path)

            if normalized_key is None:
                return None, None

            local_file_path = self._get_document_data(bucket_name, normalized_key)
            if local_file_path is None:
                return None, None

            sample_data = None
            sample_image_data =None
            # get sample data
            if self.table_entity.fileFormats[0] in [FileFormat.hwp, FileFormat.hwpx]:
                sample_data = self.get_hwp_sample(local_file_path, get_chunk_size)
            elif self.table_entity.fileFormats[0] in [FileFormat.docx]:
                sample_data = self.get_ms_sample(local_file_path, get_chunk_size)
            elif self.table_entity.fileFormats[0] in [FileFormat.txt]:
                sample_data = self.get_txt_sample(local_file_path, get_chunk_size)
            elif self.table_entity.fileFormats[0] in [FileFormat.xml]:
                sample_data = self.get_xml_sample(local_file_path, get_chunk_size)
            elif self.table_entity.fileFormats[0] in [FileFormat.json]:
                sample_data = self.get_json_sample(local_file_path, get_chunk_size)

            # get sample image data
            if self.table_entity.fileFormats[0] in [FileFormat.docx, FileFormat.doc,
                                                    FileFormat.txt,
                                                    FileFormat.xml,
                                                    FileFormat.json]:
                sample_image_data = self.get_word_sample_image(local_file_path)

            if sample_data is None and sample_image_data is None:
                logger.warn("Unsupported file type")

            return sample_data, sample_image_data

        except Exception as e:
            logger.error(e)
        finally:
            os.remove(local_file_path)

    def get_hwp_sample(self, local_file_path, chunk_size=1000):
        hwp_extractor = HwpMetadataExtractor()
        sample_data = hwp_extractor.get_sample_data(local_file_path, chunk_size)
        return sample_data

    def get_ms_sample(self, local_file_path, chunk_size=1000):
        extractor = MsWordMetadataExtractor()
        sample_text = extractor.get_sample_data(local_file_path, chunk_size)
        return sample_text

    def get_txt_sample(self, local_file_path, chunk_size=1000):
        extractor = TxtMetadataExtractor()
        sample_text = extractor.get_sample_data(local_file_path, chunk_size)
        return sample_text

    def get_xml_sample(self, local_file_path, chunk_size=1000):
        extractor = XmlMetadataExtractor()
        sample_text = extractor.get_sample_data(local_file_path, chunk_size)
        return sample_text

    def get_word_sample_image(self, local_file_path):
        extractor = PdfMetadataExtractor(local_file_path)
        pdf_path = ""
        try:
            pdf_path = extractor.convert_to_pdf()
            sample_text = extractor.get_sample_data(pdf_path)
            return sample_text
        except Exception as e:
            logger.error(e)
            return None
        finally:
            os.remove(pdf_path)

    def get_json_sample(self, local_file_path, chunk_size=1000):
        extractor = JsonMetadataExtractor()
        sample_text = extractor.get_sample_data(local_file_path, chunk_size)
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
