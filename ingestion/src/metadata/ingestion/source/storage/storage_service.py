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
Base class for ingesting Object Storage services
"""
import os
import uuid
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Iterable, List, Optional, Set

from metadata.generated.schema.api.data.createContainer import CreateContainerRequest
from metadata.generated.schema.entity.data.container import Container, Rdf, FileFormat
from metadata.generated.schema.entity.data.file import File
from metadata.generated.schema.entity.services.storageService import (
    StorageConnection,
    StorageService,
)
from metadata.generated.schema.metadataIngestion.storage.containerMetadataConfig import (
    MetadataEntry,
)
from metadata.generated.schema.metadataIngestion.storage.manifestMetadataConfig import (
    ManifestMetadataConfig,
)
from metadata.generated.schema.metadataIngestion.storageServiceMetadataPipeline import (
    NoMetadataConfigurationSource,
    StorageServiceMetadataPipeline,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.ingestion.api.delete import delete_entity_from_source
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.steps import Source
from metadata.ingestion.api.topology_runner import TopologyRunnerMixin
from metadata.ingestion.models.delete_entity import DeleteEntity
from metadata.ingestion.models.topology import (
    NodeStage,
    ServiceTopology,
    TopologyContextManager,
    TopologyNode,
)
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.connections import get_connection, get_test_connection_fn
from metadata.ingestion.source.database.glue.models import Column
from metadata.readers.dataframe.models import DatalakeTableSchemaWrapper
from metadata.readers.dataframe.reader_factory import SupportedTypes
from metadata.readers.file.s3 import S3Reader
from metadata.readers.models import ConfigSource
from metadata.utils import fqn
from metadata.utils.datalake.datalake_utils import (
    DataFrameColumnParser,
    fetch_dataframe,
)
from metadata.utils.local_dir import ensure_directory_exists
from metadata.utils.logger import ingestion_logger
from metadata.utils.storage_metadata_config import (
    StorageMetadataConfigException,
    get_manifest,
)
from metadata.utils.word.ms_word_extractor import MsWordMetadataExtractor
from metadata.utils.word.hwp_extractor import HwpMetadataExtractor
from metadata.utils.word.txt_extractor import TxtMetadataExtractor
from metadata.utils.word.json_extractor import JsonMetadataExtractor
from metadata.utils.word.xml_extractor import XmlMetadataExtractor

from metadata.utils.image.pil_extractor import PilMetadataExtractor
from metadata.utils.image.pdf_extractor import PdfMetadataExtractor
from metadata.utils.image.exifread_extractor import ExifreadMetadataExtractor
from metadata.utils.image.psd_extractor import PsdMetadataExtractor
from metadata.utils.image.imageio_extractor import ImageioMetadataExtractor
from metadata.utils.image.exr_extractor import ExrMetadataExtractor

from metadata.utils.image.yolo_detector import YoloDetector

from utils.process_config import config

logger = ingestion_logger()

KEY_SEPARATOR = "/"
OPENMETADATA_TEMPLATE_FILE_NAME = "openmetadata.json"


class Metric(Enum):
    LAST_MODIFIED = "LastModified"
    NUMBER_OF_OBJECTS = "NumberOfObjects"
    BUCKET_SIZE_BYTES = "BucketSizeBytes"


class StorageServiceTopology(ServiceTopology):
    root = TopologyNode(
        producer="get_services",
        stages=[
            NodeStage(
                type_=StorageService,
                context="objectstore_service",
                processor="yield_create_request_objectstore_service",
                overwrite=False,
                must_return=True,
                cache_entities=True,
            ),
        ],
        # children=["container"],
        children=["bucket"],
        # post_process=["mark_containers_as_deleted"]
    )

    bucket = TopologyNode(
        producer="get_buckets",
        stages=[
            NodeStage(
                type_=Container,
                context="bucket",
                processor="yield_create_container_requests",
                consumer=["objectstore_service"],
                cache_entities=True,
                use_cache=True,
            )
        ],
        children=["directory"]
    )

    directory = TopologyNode(
        producer="get_directories",
        stages=[
            NodeStage(
                type_=Container,
                context="directory",
                processor="yield_create_directory_requests",
                consumer=["objectstore_service", "bucket"],
                cache_entities=True,
                use_cache=True,
                nullable=True,
            )
        ],
        children=["container"]
    )

    container = TopologyNode(
        producer="get_containers",
        stages=[
            NodeStage(
                type_=File,
                context="file",
                processor="yield_create_file_requests",
                # consumer=["objectstore_service"],
                consumer=["objectstore_service", "bucket", "directory"],
                nullable=True,
                use_cache=True,
            )
        ],
    )


def rdfs_delete_duplicated(rdfs: List[Rdf]) -> List[Rdf]:
    """
    Remove duplicated rdf
    """
    result = []
    for rdf in rdfs:
        if rdf not in result:
            result.append(rdf)
    return result


class StorageServiceSource(TopologyRunnerMixin, Source, ABC):
    """
    Base class for Object Store Services.
    It implements the topology and context.
    """

    source_config: StorageServiceMetadataPipeline
    config: WorkflowSource
    metadata: OpenMetadata
    # Big union of types we want to fetch dynamically
    service_connection: StorageConnection.__fields__["config"].type_

    topology = StorageServiceTopology()
    context = TopologyContextManager(topology)
    container_source_state: Set = set()

    global_manifest: Optional[ManifestMetadataConfig]

    def __init__(
            self,
            config: WorkflowSource,
            metadata: OpenMetadata,
    ):
        super().__init__()
        self.config = config
        self.metadata = metadata
        self.service_connection = self.config.serviceConnection.__root__.config
        self.source_config: StorageServiceMetadataPipeline = (
            self.config.sourceConfig.config
        )
        self.connection = get_connection(self.service_connection)

        # Flag the connection for the test connection
        self.connection_obj = self.connection
        # phy
        # self.test_connection()

        # Try to get the global manifest
        self.global_manifest: Optional[
            ManifestMetadataConfig
        ] = self.get_manifest_file()

        # extractor 재사용을 위한 맵
        self.metadata_extractor = {}

    @property
    def name(self) -> str:
        return self.service_connection.type.name

    def get_manifest_file(self) -> Optional[ManifestMetadataConfig]:
        if self.source_config.storageMetadataConfigSource and not isinstance(
                self.source_config.storageMetadataConfigSource,
                NoMetadataConfigurationSource,
        ):
            try:
                return get_manifest(self.source_config.storageMetadataConfigSource)
            except StorageMetadataConfigException as exc:
                logger.warning(f"Could no get global manifest due to [{exc}]")
        return None

    @abstractmethod
    def get_containers(self) -> Iterable[Any]:
        """
        Retrieve all containers for the service
        """

    @abstractmethod
    def yield_create_container_requests(
            self, container_details: Any
    ) -> Iterable[Either[CreateContainerRequest]]:
        """Generate the create container requests based on the received details"""

    def close(self):
        """By default, nothing needs to be closed"""

    def get_services(self) -> Iterable[WorkflowSource]:
        yield self.config

    def prepare(self):
        """By default, nothing needs to be taken care of when loading the source"""

    def register_record(self, container_request: CreateContainerRequest) -> None:
        """
        Mark the container record as scanned and update
        the storage_source_state
        """
        if container_request.parent and container_request.parent.id:
            parent = self.metadata.get_by_id(
                entity=Container, entity_id=container_request.parent.id
            )
            parent_container = parent.fullyQualifiedName
        else:
            parent_container = None
        # parent_container = (
        #     self.metadata.get_by_id(
        #         entity=Container, entity_id=container_request.parent.id
        #     ).fullyQualifiedName.__root__
        #     if container_request.parent
        #     else None
        # )
        container_fqn = fqn.build(
            self.metadata,
            entity_type=Container,
            service_name=self.context.get().objectstore_service,
            parent_container=parent_container,
            container_name=container_request.name.__root__,
        )

        self.container_source_state.add(container_fqn)

    def test_connection(self) -> None:
        test_connection_fn = get_test_connection_fn(self.service_connection)
        test_connection_fn(self.metadata, self.connection_obj, self.service_connection)

    def mark_containers_as_deleted(self) -> Iterable[Either[DeleteEntity]]:
        """Method to mark the containers as deleted"""
        if self.source_config.markDeletedContainers:
            yield from delete_entity_from_source(
                metadata=self.metadata,
                entity_type=Container,
                entity_source_state=self.container_source_state,
                mark_deleted_entity=self.source_config.markDeletedContainers,
                params={"service": self.context.get().objectstore_service},
            )

    def yield_create_request_objectstore_service(self, config: WorkflowSource):
        yield Either(
            right=self.metadata.get_create_service_from_source(
                entity=StorageService, config=config
            )
        )

    @staticmethod
    def _manifest_entries_to_metadata_entries_by_container(
            container_name: str, manifest: ManifestMetadataConfig
    ) -> List[MetadataEntry]:
        """
        Convert manifest entries (which have an extra bucket property) to bucket-level metadata entries, filtered by
        a given bucket
        """
        return [
            MetadataEntry(
                dataPath=entry.dataPath,
                structureFormat=entry.structureFormat,
                isPartitioned=entry.isPartitioned,
                partitionColumns=entry.partitionColumns,
                separator=entry.separator,
            )
            for entry in manifest.entries
            if entry.containerName == container_name
        ]

    @staticmethod
    def _get_sample_file_prefix(metadata_entry: MetadataEntry) -> Optional[str]:
        """
        Return a prefix if we have structure data to read
        """
        # Adding the ending separator so that we only read files from the right directory, for example:
        # if we have files in `transactions/*` and `transactions_old/*`, only passing the prefix as
        # `transactions` would list files for both directories. We need the prefix to be `transactions/`.
        result = f"{metadata_entry.dataPath.strip(KEY_SEPARATOR)}{KEY_SEPARATOR}"
        if not metadata_entry.structureFormat:
            logger.warning(f"Ignoring un-structured metadata entry {result}")
            return None
        return result

    @staticmethod
    def extract_column_definitions(
            bucket_name: str,
            sample_key: str,
            config_source: ConfigSource,
            client: Any,
            metadata_entry: MetadataEntry,
    ) -> List[Column]:
        """Extract Column related metadata from s3"""
        data_structure_details, raw_data = fetch_dataframe(
            config_source=config_source,
            client=client,
            file_fqn=DatalakeTableSchemaWrapper(
                key=sample_key,
                bucket_name=bucket_name,
                file_extension=SupportedTypes(metadata_entry.structureFormat),
                separator=metadata_entry.separator,
            ),
            fetch_raw_data=True,
        )
        columns = []
        column_parser = DataFrameColumnParser.create(
            data_structure_details,
            SupportedTypes(metadata_entry.structureFormat),
            raw_data=raw_data,
        )
        columns = column_parser.get_columns()
        return columns

    def _get_columns(
            self,
            bucket_name: str,
            sample_key: str,
            metadata_entry: MetadataEntry,
            config_source: ConfigSource,
            client: Any,
    ) -> Optional[List[Column]]:
        """Get the columns from the file and partition information"""
        extracted_cols = self.extract_column_definitions(
            bucket_name, sample_key, config_source, client, metadata_entry
        )
        return (metadata_entry.partitionColumns or []) + (extracted_cols or [])

    def _get_document_data(self, bucket_name: str, key: str, client: Any) -> Optional[str]:
        """
        Read the document from the bucket
        """
        local_dir_path = config.document_tmp_dir

        # 다운로드 디렉토리 확인 및 생성
        ensure_directory_exists(local_dir_path)

        file_extension = key.split('.')[-1]
        local_file_path = f"{local_dir_path}/{uuid.uuid4().hex}.{file_extension}"

        try:
            s3reader = S3Reader(client)
            s3reader.download(key, local_file_path=local_file_path, bucket_name=bucket_name, verbose=True)
            return local_file_path
        except (Exception,) as e:
            print(e)
            return None

    def _get_document_meta(self, bucket_name: str, path: str, metadata_entry: MetadataEntry,
                           client: Any) -> Optional[List[Rdf]]:
        """
        Read the document from the bucket
        """
        local_file_path = self._get_document_data(bucket_name, path, client)
        if local_file_path is None:
            return None

        try:
            file_extension = path.split('.')[-1]
            if file_extension in self.metadata_extractor:
                extractor = self.metadata_extractor[file_extension]
            else:
                if file_extension in [FileFormat.hwp.value, FileFormat.hwpx.value]:
                    extractor = HwpMetadataExtractor()
                elif file_extension in [FileFormat.doc.value, FileFormat.docx.value]:
                    extractor = MsWordMetadataExtractor()
                elif file_extension == FileFormat.txt.value:
                    extractor = TxtMetadataExtractor()
                # elif file_extension == FileFormat.json.value:
                #     extractor = JsonMetadataExtractor()
                elif file_extension == FileFormat.xml.value:
                    extractor = XmlMetadataExtractor()
                else:
                    logger.warn("Unsupported file type")
                    return None

                self.metadata_extractor[file_extension] = extractor

            metadata = extractor.get_metadata(local_file_path)

            rdfs = self._get_common_rdfs(metadata.items())

        except Exception as e:
            ingestion_logger().error(f"get rdfs fail : bucket : {bucket_name}, path : {path}, {e}")
            return None
        finally:
            os.remove(local_file_path)

        return rdfs


    def _get_image_meta(self, bucket_name: str, path: str,
                           client: Any) -> Optional[List[Rdf]]:
        """
        Read the image from the bucket
        """
        local_file_path = self._get_document_data(bucket_name, path, client)
        if local_file_path is None:
            return None

        try:
            file_extension = path.split('.')[-1]

            if file_extension in [FileFormat.jpg.value, FileFormat.png.value, FileFormat.jpeg.value,
                                  FileFormat.bmp.value, FileFormat.gif.value, FileFormat.webp.value,
                                  FileFormat.cr2.value, FileFormat.nef.value, FileFormat.tiff.value, FileFormat.tif.value]:
                metadata = self._get_pil_meta(local_file_path)
            elif file_extension in [FileFormat.arw.value, FileFormat.orf.value]:
                metadata = self._get_exifread_meta(local_file_path)
            elif file_extension == FileFormat.psd.value:
                metadata = self._get_psd_meta(local_file_path)
            elif file_extension == FileFormat.hdr.value:
                metadata = self._get_imageio_meta(local_file_path)
            elif file_extension == FileFormat.exr.value:
                metadata = self._get_exr_meta(local_file_path)
            elif file_extension == FileFormat.pdf.value:
                metadata = self._get_pdf_meta(local_file_path)
            else:
                logger.warn("Unsupported file type")
                return None

            # 이미지 객체 탐지
            try:
                detected_objects = self._get_detected_objects(local_file_path)
                if len(detected_objects) > 0:
                    metadata["detected_objects"] = detected_objects
            except Exception as e:
                logger.debug(f"image file detected fail [{e}], file {local_file_path}")

            rdfs = self._get_common_rdfs(metadata.items())

        finally:
            os.remove(local_file_path)

        return rdfs

    def _get_common_rdfs(self, meta_items: dict) -> Optional[List[Rdf]]:
        rdfs = []
        for k, v in meta_items:
            if v is None:
                continue
            if isinstance(v, str) and v == "":
                continue
            rdfs.append(Rdf(name=k, object=f'{v}'))

        return rdfs

    def _get_pil_meta(self, local_file_path: str) -> dict:
        """
        Extract metadata from jpg file
        """
        extractor = PilMetadataExtractor(local_file_path)
        return extractor.extract_metadata()

    def _get_pdf_meta(self, local_file_path: str) -> dict:
        """
        Extract metadata from pdf file
        """
        extractor = PdfMetadataExtractor(local_file_path)
        return extractor.extract_metadata()

    def _get_exifread_meta(self, local_file_path: str) -> dict:
        """
        Extract metadata from image file
        """
        extractor = ExifreadMetadataExtractor(local_file_path)
        return extractor.extract_metadata()

    def _get_psd_meta(self, local_file_path: str) -> dict:
        """
        Extract metadata from image file
        """
        extractor = PsdMetadataExtractor(local_file_path)
        return extractor.extract_metadata()

    def _get_imageio_meta(self, local_file_path: str) -> dict:
        """
        Extract metadata from image file
        """
        extractor = ImageioMetadataExtractor(local_file_path)
        return extractor.extract_metadata()

    def _get_exr_meta(self, local_file_path: str) -> dict:
        """
        Extract metadata from image file
        """
        extractor = ExrMetadataExtractor(local_file_path)
        return extractor.extract_metadata()

    def _get_detected_objects(self, local_file_path: str) -> Optional[List[Rdf]]:
        """
        이미지 객체 탐지
        """
        detector = YoloDetector(local_file_path)
        return detector.detected_objects()
