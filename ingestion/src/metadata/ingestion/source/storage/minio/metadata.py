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


import os
import traceback
import urllib.parse
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from metadata.generated.schema.type.tagLabel import TagLabel, LabelType, State, TagSource, TagFQN

from metadata.generated.schema.entity.classification.tag import Tag
from pydantic import ValidationError

from metadata.generated.schema.api.data.createContainer import CreateContainerRequest
from metadata.generated.schema.entity.data.container import (
    Container,
    FileFormat,
    ContainerDataModel,
)
from metadata.generated.schema.entity.services.connections.storage.minioConnection import (
    MinioConnection,
)
from metadata.generated.schema.entity.services.ingestionPipelines.status import (
    StackTraceError,
)
from metadata.generated.schema.metadataIngestion.storage.containerMetadataConfig import (
    MetadataEntry,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.type import basic
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.server.server_api import ServerInterface
from metadata.ingestion.source.storage.minio.models import (
    MinioBucketResponse,
    MinioContainerDetails,
)
from metadata.ingestion.source.storage.storage_service import (
    KEY_SEPARATOR,
    Metric,
    StorageServiceSource,
)
from metadata.readers.dataframe.dsv import (
    CSV_SEPARATOR,
    TSV_SEPARATOR
)
from metadata.utils import fqn
from metadata.utils.filters import filter_by_container, filter_by_bucket
from metadata.utils.logger import ingestion_logger
from metadata.utils.s3_utils import list_s3_objects

logger = ingestion_logger()

S3_CLIENT_ROOT_RESPONSE = "Contents"


def get_file_format(file_name: str) -> (str, str):
    """
    Get file_format, separator from file name
    """
    _, ext = os.path.splitext(file_name)
    file_format = ext.lower().strip('.')
    if file_format == "csv":
        separator = CSV_SEPARATOR
    elif file_format == "tsv":
        separator = TSV_SEPARATOR
    else:
        separator = None
    return file_format, separator


class MinioSource(StorageServiceSource):
    """
    Source implementation to ingest MinIO buckets data.
    """

    def __init__(self, config: WorkflowSource, metadata: ServerInterface):
        super().__init__(config, metadata)
        self.minio_client = self.connection.client

        # self._bucket_cache: Dict[str, Container] = {}
        self._dir_cache: Dict[str, List[str]] = {}
        self._metadata_cache: Dict[str, MetadataEntry] = {}

    @classmethod
    def create(
            cls, config_dict, metadata: ServerInterface, pipeline_name: Optional[str] = None
    ):
        config: WorkflowSource = WorkflowSource.parse_obj(config_dict)
        connection: MinioConnection = config.serviceConnection.__root__.config
        if not isinstance(connection, MinioConnection):
            raise InvalidSourceException(f"Expected MinIOConnection, but got {connection}")
        return cls(config, metadata)

    def get_buckets(self) -> Iterable[MinioContainerDetails]:
        bucket_results = self.fetch_buckets()
        for bucket_response in bucket_results:
            try:
                # We always generate the parent container (the bucket)
                yield self._generate_bucket_container(
                    bucket_response=bucket_response
                )
            except Exception as err:
                self.status.failed(
                    StackTraceError(
                        name=bucket_response.name,
                        error=f"Wild error while creating bucket(container) from bucket details - {err}",
                        stackTrace=traceback.format_exc(),
                    )
                )

    def fetch_buckets(self) -> List[MinioBucketResponse]:
        results: List[MinioBucketResponse] = []
        try:
            # if the service connection(minio connection setting) has bucket names, use them
            if self.service_connection.bucketNames:
                return [
                    MinioBucketResponse(Name=bucket_name)
                    for bucket_name in self.service_connection.bucketNames
                ]
            # No pagination required, as there is a hard 1000 limit on nr of buckets per account
            for bucket in self.minio_client.list_buckets().get("Buckets") or []:
                # if there is a filter pattern(in metadata ingestion setting), check if the bucket name matches it
                if filter_by_bucket(self.source_config.bucketFilterPattern, bucket["Name"]):
                    self.status.filter(bucket["Name"], "Bucket Filtered Out")
                else:
                    results.append(MinioBucketResponse.parse_obj(bucket))
        except Exception as err:
            logger.debug(traceback.format_exc())
            logger.error(f"Failed to fetch buckets list - {err}")
        return results

    def _generate_bucket_container(
            self, bucket_response: MinioBucketResponse
    ) -> MinioContainerDetails:

        self._metadata_cache.clear()
        self._dir_cache.clear()
        total_count, total_size = self._set_bucket_obj_info(bucket_name=bucket_response.name)

        return MinioContainerDetails(
            name=bucket_response.name,
            prefix=KEY_SEPARATOR,
            creation_date=(bucket_response.creation_date.isoformat() if bucket_response.creation_date else None),
            number_of_objects=total_count,
            size=total_size,
            file_formats=[],
            data_model=None,
            fullPath=self._get_full_path(bucket_name=bucket_response.name),
            sourceUrl=self._get_bucket_source_url(bucket_name=bucket_response.name),
        )

    def _set_bucket_obj_info(self, bucket_name: str):
        total_count = 0
        total_size = 0
        try:
            for obj in list_s3_objects(self.minio_client, Bucket=bucket_name, EncodingType='url'):
                total_count += 1
                decoded_key = urllib.parse.unquote_plus(obj['Key'])
                total_size += obj['Size']
                file_format, separator = get_file_format(decoded_key)
                self._metadata_cache[decoded_key] = MetadataEntry(
                    dataPath=f"{decoded_key}",
                    contentSize=obj['Size'],
                    structureFormat=file_format,
                    separator=separator,
                )
                logger.debug(
                    f"key : {decoded_key}, format : {file_format}, size : {obj['Size']}")
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.error(f"Unable to list objects in bucket: {bucket_name} - {exc}")
        return total_count, total_size

    def get_directories(self):
        directory = []
        bucket_container = self.get_bucket_entity()
        bucket_name = bucket_container.name.__root__

        service = self.context.get().objectstore_service
        self._dir_cache[bucket_name] = [service, bucket_name]
        for file_name, entity in self._metadata_cache.items():
            # 촤상위 디렉토리에 대한 처리
            if '/' not in file_name:
                if '/' not in directory:
                    directory.append('/')
                    self._dir_cache[bucket_name].append('/')
                    yield self._generate_directory_container(bucket_name, ['/'], bucket_container)
                    self._dir_cache[bucket_name].remove('/')
            else:
                dir_path = os.path.dirname(file_name)
                path_parts = dir_path.split('/')
                # 디렉토리 -> 디렉토리 -> 컨테이너 구조를 처리하기 위해 '/' 를 구분자로 부모 자식 관계를 만들고,
                # 최하위 노드(컨테이너)를 처리할 수 있도록 구조 변경
                for i in range(1, len(path_parts) + 1):
                    prefix = '/'.join(path_parts[:i])
                    if prefix not in directory:
                        directory.append(prefix)
                        parent_entity = self.get_parent_entity(path_parts[:i - 1])
                        # 디렉토리 하위의 컨테이너에서 FQN 생성을 위해 상위 개체 정보를 저장한다.
                        self._dir_cache[bucket_name].extend(path_parts[:i])
                        yield self._generate_directory_container(bucket_name, path_parts[:i], parent_entity)
                        # 다른 디렉토리를 위해 현재 디렉토리 정보를 삭제한다.
                        self._dir_cache[bucket_name] = [item for item in self._dir_cache[bucket_name] if
                                                        item not in path_parts[:i]]

    def get_bucket_entity(self) -> Container:
        container_fqn = fqn._build(  # pylint: disable=protected-access
            *(
                self.context.get().objectstore_service,
                self.context.get().bucket,
            )
        )
        container_entity = self.metadata.get_by_name(
            entity=Container, fqn=container_fqn
        )
        # self._bucket_cache[self.context.get().bucket] = container_entity
        return container_entity

    def get_parent_entity(self, path_parts: [str]) -> Container:
        container_fqn = fqn._build(  # pylint: disable=protected-access
            *(
                self.context.get().objectstore_service,
                self.context.get().bucket,
                *path_parts
            )
        )
        container_entity = self.metadata.get_by_name(
            entity=Container, fqn=container_fqn
        )
        # self._bucket_cache[self.context.get().bucket] = container_entity
        return container_entity

    def _generate_directory_container(self, bucket_name: str, path: [str],
                                      parent_container: Container) -> MinioContainerDetails:
        if path == ['/']:
            # Bucket 내 디렉토리 하위에 있지 않은 데이터를 위한 껍데기 데이터
            # 최상위 디렉토리 컨테이너는 생성하지 않는다.
            dir_container = MinioContainerDetails(
                name=path[0],
                prefix=KEY_SEPARATOR,
                number_of_objects=0,
                size=0,
                file_formats=[],
                data_model=None,
            )
            return dir_container
        else:
            count, size = self.get_directory_info(bucket_name, '/'.join(path))
            dir_container = MinioContainerDetails(
                name=path[-1],
                prefix=('/' if len(path) == 1 else '/'.join(path[:-1])),
                creation_date=None,
                number_of_objects=count,
                size=size,
                file_formats=[],
                data_model=None,
                parent=EntityReference(id=parent_container.id, type="container",
                                       fullyQualifiedName=parent_container.fullyQualifiedName.__root__),
                fullPath=self._get_full_path(bucket_name=bucket_name, prefix='/'.join(path)),
                sourceUrl=self._get_bucket_source_url(bucket_name=bucket_name),
            )
            return dir_container

    def get_directory_info(self, bucket_name: str, dir_path: str):
        total_count = 0
        total_size = 0
        try:
            dir_path_parts = dir_path.split('/')
            for entry in self._metadata_cache.values():
                entry_dir_path_parts = os.path.dirname(entry.dataPath).split('/')
                include_entry = True
                for i in range(0, len(dir_path_parts)):
                    if i >= len(entry_dir_path_parts):
                        break
                    if entry_dir_path_parts[i] != dir_path_parts[i]:
                        include_entry = False
                        break
                if include_entry:
                    total_count += 1
                    total_size += entry.contentSize
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.error(f"Unable to list objects in bucket: {bucket_name} - {exc}")
        return total_count, total_size

    def yield_create_directory_requests(self, container_details: MinioContainerDetails) -> (
            Iterable)[Either[CreateContainerRequest]]:
        if container_details.name == '/':
            # bucket: Container = self.get_bucket_entity()
            # container_request = CreateContainerRequest(
            #     name=bucket.name,
            #     prefix=bucket.prefix,
            #     numberOfObjects=bucket.numberOfObjects,
            #     size=bucket.size,
            #     fileFormats=[],
            #     dataModel=None,
            #     service=self.context.get().objectstore_service,
            #     sourceUrl=bucket.sourceUrl,
            #     fullPath=bucket.fullPath,
            # )
            yield Either(right=None)
        else:
            yield from self.yield_create_container_requests(container_details)

    def get_containers(self) -> Iterable[MinioContainerDetails]:
        service = self.context.get().objectstore_service
        bucket = self.context.get().bucket
        directory = self.context.get().directory

        parent_container_fqn = self.get_parent_container_fqn()
        parent_container: Container = self.metadata.get_by_name(
            entity=Container, fqn=parent_container_fqn
        )
        if parent_container is None:
            # self.status.failed(
            #     StackTraceError(
            #         name=bucket,
            #         error=f"Validation error while get parent Container from bucket/directory - {service}/{bucket}/{directory}",
            #     )
            # )
            return Either(left=StackTraceError(
                name=bucket,
                error=f"Validation error while get parent Container from bucket/directory - {service}/{bucket}/{directory}",
            ))

        for abs_file_name, metadata_entry in self._metadata_cache.items():
            try:
                if directory is None:
                    if '/' in abs_file_name:
                        continue
                else:
                    directory = '/'.join(self._dir_cache[bucket][2:])
                    if directory not in abs_file_name or directory != os.path.dirname(abs_file_name):
                        continue

                # file_name 은 Path/File 형태이다. 순수한 file_name을 가져온다.
                file_name = Path(abs_file_name).name

                container_fqn = fqn.build(
                    self.metadata,
                    entity_type=Container,
                    service_name=service,
                    parent_container=parent_container_fqn,
                    container_name=file_name,
                )
                if filter_by_container(
                        self.source_config.containerFilterPattern,
                        container_fqn
                        if self.source_config.useFqnForFiltering
                        else file_name
                ):
                    logger.warn(f"{container_fqn} Filtered")
                    self.status.filter(abs_file_name, "Container Name pattern not allowed")
                    continue

                if (metadata_entry.structureFormat in
                        [FileFormat.csv.value, FileFormat.tsv.value, FileFormat.xls.value, FileFormat.xlsx.value]):
                    logger.info(f"Structured Data Metadata Ingestion From : {file_name}")
                    structured_container: Optional[MinioContainerDetails] = (
                        self._generate_container_details(
                            bucket_name=bucket,
                            metadata_entry=metadata_entry,
                            parent=EntityReference(id=parent_container.id, type="container",
                                                   fullyQualifiedName=parent_container_fqn),
                        ))
                    # if structured_container:
                    #     for col in structured_container.data_model.columns:
                    #         logger.debug(f"Column: {col.name.__root__}")
                    if structured_container:
                        yield structured_container
                    else:
                        logger.warn(f"Failed To Generated Structured Container Metadata: {file_name}")
                        self.status.warnings.append(f"failed to generate structured container metadata: {file_name}")
                elif metadata_entry.structureFormat in [FileFormat.doc.value, FileFormat.docx.value,
                                                        FileFormat.hwp.value, FileFormat.hwpx.value]:
                    logger.info(f"Unstructured Data Metadata Ingestion From : {file_name}")
                    unstructured_container: Optional[MinioContainerDetails] = (
                        self._generate_unstructured_container_details(
                            bucket_name=bucket,
                            metadata_entry=metadata_entry,
                            parent=EntityReference(id=parent_container.id, type="container",
                                                   fullyQualifiedName=parent_container_fqn),
                        ))
                    if unstructured_container:
                        yield unstructured_container
                    else:
                        logger.warn(f"Failed To Generated Unstructured Container Metadata: {file_name}")
                        self.status.warnings.append(f"failed to generate unstructured container metadata: {file_name}")
                else:
                    logger.debug(f"Unsupported format {metadata_entry.structureFormat}")
                    # self.status.filter(abs_file_name, f"Unsupported format {metadata_entry.structureFormat}")

            except ValidationError as err:
                self.status.failed(
                    StackTraceError(
                        name=bucket,
                        error=f"Validation error while creating Container from bucket details - {err}",
                        stackTrace=traceback.format_exc(),
                    )
                )
            except Exception as err:
                self.status.failed(
                    StackTraceError(
                        name=bucket,
                        error=f"Wild error while creating Container from bucket details - {err}",
                        stackTrace=traceback.format_exc(),
                    )
                )

    def get_parent_container_fqn(self) -> str:
        service = self.context.get().objectstore_service
        bucket = self.context.get().bucket
        directory = self.context.get().directory

        if directory == '/' or directory is None:
            return fqn._build(  # pylint: disable=protected-access
                *(
                    service,
                    bucket,
                )
            )
        else:
            return fqn._build(  # pylint: disable=protected-access
                *self._dir_cache[bucket]
            )

    def yield_create_container_requests(
            self, container_details: MinioContainerDetails
    ) -> Iterable[Either[CreateContainerRequest]]:
        """ Get 태그 """
        tag_label = self.get_classification_tag_label(container_details)
        container_request = CreateContainerRequest(
            name=basic.EntityName(__root__=container_details.name),
            prefix=container_details.prefix,
            numberOfObjects=container_details.number_of_objects,
            size=container_details.size,
            dataModel=container_details.data_model,
            service=self.context.get().objectstore_service,
            parent=container_details.parent,
            tags=[tag_label] if tag_label else None,
            sourceUrl=container_details.sourceUrl,
            fileFormats=container_details.file_formats,
            fullPath=container_details.fullPath,
        )
        if container_details.rdfs:
            container_request.rdfs = container_details.rdfs
        yield Either(right=container_request)
        self.register_record(container_request=container_request)

    def get_classification_tag_label(self, container_details: MinioContainerDetails) -> Optional[TagLabel]:
        if container_details.parent is None:
            # parent 가 없는 경우 bucket 컨테이너이다.
            # 또한 최상위 '/' 디렉토리 컨테이너는 생성되지 않는다.
            # bucket -> '/' -> 'a' , '/a/b' 로 이어지는 관계를 만들지 않기 위함이다.
            container_fqn = fqn._build(
                *(
                    self.context.get().objectstore_service,
                    container_details.name
                )
            )
        else:
            container_fqn = fqn.FQN_SEPARATOR.join(
                [container_details.parent.fullyQualifiedName, fqn.quote_name(container_details.name)])
        container_entity: Container = self.metadata.get_by_name(entity=Container, fqn=container_fqn, fields=["tags"])
        if container_entity and container_entity.tags:
            for tag in container_entity.tags:
                if "ovp_category" in tag.tagFQN.__root__:
                    return None

        classification_tag: Tag = self.metadata.get_by_name(entity=Tag, fqn="ovp_category.미분류")
        if classification_tag:
            return TagLabel(
                tagFQN=TagFQN(__root__=classification_tag.fullyQualifiedName),
                labelType=LabelType.Automated.value,
                state=State.Suggested.value,
                source=TagSource.Classification,
            )
        return None

    def _clean_path(self, path: str) -> str:
        return path.strip(KEY_SEPARATOR)

    def _get_full_path(self, bucket_name: str, prefix: str = None) -> Optional[str]:
        """
        Method to get the full path of the file
        """
        if bucket_name is None:
            return None

        full_path = f"s3://{self._clean_path(bucket_name)}"

        if prefix:
            full_path += f"/{self._clean_path(prefix)}"

        return full_path

    def _get_bucket_source_url(self, bucket_name: str) -> Optional[str]:
        """
        Method to get the source url of minio bucket
        """
        try:
            if self.config.serviceConnection.__root__.config.minioConfig.region:
                return (
                    f"{self.config.serviceConnection.__root__.config.minioConfig.endPointURL}/buckets/{bucket_name}"
                    f"?region={self.config.serviceConnection.__root__.config.minioConfig.region}&tab=objects"
                )
            return (
                f"{self.config.serviceConnection.__root__.config.minioConfig.endPointURL}/buckets/{bucket_name}"
                f"?tab=objects"
            )
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.error(f"Unable to get source url: {exc}")
        return None

    def _get_object_source_url(self, bucket_name: str, prefix: str) -> Optional[str]:
        """
        Method to get the source url of minio bucket
        """
        try:
            if self.config.serviceConnection.__root__.config.minioConfig.region:
                return (
                    f"{self.config.serviceConnection.__root__.config.minioConfig.endPointURL}/buckets/{bucket_name}"
                    f"?region={self.config.serviceConnection.__root__.config.minioConfig.region}&prefix={prefix}/"
                    f"&showversions=false"
                )
            return (
                f"{self.config.serviceConnection.__root__.config.minioConfig.endPointURL}/buckets/{bucket_name}"
                f"?prefix={prefix}/&showversions=false"
            )
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.error(f"Unable to get source url: {exc}")
        return None

    # def _load_structured_objects(self, bucket_name: str) -> List[StructuredDataInfo]:
    #     """
    #     Load the structured data from bucket, if it exists
    #     """
    #     ret_data = []
    #     credentials = self.config.serviceConnection.__root__.config.minioConfig
    #     for entry in self._metadata_cache.values():
    #         try:
    #             if entry.structureFormat == "csv":
    #                 df_reader = get_df_reader(type_=SupportedTypes(entry.structureFormat),
    #                                           config_source=credentials, client=self.minio_client,
    #                                           separator=CSV_SEPARATOR)
    #             elif entry.structureFormat == "tsv":
    #                 df_reader = get_df_reader(type_=SupportedTypes(entry.structureFormat),
    #                                           config_source=credentials, client=self.minio_client,
    #                                           separator=TSV_SEPARATOR)
    #             else:
    #                 logger.info(f"Not Structured Data Object : {entry.dataPath} from bucket : {bucket_name}")
    #                 continue
    #
    #             logger.info(f"Read Object : {entry.dataPath} from bucket : {bucket_name}")
    #
    #             for encoding in SupportedEncoding:
    #                 try:
    #                     columns = df_reader.read(key=entry.dataPath, bucket_name=bucket_name,
    #                                              encoding=encoding.value)
    #                     entry.partitionColumns = []
    #                     for col in columns.dataframes[0]:
    #                         entry.partitionColumns.append(
    #                             Column(
    #                                 name=col,
    #                                 dataType=DataType.STRING,
    #                             )
    #                         )
    #                     metadata = StructuredDataInfo(
    #                         metadata=entry,
    #                         raw={
    #                             'LastModified': (columns.raw_data['LastModified']
    #                                              if columns.raw_data['LastModified'] else None),
    #                             'ContentLength': (columns.raw_data['ContentLength']
    #                                               if columns.raw_data['ContentLength'] else 0),
    #                             'ContentType': (columns.raw_data['ContentType'] if columns.raw_data[
    #                                 'ContentType'] else None),
    #                         } if columns.raw_data else None
    #                     )
    #                     ret_data.append(metadata)
    #                     break
    #                 except Exception as exc:
    #                     if encoding == SupportedEncoding.ISO8859_1:
    #                         logger.debug(traceback.format_exc())
    #                         logger.warning(
    #                             f"Retrying to read object file s3://{bucket_name}/{entry.dataPath}-{exc}")
    #         except Exception as exc:
    #             logger.debug(traceback.format_exc())
    #             logger.warning(
    #                 f"Failed loading object file s3://{bucket_name}/{entry.dataPath}-{exc}"
    #             )
    #     return ret_data

    # def _generate_structured_containers(
    #         self,
    #         bucket: str, obj: str,
    #         bucket_response: MinioBucketResponse,
    #         parent: Optional[EntityReference] = None,
    # ) -> MinioContainerDetails:
    #     result: List[MinioContainerDetails] = []
    #     for key, metadata_entry in self._metadata_cache.items():
    #         if (metadata_entry.structureFormat not in
    #                 [FileFormat.csv.value, FileFormat.tsv.value, FileFormat.xls.value, FileFormat.xlsx.value]):
    #             logger.debug(f"Unsupported format {metadata_entry.structureFormat}")
    #             self.status.filter(key, f"Unsupported format {metadata_entry.structureFormat}")
    #             continue
    #         logger.info(
    #             f"Extracting metadata from path {metadata_entry.dataPath.strip(KEY_SEPARATOR)} "
    #             f"and generating structured container"
    #         )
    #         structured_container: Optional[
    #             MinioContainerDetails
    #         ] = self._generate_container_details(
    #             bucket_name=bucket,
    #             metadata_entry=metadata_entry,
    #             parent=parent,
    #         )
    #         if structured_container:
    #             for col in structured_container.data_model.columns:
    #                 logger.debug(f"Column: {col.name.__root__}")
    #             result.append(structured_container)
    #
    #     return result

    def _generate_container_details(
            self,
            bucket_name: str,
            metadata_entry: MetadataEntry,
            parent: Optional[EntityReference] = None,
    ) -> Optional[MinioContainerDetails]:

        columns = self._get_columns(
            bucket_name=bucket_name,
            sample_key=metadata_entry.dataPath.strip(KEY_SEPARATOR),
            metadata_entry=metadata_entry,
            config_source=self.config.serviceConnection.__root__.config.minioConfig,
            client=self.minio_client,
        )
        if columns:
            prefix = (
                f"{KEY_SEPARATOR}{metadata_entry.dataPath.strip(KEY_SEPARATOR)}"
            )
            return MinioContainerDetails(
                name=Path(metadata_entry.dataPath.strip(KEY_SEPARATOR)).name,
                prefix=prefix,
                creation_date=self._fetch_metric(bucket_name=bucket_name,
                                                 key=metadata_entry.dataPath, metric=Metric.LAST_MODIFIED),
                number_of_objects=self._fetch_metric(
                    bucket_name=bucket_name, key=metadata_entry.dataPath, metric=Metric.NUMBER_OF_OBJECTS
                ),
                size=self._fetch_metric(
                    bucket_name=bucket_name, key=metadata_entry.dataPath, metric=Metric.BUCKET_SIZE_BYTES
                ),
                file_formats=[FileFormat(metadata_entry.structureFormat)],
                data_model=ContainerDataModel(columns=columns),
                parent=parent,
                fullPath=self._get_full_path(bucket_name, prefix),
                sourceUrl=self._get_object_source_url(
                    bucket_name=bucket_name,
                    prefix=metadata_entry.dataPath.strip(KEY_SEPARATOR),
                ),
            )
        return None

    def _generate_unstructured_container_details(
            self,
            bucket_name: str,
            metadata_entry: MetadataEntry,
            parent: Optional[EntityReference] = None,
    ) -> Optional[MinioContainerDetails]:

        rdfs = self._get_document_meta(
            bucket_name=bucket_name,
            path=metadata_entry.dataPath.strip(KEY_SEPARATOR),
            metadata_entry=metadata_entry,
            client=self.minio_client,
        )
        if rdfs:
            prefix = (
                f"{KEY_SEPARATOR}{metadata_entry.dataPath.strip(KEY_SEPARATOR)}"
            )
            return MinioContainerDetails(
                name=Path(metadata_entry.dataPath.strip(KEY_SEPARATOR)).name,
                prefix=prefix,
                creation_date=self._fetch_metric(bucket_name=bucket_name,
                                                 key=metadata_entry.dataPath, metric=Metric.LAST_MODIFIED),
                number_of_objects=self._fetch_metric(
                    bucket_name=bucket_name, key=metadata_entry.dataPath, metric=Metric.NUMBER_OF_OBJECTS
                ),
                size=self._fetch_metric(
                    bucket_name=bucket_name, key=metadata_entry.dataPath, metric=Metric.BUCKET_SIZE_BYTES
                ),
                file_formats=[FileFormat(metadata_entry.structureFormat)],
                data_model=None,
                rdfs=rdfs,
                parent=parent,
                fullPath=self._get_full_path(bucket_name, prefix),
                sourceUrl=self._get_object_source_url(
                    bucket_name=bucket_name,
                    prefix=metadata_entry.dataPath.strip(KEY_SEPARATOR),
                ),
            )
        return None

    def _fetch_metric(self, bucket_name: str, key: str, metric: Metric):
        try:
            key = urllib.parse.unquote_plus(key)
            res = self.minio_client.head_object(Bucket=bucket_name, Key=key)
            if metric == Metric.BUCKET_SIZE_BYTES:
                return float(res['ContentLength'])
            elif metric == Metric.NUMBER_OF_OBJECTS:
                return float(1)
            elif metric == Metric.LAST_MODIFIED:
                return str(res['LastModified'].isoformat())
        except Exception as err:
            logger.debug(traceback.format_exc())
            logger.warning(
                f"Failed fetching metric {metric.value} for bucket {bucket_name}, returning 0 - {err}"
            )
        return 0
