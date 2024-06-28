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

import os
import traceback
import urllib.parse
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional

from pydantic import BaseModel, ValidationError

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
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.ometa.ometa_api import OpenMetadata
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
from metadata.utils.filters import filter_by_container
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

    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        super().__init__(config, metadata)
        self.minio_client = self.connection.minio_client

        self._bucket_cache: Dict[str, Container] = {}
        self._metadata_cache: Dict[str, MetadataEntry] = {}

    @classmethod
    def create(
            cls, config_dict, metadata: OpenMetadata, pipeline_name: Optional[str] = None
    ):
        config: WorkflowSource = WorkflowSource.parse_obj(config_dict)
        connection: MinioConnection = config.serviceConnection.__root__.config
        if not isinstance(connection, MinioConnection):
            raise InvalidSourceException(f"Expected MinIOConnection, but got {connection}")
        return cls(config, metadata)

    def get_containers(self) -> Iterable[MinioContainerDetails]:
        bucket_results = self.fetch_buckets()

        for bucket_response in bucket_results:
            bucket_name = bucket_response.name
            try:
                # We always generate the parent container (the bucket)
                yield self._generate_unstructured_container(
                    bucket_response=bucket_response
                )
                container_fqn = fqn._build(  # pylint: disable=protected-access
                    *(
                        self.context.get().objectstore_service,
                        self.context.get().container,
                    )
                )
                container_entity = self.metadata.get_by_name(
                    entity=Container, fqn=container_fqn
                )
                self._bucket_cache[bucket_name] = container_entity
                parent_entity: EntityReference = EntityReference(
                    id=self._bucket_cache[bucket_name].id.__root__, type="container"
                )
                yield from self._generate_structured_containers(
                    bucket_response=bucket_response,
                    parent=parent_entity,
                )
            except ValidationError as err:
                self.status.failed(
                    StackTraceError(
                        name=bucket_response.name,
                        error=f"Validation error while creating Container from bucket details - {err}",
                        stackTrace=traceback.format_exc(),
                    )
                )
            except Exception as err:
                self.status.failed(
                    StackTraceError(
                        name=bucket_response.name,
                        error=f"Wild error while creating Container from bucket details - {err}",
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
                if filter_by_container(
                        self.source_config.containerFilterPattern,
                        container_name=bucket["Name"],
                ):
                    self.status.filter(bucket["Name"], "Bucket Filtered Out")
                else:
                    results.append(MinioBucketResponse.parse_obj(bucket))
        except Exception as err:
            logger.debug(traceback.format_exc())
            logger.error(f"Failed to fetch buckets list - {err}")
        return results

    def yield_create_container_requests(
            self, container_details: MinioContainerDetails
    ) -> Iterable[Either[CreateContainerRequest]]:
        container_request = CreateContainerRequest(
            name=container_details.name,
            prefix=container_details.prefix,
            numberOfObjects=container_details.number_of_objects,
            size=container_details.size,
            dataModel=container_details.data_model,
            service=self.context.get().objectstore_service,
            parent=container_details.parent,
            sourceUrl=container_details.sourceUrl,
            fileFormats=container_details.file_formats,
            fullPath=container_details.fullPath,
        )
        yield Either(right=container_request)
        self.register_record(container_request=container_request)

    def _generate_unstructured_container(
            self, bucket_response: MinioBucketResponse
    ) -> MinioContainerDetails:
        return MinioContainerDetails(
            name=bucket_response.name,
            prefix=KEY_SEPARATOR,
            creation_date=(bucket_response.creation_date.isoformat() if bucket_response.creation_date else None),
            number_of_objects=self.list_objects(bucket_name=bucket_response.name),
            size=self.get_bucket_size(bucket_response.name),
            file_formats=[],
            data_model=None,
            fullPath=self._get_full_path(bucket_name=bucket_response.name),
            sourceUrl=self._get_bucket_source_url(bucket_name=bucket_response.name),
        )

    def list_objects(self, bucket_name: str) -> int:
        """
        Method to list the objects in the bucket
        """
        count = 0
        try:
            kwargs = {
                'Bucket': bucket_name,
                'EncodingType': 'url',
            }
            for key in list_s3_objects(self.minio_client, **kwargs):
                # Object Filtering
                # if filter_by_container(
                #         self.source_config.containerFilterPattern,
                #         container_name=bucket["Name"],
                # ):
                #     self.status.filter(bucket["Name"], "Bucket Filtered Out")
                # else:
                #     results.append(MinioBucketResponse.parse_obj(bucket))
                count += 1
                decoded_key = urllib.parse.unquote_plus(key['Key'])
                file_format, separator = get_file_format(decoded_key)
                self._metadata_cache[decoded_key] = MetadataEntry(
                    dataPath=f"{decoded_key}",
                    structureFormat=file_format,
                    separator=separator,
                )
                logger.debug(
                    f"key : {decoded_key}, format : {file_format}, size : {key['Size']}, "
                    f"storageClass : {key['StorageClass']}")
            return count
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.error(f"Unable to list objects in bucket: {bucket_name} - {exc}")
        return count

    def get_bucket_size(self, bucket_name: str) -> Optional[int]:
        """
        get the size of the bucket
        """
        try:
            response = self.minio_client.list_objects_v2(Bucket=bucket_name)
            return sum(
                [
                    obj["Size"]
                    for obj in response[S3_CLIENT_ROOT_RESPONSE]
                    if obj and obj.get("Size")
                ]
            )
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.error(f"Unable to get the size of the bucket: {bucket_name} - {exc}")
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

    def _generate_structured_containers(
            self,
            bucket_response: MinioBucketResponse,
            parent: Optional[EntityReference] = None,
    ) -> List[MinioContainerDetails]:
        result: List[MinioContainerDetails] = []
        for key, metadata_entry in self._metadata_cache.items():
            if (metadata_entry.structureFormat not in
                    [FileFormat.csv.value, FileFormat.tsv.value, FileFormat.xls.value, FileFormat.xlsx.value]):
                logger.debug(f"Unsupported format {metadata_entry.structureFormat}")
                self.status.filter(key, f"Unsupported format {metadata_entry.structureFormat}")
                continue
            logger.info(
                f"Extracting metadata from path {metadata_entry.dataPath.strip(KEY_SEPARATOR)} "
                f"and generating structured container"
            )
            structured_container: Optional[
                MinioContainerDetails
            ] = self._generate_container_details(
                bucket_response=bucket_response,
                metadata_entry=metadata_entry,
                parent=parent,
            )
            if structured_container:
                for col in structured_container.data_model.columns:
                    logger.debug(f"Column: {col.name.__root__}")
                result.append(structured_container)

        return result

    def _generate_container_details(
            self,
            bucket_response: MinioBucketResponse,
            metadata_entry: MetadataEntry,
            parent: Optional[EntityReference] = None,
    ) -> Optional[MinioContainerDetails]:
        bucket_name = bucket_response.name

        columns = self._get_columns(
            container_name=bucket_name,
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
                name=metadata_entry.dataPath.strip(KEY_SEPARATOR),
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
