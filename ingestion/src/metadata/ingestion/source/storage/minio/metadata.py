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

import json
import secrets
import traceback
import urllib.parse
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Iterable, List, Optional

from pydantic import ValidationError
from metadata.utils.s3_utils import list_s3_objects

from metadata.generated.schema.api.data.createContainer import CreateContainerRequest
from metadata.generated.schema.entity.data import container
from metadata.generated.schema.entity.data.container import (
    Container,
    ContainerDataModel,
)

# from metadata.generated.schema.entity.services.connections.database.datalake.s3Config import (
#     S3Config,
# )
from metadata.generated.schema.entity.services.connections.storage.minioConnection import (
    MinioConnection,
)
from metadata.generated.schema.entity.services.ingestionPipelines.status import (
    StackTraceError,
)
from metadata.generated.schema.metadataIngestion.storage.containerMetadataConfig import (
    MetadataEntry,
    StorageContainerConfig,
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
    StructuredDataDetails,
)
from metadata.ingestion.source.storage.storage_service import (
    KEY_SEPARATOR,
    OPENMETADATA_TEMPLATE_FILE_NAME,
    StorageServiceSource,
)
from metadata.readers.file.base import ReadException
from metadata.readers.file.config_source_factory import get_reader
from metadata.utils import fqn
from metadata.utils.filters import filter_by_container
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()

S3_CLIENT_ROOT_RESPONSE = "Contents"


class MinioSource(StorageServiceSource):
    """
    Source implementation to ingest MinIO buckets data.
    """

    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        super().__init__(config, metadata)
        self.minio_client = self.connection.minio_client

        self._bucket_cache: Dict[str, Container] = {}

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
                if self.global_manifest:
                    manifest_entries_for_current_bucket = (
                        self._manifest_entries_to_metadata_entries_by_container(
                            container_name=bucket_name, manifest=self.global_manifest
                        )
                    )
                    # Check if we have entries in the manifest file belonging to this bucket
                    if manifest_entries_for_current_bucket:
                        # ingest all the relevant valid paths from it
                        yield from self._generate_structured_containers(
                            bucket_response=bucket_response,
                            entries=manifest_entries_for_current_bucket,
                            parent=parent_entity,
                        )
                        # nothing else do to for the current bucket, skipping to the next
                        continue
                # If no global file, or no valid entries in the manifest, check for bucket level metadata file
                metadata_config = self._load_metadata_file(bucket_name=bucket_name)
                if metadata_config:
                    yield from self._generate_structured_containers(
                        bucket_response=bucket_response,
                        entries=metadata_config.entries,
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

    def _generate_container_details(
        self,
        bucket_response: MinioBucketResponse,
        metadata_entry: MetadataEntry,
        parent: Optional[EntityReference] = None,
    ) -> Optional[MinioContainerDetails]:
        bucket_name = bucket_response.name
        sample_key = self._get_sample_file_path(
            bucket_name=bucket_name, metadata_entry=metadata_entry
        )
        # if we have a sample file to fetch a schema from
        if sample_key:
            columns = self._get_columns(
                container_name=bucket_name,
                sample_key=sample_key,
                metadata_entry=metadata_entry,
                config_source=None,
                # S3Config(
                #     securityConfig=self.service_connection.awsConfig
                # ),
                client=self.minio_client,
            )
            if columns:
                prefix = (
                    f"{KEY_SEPARATOR}{metadata_entry.dataPath.strip(KEY_SEPARATOR)}"
                )
                return MinioContainerDetails(
                    name=metadata_entry.dataPath.strip(KEY_SEPARATOR),
                    prefix=prefix,
                    creation_date=bucket_response.creation_date.isoformat()
                    if bucket_response.creation_date
                    else None,
                    number_of_objects=list_s3_objects(self.minio_client, bucket_name=bucket_name),
                    size=self.get_bucket_size(bucket_name=bucket_name),
                    file_formats=[container.FileFormat(metadata_entry.structureFormat)],
                    data_model=ContainerDataModel(
                        isPartitioned=metadata_entry.isPartitioned, columns=columns
                    ),
                    parent=parent,
                    fullPath=self._get_full_path(bucket_name, prefix),
                    sourceUrl=self._get_object_source_url(
                        bucket_name=bucket_name,
                        prefix=metadata_entry.dataPath.strip(KEY_SEPARATOR),
                    ),
                )
        return None

    def _generate_structured_containers(
        self,
        bucket_response: MinioBucketResponse,
        entries: List[MetadataEntry],
        parent: Optional[EntityReference] = None,
    ) -> List[MinioContainerDetails]:
        result: List[MinioContainerDetails] = []
        for metadata_entry in entries:
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
                result.append(structured_container)

        return result

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


    def list_objects(self, bucket_name: str) -> int:
        """
        Method to list the objects in the bucket
        """
        # response = client.list_objects_v2(
        #     Bucket='string', Delimiter='string', EncodingType='url', \
        #         MaxKeys=123, Prefix='string', ContinuationToken='string',
        #     FetchOwner=True | False, StartAfter='string', RequestPayer='requester', ExpectedBucketOwner='string',
        #     OptionalObjectAttributes=[
        #         'RestoreStatus',
        #     ]
        # )

        # ...
        # 'Contents': [
        #    {
        #      'Key': 'string',
        #      'LastModified': datetime(2015, 1, 1),
        #      'ETag': 'string',
        #      'ChecksumAlgorithm': [
        #          'CRC32'|'CRC32C'|'SHA1'|'SHA256',
        #      ],
        #      'Size': 123,
        #      'StorageClass': 'STANDARD'|'REDUCED_REDUNDANCY'|'GLACIER'|\
        #         'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT_TIERING'|'DEEP_ARCHIVE'|\
        #         'OUTPOSTS'|'GLACIER_IR'|'SNOW'|'EXPRESS_ONEZONE',
        #      'Owner': {
        #          'DisplayName': 'string',
        #          'ID': 'string'
        #      },
        #      'RestoreStatus': {
        #          'IsRestoreInProgress': True|False,
        #          'RestoreExpiryDate': datetime(2015, 1, 1)
        #      }
        #    },
        #  ],
        #  ...
        #

        try:
            kwargs = {
                'Bucket': bucket_name,
                'EncodingType': 'url',
                # 'Prefix': prefix if prefix.endswith("/") else f"{prefix}/"
            }
            for key in list_s3_objects(self.minio_client, **kwargs):
                decoded_key = urllib.parse.unquote(key['Key'])
                logger.info(f"key          : {decoded_key}")
                logger.info(f"size         : {key['Size']}")
                logger.info(f"storageClass : {key['StorageClass']}")
            return 0
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.error(f"Unable to list objects in bucket: {bucket_name} - {exc}")
        return 0


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

    def _get_sample_file_path(
        self, bucket_name: str, metadata_entry: MetadataEntry
    ) -> Optional[str]:
        """
        Given a bucket and a metadata entry, returns the full path key to a file which can then be used to infer schema
        or None in the case of a non-structured metadata entry, or if no such keys can be found
        """
        prefix = self._get_sample_file_prefix(metadata_entry=metadata_entry)
        # this will look only in the first 1000 files under that path (default for list_objects_v2).
        # We'd rather not do pagination here as it would incur unwanted costs
        try:
            if prefix:
                response = self.minio_client.list_objects_v2(
                    Bucket=bucket_name, Prefix=prefix
                )
                candidate_keys = [
                    entry["Key"]
                    for entry in response[S3_CLIENT_ROOT_RESPONSE]
                    if entry and entry.get("Key")
                ]
                # pick a random key out of the candidates if any were returned
                if candidate_keys:
                    result_key = secrets.choice(candidate_keys)
                    logger.info(
                        f"File {result_key} was picked to infer data structure from."
                    )
                    return result_key
                logger.warning(
                    f"No sample files found in {prefix} with {metadata_entry.structureFormat} extension"
                )
            return None
        except Exception:
            logger.debug(traceback.format_exc())
            logger.warning(
                f"Error when trying to list objects in S3 bucket {bucket_name} at prefix {prefix}"
            )
            return None

    # def get_bucket_region(self, bucket_name: str) -> str:
    #     """
    #     Method to fetch the bucket region
    #     """
    #     region = None
    #     try:
    #         self.connection.minio_config.region
    #         region_resp = self.minio_client.get_bucket_location(Bucket=bucket_name)
    #         region = region_resp.get("LocationConstraint")
    #     except Exception:
    #         logger.debug(traceback.format_exc())
    #         logger.warning(f"Unable to get the region for bucket: {bucket_name}")
    #     return region or self.service_connection.awsConfig.awsRegion

    def _get_bucket_source_url(self, bucket_name: str) -> Optional[str]:
        """
        Method to get the source url of s3 bucket
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
        Method to get the source url of s3 bucket
        """
        try:
            region = self.get_aws_bucket_region(bucket_name=bucket_name)
            return (
                f"https://s3.console.aws.amazon.com/s3/buckets/{bucket_name}"
                f"?region={region}&prefix={prefix}/"
                f"&showversions=false"
            )
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.error(f"Unable to get source url: {exc}")
        return None

    def _load_metadata_file(self, bucket_name: str) -> Optional[StorageContainerConfig]:
        """
        Load the metadata template file from the root of the bucket, if it exists
        """
        # try:
        #     logger.info(
        #         f"Looking for metadata template file at - s3://{bucket_name}/{OPENMETADATA_TEMPLATE_FILE_NAME}"
        #     )
        #     response_object = self.s3_reader.read(
        #         path=OPENMETADATA_TEMPLATE_FILE_NAME,
        #         bucket_name=bucket_name,
        #         verbose=False,
        #     )
        #     content = json.loads(response_object)
        #     metadata_config = StorageContainerConfig.parse_obj(content)
        #     return metadata_config
        # except ReadException:
        #     logger.warning(
        #         f"No metadata file found at s3://{bucket_name}/{OPENMETADATA_TEMPLATE_FILE_NAME}"
        #     )
        # except Exception as exc:
        #     logger.debug(traceback.format_exc())
        #     logger.warning(
        #         f"Failed loading metadata file s3://{bucket_name}/{OPENMETADATA_TEMPLATE_FILE_NAME}-{exc}"
        #     )
        return None

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

# airflow', 'tasks', 'run', 'efe11ad4-02ef-464f-8841-7b63ae336acb',
# 'ingestion_task', 'manual__2024-06-20T10:10:23.157460+00:00',
# '--job-id', '12', '--raw', '--subdir', 'DAGS_FOLDER/efe11ad4-02ef-464f-8841-7b63ae336acb.py',
# '--cfg-path', '/tmp/tmpcgrv73_r']