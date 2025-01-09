
import json
import secrets
import traceback
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional

from pydantic import ValidationError

from metadata.generated.schema.api.data.createDirectory import CreateDirectoryRequest

from metadata.generated.schema.entity.data import container
from metadata.generated.schema.entity.data.container import (
    Container,
    ContainerDataModel
)
from metadata.generated.schema.entity.services.connections.database.datalake.s3Config import (
    S3Config,
)
from metadata.generated.schema.entity.services.connections.ssh.sshConnection import (
    SshConnection,
)
from metadata.generated.schema.metadataIngestion.storage.containerMetadataConfig import (
    MetadataEntry,
    StorageContainerConfig,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.api.models import Either, StackTraceError
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.filesystem.linux.models import (
    DirectoryDetails
)

from metadata.ingestion.source.filesystem.filesystem_service import (
    FilesystemServiceSource,
)
from metadata.readers.file.base import ReadException
from metadata.readers.file.config_source_factory import get_reader
from metadata.utils import fqn
from metadata.utils.filters import filter_by_container
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


class LinuxSource(FilesystemServiceSource):

    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        super().__init__(config, metadata)
        self.ssh_client = self.connection

        # self._bucket_cache: Dict[str, Container] = {}
        # self.s3_reader = get_reader(config_source=S3Config(), client=self.s3_client)

    @classmethod
    def create(cls, config_dict, metadata: OpenMetadata):
        config: WorkflowSource = WorkflowSource.parse_obj(config_dict)
        connection: SshConnection = config.serviceConnection.__root__.config
        if not isinstance(connection, SshConnection):
            raise InvalidSourceException(
                f"Expected SShConnection, but got {connection}"
            )
        return cls(config, metadata)



    # def get_directory(self) -> Iterable[DirectoryDetails]:
    def get_directory(self) -> List[DirectoryDetails]:

        return self.fetch_directory()


    def fetch_directory(self):
        """
        ssh 확인
        :return:
        """

        directory_list = []
        stdin, stdout, stderr = self.ssh_client.exec_command('ls -l')

        data = stdout.readlines()

        for line in data:
            directory_info_arr = line.split()
            if len(directory_info_arr) == 9:
                owner = directory_info_arr[2]
                size = directory_info_arr[4]
                last_modified = f"{directory_info_arr[5]} {directory_info_arr[6]} {directory_info_arr[7]}"
                name = directory_info_arr[8]


                directory_list.append(DirectoryDetails(
                    name=name,
                    size=size,
                    owner=owner,
                    last_modified=last_modified
                ))

                # print(f"owner : {owner}")
                # print(f"size : {size}")
                # print(f"last_modified : {last_modified}")
                # print(f"name : {name}")
                # print("="*20)

        return directory_list



    def yield_create_directory_requests(
            self, directory_details: DirectoryDetails
    ) -> Iterable[Either[CreateDirectoryRequest]]:
        yield Either(
            right=CreateDirectoryRequest(
                name=directory_details.name,
                service=self.context.filesystem_service,
                # numberOfFiles=directory_details.number_of_objects,
                size=directory_details.size,
                owner=directory_details.owner,
                last_modified=directory_details.last_modified,
                systemType=self.config.systemType,
            )
        )


