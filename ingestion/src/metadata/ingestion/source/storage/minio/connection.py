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

"""
Source connection handler for MinIO object store. For this to work require :
ListBucket, GetObject, GetBucketLocation
"""
from dataclasses import dataclass
from functools import partial
from typing import Optional

from botocore.client import BaseClient
from metadata.clients.minio_client import MinioClient
from metadata.generated.schema.entity.automations.workflow import (
    Workflow as AutomationWorkflow,
)

from metadata.generated.schema.entity.services.connections.storage.minioConnection import (
    MinioConnection,
)

from metadata.ingestion.connections.test_connections import test_connection_steps
from metadata.ingestion.server.server_api import ServerInterface


@dataclass
class MinioObjectStoreClient:
    client: BaseClient


def get_connection(connection: MinioConnection) -> MinioObjectStoreClient:
    """
    Returns client - the minio client
    """
    client = MinioClient(connection.minioConfig)
    return MinioObjectStoreClient(
        client=client.get_client(),
    )


def test_connection(
        metadata: ServerInterface,
        client: MinioObjectStoreClient,
        service_connection: MinioConnection,
        automation_workflow: Optional[AutomationWorkflow] = None,
) -> None:
    """
    Test connection. This can be executed either as part
    of a metadata workflow or during an Automation Workflow
    """

    def test_buckets(connection: MinioConnection, client: MinioObjectStoreClient):
        if connection.bucketNames:
            for bucket_name in connection.bucketNames:
                client.client.list_objects(Bucket=bucket_name)
            return
        client.client.list_buckets()

    test_fn = {
        "ListBuckets": partial(
            test_buckets, client=client, connection=service_connection
        ),
    }

    test_connection_steps(
        metadata=metadata,
        service_type=service_connection.type.value,
        test_fn=test_fn,
        automation_workflow=automation_workflow,
    )
