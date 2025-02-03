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

from typing import TYPE_CHECKING

import boto3
import pytest
from testcontainers.localstack import LocalStackContainer

from metadata.generated.schema.api.services.createDatabaseService import (
    CreateDatabaseServiceRequest,
)
from metadata.generated.schema.entity.services.connections.database.dynamoDBConnection import (
    DynamoDBConnection,
)
from metadata.generated.schema.entity.services.databaseService import (
    DatabaseConnection,
    DatabaseService,
    DatabaseServiceType,
)
from metadata.generated.schema.security.credentials.awsCredentials import AWSCredentials

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient
else:
    DynamoDBClient = None


@pytest.fixture(scope="session")
def localstack_container():
    with LocalStackContainer("localstack/localstack:3.3") as container:
        yield container


@pytest.fixture(scope="session")
def ingest_sample_data(localstack_container):
    client: DynamoDBClient = boto3.client(
        "dynamodb",
        region_name="us-east-1",
        endpoint_url=localstack_container.get_url(),
        aws_access_key_id="does-not-matter",
        aws_secret_access_key="does-not-matter",
    )
    client.create_table(
        TableName="test_table",
        KeySchema=[
            {"AttributeName": "id", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "id", "AttributeType": "S"},
        ],
        ProvisionedThroughput={
            "ReadCapacityUnits": 5,
            "WriteCapacityUnits": 5,
        },
    )
    rows = [
        {"id": "1", "name": "Alice"},
        {"id": "2", "name": "Bob"},
    ]
    for row in rows:
        client.put_item(
            TableName="test_table", Item={k: {"S": v} for k, v in row.items()}
        )


@pytest.fixture(scope="module")
def db_service(metadata, localstack_container):
    service = CreateDatabaseServiceRequest(
        name="docker_dynamo_db",
        serviceType=DatabaseServiceType.DynamoDB,
        connection=DatabaseConnection(
            config=DynamoDBConnection(
                awsConfig=AWSCredentials(
                    awsRegion="us-east-1",
                    endPointURL=localstack_container.get_url(),
                    awsAccessKeyId="does-not-matter",
                    awsSecretAccessKey="does-not-matter",
                ),
            ),
        ),
    )
    service_entity = metadata.create_or_update(data=service)
    yield service_entity
    metadata.delete(
        DatabaseService, service_entity.id, recursive=True, hard_delete=True
    )
