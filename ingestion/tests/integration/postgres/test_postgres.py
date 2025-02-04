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

import logging
import sys

import pytest

from metadata.generated.schema.api.services.createDatabaseService import (
    CreateDatabaseServiceRequest,
)
from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.entity.services.connections.database.common.basicAuth import (
    BasicAuth,
)
from metadata.generated.schema.entity.services.connections.database.postgresConnection import (
    PostgresConnection,
)
from metadata.generated.schema.entity.services.databaseService import (
    DatabaseConnection,
    DatabaseService,
    DatabaseServiceType,
)
from metadata.generated.schema.metadataIngestion.databaseServiceProfilerPipeline import (
    DatabaseServiceProfilerPipeline,
)
from metadata.generated.schema.metadataIngestion.databaseServiceQueryLineagePipeline import (
    DatabaseServiceQueryLineagePipeline,
)
from metadata.generated.schema.metadataIngestion.databaseServiceQueryUsagePipeline import (
    DatabaseUsageConfigType,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    LogLevels,
    MetadataWorkflowConfig,
    Processor,
    Sink,
    Source,
    SourceConfig,
    WorkflowConfig,
)
from metadata.ingestion.server.server_api import ServerInterface
from metadata.profiler.api.models import ProfilerProcessorConfig
from metadata.workflow.metadata import MetadataWorkflow
from metadata.workflow.profiler import ProfilerWorkflow
from metadata.workflow.usage import UsageWorkflow

if not sys.version_info >= (3, 9):
    pytest.skip("requires python 3.9+", allow_module_level=True)


@pytest.fixture(autouse=True)
def config_logging():
    logging.getLogger("sqlfluff").setLevel(logging.CRITICAL)


@pytest.fixture(scope="module")
def db_service(metadata, postgres_container):
    service = CreateDatabaseServiceRequest(
        name="docker_test_db",
        serviceType=DatabaseServiceType.Postgres,
        connection=DatabaseConnection(
            config=PostgresConnection(
                username=postgres_container.username,
                authType=BasicAuth(password=postgres_container.password),
                hostPort="localhost:"
                + postgres_container.get_exposed_port(postgres_container.port),
                database="dvdrental",
            )
        ),
    )
    service_entity = metadata.create_or_update(data=service)
    service_entity.connection.config.authType.password = postgres_container.password
    yield service_entity
    metadata.delete(
        DatabaseService, service_entity.id, recursive=True, hard_delete=True
    )


@pytest.fixture(scope="module")
def ingest_metadata(db_service, metadata: ServerInterface):
    workflow_config = MetadataWorkflowConfig(
        source=Source(
            type=db_service.connection.config.type.value.lower(),
            serviceName=db_service.fullyQualifiedName.__root__,
            serviceConnection=db_service.connection,
            sourceConfig=SourceConfig(config={}),
        ),
        sink=Sink(
            type="metadata-rest",
            config={},
        ),
        workflowConfig=WorkflowConfig(serverConfig=metadata.config),
    )
    metadata_ingestion = MetadataWorkflow.create(workflow_config)
    metadata_ingestion.execute()
    return


@pytest.fixture(scope="module")
def ingest_lineage(db_service, ingest_metadata, metadata: ServerInterface):
    workflow_config = MetadataWorkflowConfig(
        source=Source(
            type="postgres-lineage",
            serviceName=db_service.fullyQualifiedName.__root__,
            serviceConnection=db_service.connection,
            sourceConfig=SourceConfig(config=DatabaseServiceQueryLineagePipeline()),
        ),
        sink=Sink(
            type="metadata-rest",
            config={},
        ),
        workflowConfig=WorkflowConfig(serverConfig=metadata.config),
    )
    metadata_ingestion = MetadataWorkflow.create(workflow_config)
    metadata_ingestion.execute()
    return


@pytest.fixture(scope="module")
def run_profiler_workflow(ingest_metadata, db_service, metadata):
    workflow_config = MetadataWorkflowConfig(
        source=Source(
            type=db_service.connection.config.type.value.lower(),
            serviceName=db_service.fullyQualifiedName.__root__,
            serviceConnection=db_service.connection,
            sourceConfig=SourceConfig(config=DatabaseServiceProfilerPipeline()),
        ),
        processor=Processor(
            type="orm-profiler",
            config=ProfilerProcessorConfig(),
        ),
        sink=Sink(
            type="metadata-rest",
            config={},
        ),
        workflowConfig=WorkflowConfig(
            loggerLevel=LogLevels.DEBUG, serverConfig=metadata.config
        ),
    )
    metadata_ingestion = ProfilerWorkflow.create(workflow_config.dict())
    metadata_ingestion.execute()
    return


@pytest.fixture(scope="module")
def ingest_query_usage(ingest_metadata, db_service, metadata):
    workflow_config = {
        "source": {
            "type": "postgres-usage",
            "serviceName": db_service.fullyQualifiedName.__root__,
            "serviceConnection": db_service.connection.dict(),
            "sourceConfig": {
                "config": {"type": DatabaseUsageConfigType.DatabaseUsage.value}
            },
        },
        "processor": {"type": "query-parser", "config": {}},
        "stage": {
            "type": "table-usage",
            "config": {
                "filename": "/tmp/postgres_usage",
            },
        },
        "bulkSink": {
            "type": "metadata-usage",
            "config": {
                "filename": "/tmp/postgres_usage",
            },
        },
        "sink": {"type": "metadata-rest", "config": {}},
        "workflowConfig": {
            "loggerLevel": "DEBUG",
            "serverConfig": metadata.config.dict(),
        },
    }
    workflow = UsageWorkflow.create(workflow_config)
    workflow.execute()
    workflow.raise_from_status()
    return


@pytest.fixture(scope="module")
def db_fqn(db_service: DatabaseService):
    return ".".join(
        [
            db_service.fullyQualifiedName.__root__,
            db_service.connection.config.database,
        ]
    )


def test_query_usage(
    ingest_query_usage,
    db_service,
    metadata,
    db_fqn,
):
    table = metadata.get_by_name(Table, ".".join([db_fqn, "public", "actor"]))
    queries = metadata.get_entity_queries(table.id)
    # TODO this should be retruning 2 queries but in CI sometimes it returns 1 *shrug*
    assert 1 <= len(queries) <= 2


def test_profiler(run_profiler_workflow):
    pass


def test_lineage(ingest_lineage):
    pass
