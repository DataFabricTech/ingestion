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
from metadata.generated.schema.entity.data.table import Constraint, Table
from metadata.generated.schema.entity.services.connections.database.mssqlConnection import (
    MssqlConnection,
    MssqlScheme,
)
from metadata.generated.schema.entity.services.databaseService import (
    DatabaseConnection,
    DatabaseService,
    DatabaseServiceType,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    MetadataWorkflowConfig,
    Sink,
    Source,
    SourceConfig,
    WorkflowConfig,
)
from metadata.ingestion.server.server_api import ServerInterface
from metadata.workflow.metadata import MetadataWorkflow

if not sys.version_info >= (3, 9):
    pytest.skip("requires python 3.9+", allow_module_level=True)


@pytest.fixture(
    scope="module",
    params=[
        MssqlScheme.mssql_pytds,
        pytest.param(
            MssqlScheme.mssql_pyodbc, marks=pytest.mark.xfail(reason="fails with mssql")
        ),
    ],
)
def db_service(metadata, sql_server_container, request):
    service = CreateDatabaseServiceRequest(
        name="docker_test_db_" + request.param.name,
        serviceType=DatabaseServiceType.Mssql,
        connection=DatabaseConnection(
            config=MssqlConnection(
                scheme=request.param,
                username=sql_server_container.username,
                password=sql_server_container.password,
                hostPort="localhost:"
                + sql_server_container.get_exposed_port(sql_server_container.port),
                database="AdventureWorks",
            )
        ),
    )
    service_entity = metadata.create_or_update(data=service)
    service_entity.connection.config.password = sql_server_container.password
    yield service_entity
    metadata.delete(
        DatabaseService, service_entity.id, recursive=True, hard_delete=True
    )


@pytest.fixture(autouse=True, scope="module")
def ingest_metadata(db_service, metadata: ServerInterface):
    workflow_config = MetadataWorkflowConfig(
        source=Source(
            type=db_service.connection.config.type.value.lower(),
            serviceName=db_service.fullyQualifiedName.__root__,
            sourceConfig=SourceConfig(config={}),
            serviceConnection=db_service.connection,
        ),
        sink=Sink(
            type="metadata-rest",
            config={},
        ),
        workflowConfig=WorkflowConfig(serverConfig=metadata.config),
    )
    logging.getLogger("sqlfluff").setLevel(logging.CRITICAL)
    metadata_ingestion = MetadataWorkflow.create(workflow_config)
    metadata_ingestion.execute()
    metadata_ingestion.raise_from_status()
    return


@pytest.fixture(scope="module")
def db_fqn(db_service: DatabaseService):
    return ".".join(
        [
            db_service.fullyQualifiedName.__root__,
            db_service.connection.config.database,
        ]
    )


def test_pass(
    db_service,
    metadata,
    db_fqn,
):
    table: Table = metadata.get_by_name(Table, f"{db_fqn}.HumanResources.Department")
    assert table is not None
    assert table.columns[0].name.__root__ == "DepartmentID"
    assert table.columns[0].constraint == Constraint.PRIMARY_KEY
    assert table.columns[1].name.__root__ == "Name"
    assert table.columns[2].name.__root__ == "GroupName"
    assert table.columns[3].name.__root__ == "ModifiedDate"
