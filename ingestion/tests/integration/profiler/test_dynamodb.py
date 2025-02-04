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

import pytest

from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.entity.services.databaseService import DatabaseService
from metadata.generated.schema.metadataIngestion.databaseServiceProfilerPipeline import (
    ProfilerConfigType,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    LogLevels,
    MetadataWorkflowConfig,
    Sink,
    Source,
    SourceConfig,
    WorkflowConfig,
)
from metadata.ingestion.server.server_api import ServerInterface
from metadata.workflow.metadata import MetadataWorkflow
from metadata.workflow.profiler import ProfilerWorkflow


@pytest.fixture(autouse=True, scope="module")
def ingest_metadata(
    db_service: DatabaseService, metadata: ServerInterface, ingest_sample_data
):
    workflow_config = MetadataWorkflowConfig(
        source=Source(
            type=db_service.serviceType.name.lower(),
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
    metadata_ingestion = MetadataWorkflow.create(workflow_config)
    metadata_ingestion.execute()
    metadata_ingestion.raise_from_status()
    return


@pytest.fixture(scope="module")
def db_fqn(db_service: DatabaseService):
    return ".".join(
        [
            db_service.fullyQualifiedName.__root__,
            "default",
            "default",
        ]
    )


def test_sample_data(db_service, db_fqn, metadata):
    workflow_config = {
        "source": {
            "type": db_service.serviceType.name.lower(),
            "serviceName": db_service.fullyQualifiedName.__root__,
            "sourceConfig": {
                "config": {
                    "type": ProfilerConfigType.Profiler.value,
                },
            },
        },
        "processor": {
            "type": "orm-profiler",
            "config": {},
        },
        "sink": {
            "type": "metadata-rest",
            "config": {},
        },
        "workflowConfig": {
            "loggerLevel": LogLevels.DEBUG,
            "serverConfig": metadata.config.dict(),
        },
    }
    profiler_workflow = ProfilerWorkflow.create(workflow_config)
    profiler_workflow.execute()
    profiler_workflow.raise_from_status()
    table = metadata.list_entities(
        Table,
        fields=["fullyQualifiedName"],
        params={
            "databaseSchema": db_fqn,
        },
    ).entities[0]
    sample_data = metadata.get_sample_data(table).sampleData
    assert sample_data is not None
    assert len(sample_data.columns) == 2
    assert len(sample_data.rows) == 2
    assert sample_data.rows[0][0] == "Alice"
