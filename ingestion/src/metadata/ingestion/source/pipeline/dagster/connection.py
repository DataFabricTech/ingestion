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
Source connection handler
"""
from typing import Optional

from metadata.generated.schema.entity.automations.workflow import (
    Workflow as AutomationWorkflow,
)
from metadata.generated.schema.entity.services.connections.pipeline.dagsterConnection import (
    DagsterConnection,
)
from metadata.ingestion.connections.test_connections import test_connection_steps
from metadata.ingestion.server.server_api import ServerInterface
from metadata.ingestion.source.pipeline.dagster.client import DagsterClient
from metadata.ingestion.source.pipeline.dagster.queries import TEST_QUERY_GRAPHQL


def get_connection(connection: DagsterConnection) -> DagsterClient:
    """
    Create connection
    """
    return DagsterClient(connection)


def test_connection(
    metadata: ServerInterface,
    client: DagsterClient,
    service_connection: DagsterConnection,
    automation_workflow: Optional[AutomationWorkflow] = None,
) -> None:
    """
    Test connection. This can be executed either as part
    of a metadata workflow or during an Automation Workflow
    """

    def custom_executor_for_pipeline():
        client.client._execute(TEST_QUERY_GRAPHQL)  # pylint: disable=protected-access

    test_fn = {"GetPipelines": custom_executor_for_pipeline}

    test_connection_steps(
        metadata=metadata,
        test_fn=test_fn,
        service_type=service_connection.type.value,
        automation_workflow=automation_workflow,
    )
