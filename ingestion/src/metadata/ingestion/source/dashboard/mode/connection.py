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
from functools import partial
from typing import Optional

from metadata.generated.schema.entity.automations.workflow import (
    Workflow as AutomationWorkflow,
)
from metadata.generated.schema.entity.services.connections.dashboard.modeConnection import (
    ModeConnection,
)
from metadata.ingestion.connections.test_connections import test_connection_steps
from metadata.ingestion.server.server_api import ServerInterface
from metadata.ingestion.source.dashboard.mode.client import ModeApiClient


def get_connection(connection: ModeConnection) -> ModeApiClient:
    """
    Create connection
    """
    return ModeApiClient(connection)


def test_connection(
    metadata: ServerInterface,
    client: ModeApiClient,
    service_connection: ModeConnection,
    automation_workflow: Optional[AutomationWorkflow] = None,
) -> None:
    """
    Test connection. This can be executed either as part
    of a metadata workflow or during an Automation Workflow
    """

    test_fn = {
        "CheckDashboards": partial(
            client.fetch_all_reports, service_connection.workspaceName
        )
    }

    test_connection_steps(
        metadata=metadata,
        test_fn=test_fn,
        service_type=service_connection.type.value,
        automation_workflow=automation_workflow,
    )
