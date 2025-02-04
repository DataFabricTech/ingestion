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

from pydomo import Domo

from metadata.generated.schema.entity.automations.workflow import (
    Workflow as AutomationWorkflow,
)
from metadata.generated.schema.entity.services.connections.database.domoDatabaseConnection import (
    DomoDatabaseConnection,
)
from metadata.ingestion.connections.test_connections import (
    SourceConnectionException,
    test_connection_steps,
)
from metadata.ingestion.server.server_api import ServerInterface


def get_connection(connection: DomoDatabaseConnection) -> Domo:
    """
    Create connection
    """
    try:
        domo = Domo(
            connection.clientId,
            connection.secretToken.get_secret_value(),
            api_host=connection.apiHost,
        )
        return domo
    except Exception as exc:
        msg = f"Unknown error connecting with {connection}: {exc}."
        raise SourceConnectionException(msg)


def test_connection(
    metadata: ServerInterface,
    domo: Domo,
    service_connection: DomoDatabaseConnection,
    automation_workflow: Optional[AutomationWorkflow] = None,
) -> None:
    """
    Test connection. This can be executed either as part
    of a metadata workflow or during an Automation Workflow
    """

    def custom_executor():
        result = domo.datasets.list()
        return list(result)

    test_fn = {
        "GetTables": custom_executor,
    }

    test_connection_steps(
        metadata=metadata,
        test_fn=test_fn,
        service_type=service_connection.type.value,
        automation_workflow=automation_workflow,
    )
