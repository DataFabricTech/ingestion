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
Metadata high-level API Workflow test
"""
from unittest import TestCase

from sqlalchemy.engine import Engine

from metadata.generated.schema.api.automations.createWorkflow import (
    CreateWorkflowRequest,
)
from metadata.generated.schema.entity.automations.testServiceConnection import (
    TestServiceConnectionRequest,
)
from metadata.generated.schema.entity.automations.workflow import (
    Workflow,
    WorkflowStatus,
    WorkflowType,
)
from metadata.generated.schema.entity.services.connections.database.common.basicAuth import (
    BasicAuth,
)
from metadata.generated.schema.entity.services.connections.database.mysqlConnection import (
    MysqlConnection,
    MySQLType,
)
from metadata.generated.schema.entity.services.connections.metadata.metadataConnection import (
    MetadataConnection,
)
from metadata.generated.schema.entity.services.connections.testConnectionResult import (
    StatusType,
)
from metadata.generated.schema.entity.services.databaseService import DatabaseConnection
from metadata.generated.schema.entity.services.serviceType import ServiceType
from metadata.generated.schema.security.client.metadataJWTClientConfig import (
    MetadataJWTClientConfig,
)
from metadata.ingestion.server.server_api import ServerInterface
from metadata.ingestion.source.connections import get_connection, get_test_connection_fn

SERVICE_CONNECTION = MysqlConnection(
    username="metadata_user",
    authType=BasicAuth(password="metadata_password"),
    hostPort="localhost:3306",
)


class TestConnectionAutomationTest(TestCase):
    """
    Run this integration test with the local API available
    Install the ingestion package before running the tests
    """

    service_entity_id = None

    server_config = MetadataConnection(
        hostPort="http://localhost:8585/api",
        authProvider="metadata",
        securityConfig=MetadataJWTClientConfig(
            jwtToken="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhZG1pbiIsImlzQm90IjpmYWxzZSwiaXNzIjoib3Blbi1tZXRhZGF0YS5vcmciLCJpYXQiOjE2NjM5Mzg0NjIsImVtYWlsIjoiYWRtaW5Ab3Blbm1ldGFkYXRhLm9yZyJ9.tS8um_5DKu7HgzGBzS1VTA5uUjKWOCU0B_j08WXBiEC0mr0zNREkqVfwFDD-d24HlNEbrqioLsBuFRiwIWKc1m_ZlVQbG7P36RUxhuv2vbSp80FKyNM-Tj93FDzq91jsyNmsQhyNv_fNr3TXfzzSPjHt8Go0FMMP66weoKMgW2PbXlhVKwEuXUHyakLLzewm9UMeQaEiRzhiTMU3UkLXcKbYEJJvfNFcLwSl9W8JCO_l0Yj3ud-qt_nQYEZwqW6u5nfdQllN133iikV4fM5QZsMCnm8Rq1mvLR0y9bmJiD7fwM1tmJ791TUWqmKaTnP49U493VanKpUAfzIiOiIbhg"
        ),
    )
    metadata = ServerInterface(server_config)

    assert metadata.health_check()

    def test_connection_workflow(self):
        """
        Test all the steps related to the test connection automation workflow
        """
        new_workflow_request = CreateWorkflowRequest(
            name="test-connection-mysql",
            description="description",
            workflowType=WorkflowType.TEST_CONNECTION,
            request=TestServiceConnectionRequest(
                serviceType=ServiceType.Database,
                connectionType=MySQLType.Mysql.value,
                connection=DatabaseConnection(
                    config=SERVICE_CONNECTION,
                ),
            ),
        )

        automation_workflow: Workflow = self.metadata.create_or_update(
            data=new_workflow_request
        )
        engine: Engine = get_connection(SERVICE_CONNECTION)

        test_connection_fn = get_test_connection_fn(SERVICE_CONNECTION)
        test_connection_fn(
            self.metadata, engine, SERVICE_CONNECTION, automation_workflow
        )

        final_workflow: Workflow = self.metadata.get_by_name(
            entity=Workflow, fqn="test-connection-mysql"
        )

        self.assertEqual(final_workflow.status, WorkflowStatus.Successful)
        self.assertEqual(len(final_workflow.response.steps), 4)
        self.assertEqual(
            final_workflow.response.status.value, StatusType.Successful.value
        )

        self.metadata.delete(
            entity=Workflow,
            entity_id=str(automation_workflow.id.__root__),
            hard_delete=True,
        )

    def test_connection_workflow_ko(self):
        """Test connection that will fail"""
        wrong_service_connection = MysqlConnection(
            username="metadata_user",
            authType=BasicAuth(password="metadata_password"),
            hostPort="localhost:8585",  # There's something running there, but it's not MySQL
            databaseSchema="openmetadata_db",
        )

        wrong_workflow_request = CreateWorkflowRequest(
            name="test-connection-mysql-bad",
            description="description",
            workflowType=WorkflowType.TEST_CONNECTION,
            request=TestServiceConnectionRequest(
                serviceType=ServiceType.Database,
                connectionType=MySQLType.Mysql.value,
                connection=DatabaseConnection(
                    config=wrong_service_connection,
                ),
            ),
        )

        automation_workflow: Workflow = self.metadata.create_or_update(
            data=wrong_workflow_request
        )
        engine: Engine = get_connection(wrong_service_connection)

        test_connection_fn = get_test_connection_fn(wrong_service_connection)
        test_connection_fn(
            self.metadata, engine, wrong_service_connection, automation_workflow
        )

        final_workflow: Workflow = self.metadata.get_by_name(
            entity=Workflow, fqn="test-connection-mysql-bad"
        )

        self.assertEqual(final_workflow.response.status, StatusType.Failed)

        self.metadata.delete(
            entity=Workflow,
            entity_id=str(automation_workflow.id.__root__),
            hard_delete=True,
        )
