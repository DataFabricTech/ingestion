#  Copyright 2021 Collate
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
OpenMetadata base class for tests
"""
import uuid
from datetime import datetime
from textwrap import dedent
from typing import Any, List, Optional, Type

from airflow import DAG
from airflow.operators.bash import BashOperator

from metadata.generated.schema.api.data.createDashboard import CreateDashboardRequest
from metadata.generated.schema.api.data.createDashboardDataModel import (
    CreateDashboardDataModelRequest,
)
from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from metadata.generated.schema.api.data.createDatabaseSchema import (
    CreateDatabaseSchemaRequest,
)
from metadata.generated.schema.api.data.createPipeline import CreatePipelineRequest
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.api.services.createDashboardService import (
    CreateDashboardServiceRequest,
)
from metadata.generated.schema.api.services.createDatabaseService import (
    CreateDatabaseServiceRequest,
)
from metadata.generated.schema.api.services.createPipelineService import (
    CreatePipelineServiceRequest,
)
from metadata.generated.schema.api.teams.createTeam import CreateTeamRequest
from metadata.generated.schema.api.teams.createUser import CreateUserRequest
from metadata.generated.schema.api.tests.createTestCase import CreateTestCaseRequest
from metadata.generated.schema.api.tests.createTestDefinition import (
    CreateTestDefinitionRequest,
)
from metadata.generated.schema.api.tests.createTestSuite import CreateTestSuiteRequest
from metadata.generated.schema.entity.data.dashboard import Dashboard
from metadata.generated.schema.entity.data.dashboardDataModel import (
    DashboardDataModel,
    DataModelType,
)
from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
from metadata.generated.schema.entity.data.pipeline import Pipeline, Task
from metadata.generated.schema.entity.data.table import Column, DataType, Table
from metadata.generated.schema.entity.services.connections.dashboard.lookerConnection import (
    LookerConnection,
)
from metadata.generated.schema.entity.services.connections.database.common.basicAuth import (
    BasicAuth,
)
from metadata.generated.schema.entity.services.connections.database.mysqlConnection import (
    MysqlConnection,
)
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    AuthProvider,
    OpenMetadataConnection,
)
from metadata.generated.schema.entity.services.connections.pipeline.customPipelineConnection import (
    CustomPipelineConnection,
    CustomPipelineType,
)
from metadata.generated.schema.entity.services.dashboardService import (
    DashboardConnection,
    DashboardService,
    DashboardServiceType,
)
from metadata.generated.schema.entity.services.databaseService import (
    DatabaseConnection,
    DatabaseService,
    DatabaseServiceType,
)
from metadata.generated.schema.entity.services.pipelineService import (
    PipelineConnection,
    PipelineService,
    PipelineServiceType,
)
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
    OpenMetadataJWTClientConfig,
)
from metadata.generated.schema.tests.testCase import TestCaseParameterValue
from metadata.generated.schema.tests.testDefinition import (
    TestCaseParameterDefinition,
    TestPlatform,
)
from metadata.generated.schema.type.basic import EntityName, FullyQualifiedEntityName
from metadata.ingestion.models.custom_pydantic import CustomSecretStr
from metadata.ingestion.ometa.ometa_api import C, OpenMetadata, T
from metadata.utils.dispatch import class_register

OM_JWT = "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhZG1pbiIsImlzQm90IjpmYWxzZSwiaXNzIjoib3Blbi1tZXRhZGF0YS5vcmciLCJpYXQiOjE2NjM5Mzg0NjIsImVtYWlsIjoiYWRtaW5Ab3Blbm1ldGFkYXRhLm9yZyJ9.tS8um_5DKu7HgzGBzS1VTA5uUjKWOCU0B_j08WXBiEC0mr0zNREkqVfwFDD-d24HlNEbrqioLsBuFRiwIWKc1m_ZlVQbG7P36RUxhuv2vbSp80FKyNM-Tj93FDzq91jsyNmsQhyNv_fNr3TXfzzSPjHt8Go0FMMP66weoKMgW2PbXlhVKwEuXUHyakLLzewm9UMeQaEiRzhiTMU3UkLXcKbYEJJvfNFcLwSl9W8JCO_l0Yj3ud-qt_nQYEZwqW6u5nfdQllN133iikV4fM5QZsMCnm8Rq1mvLR0y9bmJiD7fwM1tmJ791TUWqmKaTnP49U493VanKpUAfzIiOiIbhg"
COLUMNS = [
    Column(name="id", dataType=DataType.BIGINT),
    Column(name="another", dataType=DataType.BIGINT),
    Column(
        name="struct",
        dataType=DataType.STRUCT,
        children=[
            Column(name="id", dataType=DataType.INT),
            Column(name="name", dataType=DataType.STRING),
        ],
    ),
]

METADATA_INGESTION_CONFIG_TEMPLATE = dedent(
    """{{
        "source": {{
            "type": "{type}",
            "serviceName": "{service_name}",
            "serviceConnection": {{
                "config": {service_config}
            }},
            "sourceConfig": {{"config": {source_config} }}
        }},
        "sink": {{"type": "metadata-rest", "config": {{}}}},
        "workflowConfig": {{
            "loggerLevel": "DEBUG",
            "openMetadataServerConfig": {{
                "hostPort": "{hostport}",
                "authProvider": "openmetadata",
                "securityConfig": {{
                    "jwtToken": "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJyb2xlcyI6WyJJbmdlc3Rpb25Cb3RSb2xlIl0sImVtYWlsIjoiaW5nZXN0aW9uLWJvdEBvcGVubWV0YWRhdGEub3JnIiwiaXNCb3QiOnRydWUsInRva2VuVHlwZSI6IkJPVCIsImlhdCI6MTcxNjAxNzExMiwiZXhwIjpudWxsfQ.MgzchNADcN3nyYKz2LmAg1rGREYQEJPVyfvUTvIlqLYgV7_D9EUezctL9hpPYP_TUomHPezWNmkb5SfSyLGnGQa0N7m9QilZpKdSqNF8gE10D16fAolluwtaDqNunNQesIzoj1Pn5HLkOUexkLlYNVE9XtgL1eXR_feLWuzUIfjO6zlmaMuN6IFtADIcQy1LGRp-IP4gam0bwMVAGLe-_0_Sn_o5HvkznZmN1gssJ5nTFc8v-GrE7BwM3Rd4dqLSabiWf_EyleFf34oP6PEg7-TZidxqPtDqTxbPdqbN6mSv-Zilc92qXB6GlHWHMV9iQMRK5n8sGTK19PTDIYj6dA"
                }}
            }}
        }}
    }}"""
)

PROFILER_INGESTION_CONFIG_TEMPLATE = dedent(
    """{{
        "source": {{
            "type": "{type}",
            "serviceName": "{service_name}",
            "serviceConnection": {{
                "config": {service_config}
            }},
            "sourceConfig": {{
                "config": {{
                    "type":"{Profiler}", 
                    "generateSampleData": true, 
                    "profileSampleType": "PERCENTAGE", 
                    "profileSample": 60,
                    "sampleDataCount": 100
                }}
            }}
        }},
        "processor": {{"type": "orm-profiler", "config": {{}}}},
        "sink": {{"type": "metadata-rest", "config": {{}}}},
        "workflowConfig": {{
            "loggerLevel": "DEBUG",
            "openMetadataServerConfig": {{
                "hostPort": "{hostport}",
                "authProvider": "openmetadata",
                "securityConfig": {{
                    "jwtToken": "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJyb2xlcyI6WyJJbmdlc3Rpb25Cb3RSb2xlIl0sImVtYWlsIjoiaW5nZXN0aW9uLWJvdEBvcGVubWV0YWRhdGEub3JnIiwiaXNCb3QiOnRydWUsInRva2VuVHlwZSI6IkJPVCIsImlhdCI6MTcxNjAxNzExMiwiZXhwIjpudWxsfQ.MgzchNADcN3nyYKz2LmAg1rGREYQEJPVyfvUTvIlqLYgV7_D9EUezctL9hpPYP_TUomHPezWNmkb5SfSyLGnGQa0N7m9QilZpKdSqNF8gE10D16fAolluwtaDqNunNQesIzoj1Pn5HLkOUexkLlYNVE9XtgL1eXR_feLWuzUIfjO6zlmaMuN6IFtADIcQy1LGRp-IP4gam0bwMVAGLe-_0_Sn_o5HvkznZmN1gssJ5nTFc8v-GrE7BwM3Rd4dqLSabiWf_EyleFf34oP6PEg7-TZidxqPtDqTxbPdqbN6mSv-Zilc92qXB6GlHWHMV9iQMRK5n8sGTK19PTDIYj6dA"
                }}
            }}
        }}
    }}"""
)

MINIO_PROFILER_TEST = """
{
    "source": {
        "type": "minio", 
        "serviceName": "fabric-minio", 
        "serviceConnection": {
            "config": {
                "type": "MinIO", 
                "minioConfig": {
                    "accessKeyId": "fabric", 
                    "secretKey": "fabric12##", 
                    "sessionToken": null, 
                    "region": null, 
                    "endPointURL": "http://192.168.106.12:9000"
                }, 
                "bucketNames": [], 
                "supportsMetadataExtraction": true, 
                "supportsStorageProfiler": true
            }
        }, 
        "sourceConfig": {
            "config": {
                "type": "StorageProfiler", 
                "bucketFilterPattern": {
                    "includes": ["datafabric"],
                    "excludes": []
                }, 
                "containerFilterPattern": null, 
                "useFqnForFiltering": false, 
                "generateSampleData": true, 
                "computeMetrics": true, 
                "processPiiSensitive": false, 
                "confidence": 80.0, 
                "profileSampleType": "PERCENTAGE", 
                "profileSample": 60, 
                "sampleDataCount": 70, 
                "threadCount": 5.0, 
                "timeoutSeconds": 43200
            }
        }
    }, 
    "processor": {
        "type": "orm-profiler", "config": {}
    }, 
    "sink": {
        "type": "metadata-rest", "config": {}
    }, 
    "workflowConfig": {
        "loggerLevel": "DEBUG", 
        "openMetadataServerConfig": {
            "hostPort": "http://192.168.105.51:8585/api", 
            "authProvider": "openmetadata",
            "securityConfig": {
                "jwtToken": "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJyb2xlcyI6WyJJbmdlc3Rpb25Cb3RSb2xlIl0sImVtYWlsIjoiaW5nZXN0aW9uLWJvdEBtZXRhZGF0YS5vcmciLCJpc0JvdCI6dHJ1ZSwidG9rZW5UeXBlIjoiQk9UIiwiaWF0IjoxNzI3MDgwNTY1LCJleHAiOm51bGx9.pL8fVWI9QPjL8Cc5jxereIbvNmO_CDTXp0_LotFGywltaX-42N-IZVSkGLJcG7sTCqqPnERQyFghVV1i45bPXHcXYeq9k0LQkfDOEnV4dYcN0RPwUK35EKeidM5UiRXr39sjy8qSS_OAH7KraWX_wucZPXoPLzrHGgo2N_8yl-huaLLhzEwPqggsofOFyaVd6MMCr4HLxIniRMBOJLU3qnRcj--HINru3dK9I_mM8uESJNfl9wRmObhXyRwUrVIqTMejG6XgjO6L4USiyKOlMyIbupgFvq7SP2lL-soEzT7mw_mZOWD1ZdUWoasCRjysFpqQ3pCwf94XTAxWtTfppA"
            }
        }
    } 
}"""


def int_admin_ometa(url: str = "http://localhost:8585/api") -> OpenMetadata:
    """Initialize the ometa connection with default admin:admin creds"""
    server_config = OpenMetadataConnection(
        hostPort=url,
        authProvider=AuthProvider.openmetadata,
        securityConfig=OpenMetadataJWTClientConfig(jwtToken=CustomSecretStr(OM_JWT)),
    )
    metadata = OpenMetadata(server_config)
    assert metadata.health_check()
    return metadata


def generate_name() -> EntityName:
    """Generate a random for the asset"""
    return EntityName(__root__=str(uuid.uuid4()))


create_service_registry = class_register()


def get_create_service(entity: Type[T], name: Optional[EntityName] = None) -> C:
    """Create a vanilla service based on the input type"""
    func = create_service_registry.registry.get(entity.__name__)
    if not func:
        raise ValueError(
            f"Create Service for type {entity.__name__} has not yet been implemented. Add it on `integration_base.py`"
        )

    if not name:
        name = generate_name()

    return func(name)


@create_service_registry.add(PipelineService)
def _(name: EntityName) -> C:
    """Prepare a Create service request"""
    return CreatePipelineServiceRequest(
        name=name,
        serviceType=PipelineServiceType.CustomPipeline,
        connection=PipelineConnection(
            config=CustomPipelineConnection(type=CustomPipelineType.CustomPipeline)
        ),
    )


@create_service_registry.add(DatabaseService)
def _(name: EntityName) -> C:
    """Prepare a Create service request"""
    return CreateDatabaseServiceRequest(
        name=name,
        serviceType=DatabaseServiceType.Mysql,
        connection=DatabaseConnection(
            config=MysqlConnection(
                username="username",
                authType=BasicAuth(
                    password="password",
                ),
                hostPort="http://localhost:1234",
            )
        ),
    )


@create_service_registry.add(DashboardService)
def _(name: EntityName) -> C:
    """Prepare a Create service request"""
    return CreateDashboardServiceRequest(
        name=name,
        serviceType=DashboardServiceType.Looker,
        connection=DashboardConnection(
            config=LookerConnection(
                hostPort="http://hostPort", clientId="id", clientSecret="secret"
            )
        ),
    )


create_entity_registry = class_register()


def get_create_entity(
    entity: Type[T],
    reference: Any,
    name: Optional[EntityName] = None,
) -> C:
    """Create a vanilla entity based on the input type"""
    func = create_entity_registry.registry.get(entity.__name__)
    if not func:
        raise ValueError(
            f"Create Service for type {entity.__name__} has not yet been implemented. Add it on `integration_base.py`"
        )

    if not name:
        name = generate_name()

    return func(reference, name)


@create_entity_registry.add(Pipeline)
def _(reference: FullyQualifiedEntityName, name: EntityName) -> C:
    return CreatePipelineRequest(
        name=name,
        service=reference,
        tasks=[
            Task(name="task1"),
            Task(name="task2", downstreamTasks=["task1"]),
            Task(name="task3", downstreamTasks=["task2"]),
            Task(name="task4", downstreamTasks=["task2"]),
        ],
    )


@create_entity_registry.add(Database)
def _(reference: FullyQualifiedEntityName, name: EntityName) -> C:
    return CreateDatabaseRequest(
        name=name,
        service=reference,
    )


@create_entity_registry.add(DatabaseSchema)
def _(reference: FullyQualifiedEntityName, name: EntityName) -> C:
    return CreateDatabaseSchemaRequest(
        name=name,
        database=reference,
    )


@create_entity_registry.add(Table)
def _(reference: FullyQualifiedEntityName, name: EntityName) -> C:
    return CreateTableRequest(
        name=name,
        databaseSchema=reference,
        columns=COLUMNS,
    )


@create_entity_registry.add(Dashboard)
def _(reference: FullyQualifiedEntityName, name: EntityName) -> C:
    return CreateDashboardRequest(
        name=name,
        service=reference,
    )


@create_entity_registry.add(DashboardDataModel)
def _(reference: FullyQualifiedEntityName, name: EntityName) -> C:
    return CreateDashboardDataModelRequest(
        name=name,
        service=reference,
        dataModelType=DataModelType.LookMlExplore,
        columns=COLUMNS,
    )


def get_create_user_entity(
    name: Optional[EntityName] = None, email: Optional[str] = None
):
    if not name:
        name = generate_name()
    if not email:
        email = f"{generate_name().__root__}@getcollate.io"
    return CreateUserRequest(name=name, email=email)


def get_create_team_entity(name: Optional[EntityName] = None, users=List[str]):
    if not name:
        name = generate_name()
    return CreateTeamRequest(name=name, teamType="Group", users=users)


def get_create_test_definition(
    parameter_definition: List[TestCaseParameterDefinition],
    entity_type: [T],
    name: Optional[EntityName] = None,
    description: Optional[str] = None,
):
    if not name:
        name = generate_name()
    if not description:
        description = generate_name().__root__
    return CreateTestDefinitionRequest(
        name=name,
        description=description,
        entityType=entity_type,
        testPlatforms=[TestPlatform.GreatExpectations],
        parameterDefinition=parameter_definition,
    )


def get_create_test_suite(
    executable_entity_reference: str,
    name: Optional[EntityName] = None,
    description: Optional[str] = None,
):
    if not name:
        name = generate_name()
    if not description:
        description = generate_name().__root__
    return CreateTestSuiteRequest(
        name=name,
        description=description,
        executableEntityReference=executable_entity_reference,
    )


def get_create_test_case(
    entity_link: str,
    test_suite: FullyQualifiedEntityName,
    test_definition: FullyQualifiedEntityName,
    parameter_values: List[TestCaseParameterValue],
    name: Optional[EntityName] = None,
):
    if not name:
        name = generate_name()
    return CreateTestCaseRequest(
        name=name,
        entityLink=entity_link,
        testSuite=test_suite,
        testDefinition=test_definition,
        parameterValues=parameter_values,
    )


def get_test_dag(name: str) -> DAG:
    """Get a DAG with the tasks created in the CreatePipelineRequest"""
    with DAG(name, start_date=datetime(2021, 1, 1)) as dag:
        tasks = [
            BashOperator(
                task_id=task_id,
                bash_command="date",
            )
            for task_id in ("task1", "task2", "task3", "task4")
        ]

        tasks[0] >> tasks[1] >> [tasks[2], tasks[3]]

    return dag
