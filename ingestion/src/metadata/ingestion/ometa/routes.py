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
OMeta API endpoints
"""
# from src.metadata.generated.schema.analytics.webAnalyticEventData import (
#     WebAnalyticEventData,
# )
# from src.metadata.generated.schema.api.automations.createWorkflow import (
#     CreateWorkflowRequest,
# )
# from src.metadata.generated.schema.api.classification.createClassification import (
#     CreateClassificationRequest,
# )
# from src.metadata.generated.schema.api.classification.createTag import CreateTagRequest
# from src.metadata.generated.schema.api.data.createChart import CreateChartRequest
from src.metadata.generated.schema.api.data.createContainer import CreateContainerRequest
from src.metadata.generated.schema.api.data.createDirectory import CreateDirectoryRequest
# from src.metadata.generated.schema.api.data.createDashboard import CreateDashboardRequest
# from src.metadata.generated.schema.api.data.createDashboardDataModel import (
#     CreateDashboardDataModelRequest,
# )
from src.metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from src.metadata.generated.schema.api.data.createDatabaseSchema import (
    CreateDatabaseSchemaRequest,
)
# from src.metadata.generated.schema.api.data.createGlossary import CreateGlossaryRequest
# from src.metadata.generated.schema.api.data.createGlossaryTerm import (
#     CreateGlossaryTermRequest,
# )
# from src.metadata.generated.schema.api.data.createMlModel import CreateMlModelRequest
from src.metadata.generated.schema.api.data.createPipeline import CreatePipelineRequest
# from src.metadata.generated.schema.api.data.createQuery import CreateQueryRequest
from src.metadata.generated.schema.api.data.createSearchIndex import (
    CreateSearchIndexRequest,
)
# from src.metadata.generated.schema.api.data.createStoredProcedure import (
#     CreateStoredProcedureRequest,
# )
from src.metadata.generated.schema.api.data.createTable import CreateTableRequest
# from src.metadata.generated.schema.api.data.createTopic import CreateTopicRequest
# from src.metadata.generated.schema.api.domains.createDataProduct import (
#     CreateDataProductRequest,
# )
# from src.metadata.generated.schema.api.domains.createDomain import CreateDomainRequest
# from src.metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
# from src.metadata.generated.schema.api.policies.createPolicy import CreatePolicyRequest
# from src.metadata.generated.schema.api.services.createDashboardService import (
#     CreateDashboardServiceRequest,
# )
from src.metadata.generated.schema.api.services.createDatabaseService import (
    CreateDatabaseServiceRequest,
)
# from src.metadata.generated.schema.api.services.createMessagingService import (
#     CreateMessagingServiceRequest,
# )
# from src.metadata.generated.schema.api.services.createMetadataService import (
#     CreateMetadataServiceRequest,
# )
# from src.metadata.generated.schema.api.services.createMlModelService import (
#     CreateMlModelServiceRequest,
# )
from src.metadata.generated.schema.api.services.createPipelineService import (
    CreatePipelineServiceRequest,
)
from src.metadata.generated.schema.api.services.createSearchService import (
    CreateSearchServiceRequest,
)
from src.metadata.generated.schema.api.services.createStorageService import (
    CreateStorageServiceRequest,
)
from src.metadata.generated.schema.api.services.createFilesystemService import (
    CreateFilesystemServiceRequest
)
from src.metadata.generated.schema.api.services.ingestionPipelines.createIngestionPipeline import (
    CreateIngestionPipelineRequest,
)
# from src.metadata.generated.schema.api.teams.createRole import CreateRoleRequest
# from src.metadata.generated.schema.api.teams.createTeam import CreateTeamRequest
# from src.metadata.generated.schema.api.teams.createUser import CreateUserRequest
# from src.metadata.generated.schema.api.tests.createTestCase import CreateTestCaseRequest
# from src.metadata.generated.schema.api.tests.createTestDefinition import (
#     CreateTestDefinitionRequest,
# )
# from src.metadata.generated.schema.api.tests.createTestSuite import CreateTestSuiteRequest
# from src.metadata.generated.schema.dataInsight.dataInsightChart import DataInsightChart
# from src.metadata.generated.schema.dataInsight.kpi.kpi import Kpi
# from src.metadata.generated.schema.entity.automations.workflow import Workflow
# from src.metadata.generated.schema.entity.classification.classification import (
#     Classification,
# )
# from src.metadata.generated.schema.entity.classification.tag import Tag
# from src.metadata.generated.schema.entity.data.chart import Chart
from src.metadata.generated.schema.entity.data.container import Container
from src.metadata.generated.schema.entity.data.directory import Directory
# from src.metadata.generated.schema.entity.data.dashboard import Dashboard
# from src.metadata.generated.schema.entity.data.dashboardDataModel import DashboardDataModel
from src.metadata.generated.schema.entity.data.database import Database
from src.metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
# from src.metadata.generated.schema.entity.data.glossary import Glossary
# from src.metadata.generated.schema.entity.data.glossaryTerm import GlossaryTerm
# from src.metadata.generated.schema.entity.data.metrics import Metrics
# from src.metadata.generated.schema.entity.data.mlmodel import MlModel
from src.metadata.generated.schema.entity.data.pipeline import Pipeline
# from src.metadata.generated.schema.entity.data.query import Query
# from src.metadata.generated.schema.entity.data.report import Report
from src.metadata.generated.schema.entity.data.searchIndex import SearchIndex
# from src.metadata.generated.schema.entity.data.storedProcedure import StoredProcedure
from src.metadata.generated.schema.entity.data.table import Table
# from src.metadata.generated.schema.entity.data.topic import Topic
from src.metadata.generated.schema.entity.data.ingestion import IngestionCheck, IngestionStatus
# from src.metadata.generated.schema.entity.domains.dataProduct import DataProduct
# from src.metadata.generated.schema.entity.domains.domain import Domain
# from src.metadata.generated.schema.entity.policies.policy import Policy
from src.metadata.generated.schema.entity.services.connections.testConnectionDefinition import (
    TestConnectionDefinition,
)
# from src.metadata.generated.schema.entity.services.dashboardService import DashboardService
from src.metadata.generated.schema.entity.services.databaseService import DatabaseService
from src.metadata.generated.schema.entity.services.ingestionPipelines.ingestionPipeline import (
    IngestionPipeline,
)
# from src.metadata.generated.schema.entity.services.messagingService import MessagingService
# from src.metadata.generated.schema.entity.services.metadataService import MetadataService
# from src.metadata.generated.schema.entity.services.mlmodelService import MlModelService
from src.metadata.generated.schema.entity.services.pipelineService import PipelineService
from src.metadata.generated.schema.entity.services.searchService import SearchService
from src.metadata.generated.schema.entity.services.storageService import StorageService
# from src.metadata.generated.schema.entity.teams.role import Role
# from src.metadata.generated.schema.entity.teams.team import Team
# from src.metadata.generated.schema.entity.teams.user import AuthenticationMechanism, User
# from src.metadata.generated.schema.tests.testCase import TestCase
# from src.metadata.generated.schema.tests.testDefinition import TestDefinition
# from src.metadata.generated.schema.tests.testSuite import TestSuite

ROUTES = {
    # MlModel.__name__: "/mlmodels",
    # CreateMlModelRequest.__name__: "/mlmodels",
    # Chart.__name__: "/charts",
    # CreateChartRequest.__name__: "/charts",
    # DashboardDataModel.__name__: "/dashboard/datamodels",
    # CreateDashboardDataModelRequest.__name__: "/dashboard/datamodels",
    # Dashboard.__name__: "/dashboards",
    # CreateDashboardRequest.__name__: "/dashboards",
    Database.__name__: "/databases",
    CreateDatabaseRequest.__name__: "/databases",
    DatabaseSchema.__name__: "/databaseSchemas",
    CreateDatabaseSchemaRequest.__name__: "/databaseSchemas",
    Pipeline.__name__: "/pipelines",
    CreatePipelineRequest.__name__: "/pipelines",
    Table.__name__: "/tables",
    CreateTableRequest.__name__: "/tables",
    # Topic.__name__: "/topics",
    # CreateTopicRequest.__name__: "/topics",
    # Metrics.__name__: "/metrics",
    # AddLineageRequest.__name__: "/lineage",
    # Report.__name__: "/reports",
    # Query.__name__: "/queries",
    # CreateQueryRequest.__name__: "/queries",
    Container.__name__: "/containers",
    CreateContainerRequest.__name__: "/containers",
    Directory.__name__: "/directory",
    CreateDirectoryRequest.__name__: "/directory",
    SearchIndex.__name__: "/searchIndexes",
    CreateSearchIndexRequest.__name__: "/searchIndexes",
    # StoredProcedure.__name__: "/storedProcedures",
    # CreateStoredProcedureRequest.__name__: "/storedProcedures",
    # Classifications
    # Tag.__name__: "/tags",
    # CreateTagRequest.__name__: "/tags",
    # Classification.__name__: "/classifications",
    # CreateClassificationRequest.__name__: "/classifications",
    # Glossaries
    # Glossary.__name__: "/glossaries",
    # CreateGlossaryRequest.__name__: "/glossaries",
    # GlossaryTerm.__name__: "/glossaryTerms",
    # CreateGlossaryTermRequest.__name__: "/glossaryTerms",
    # Users
    # Team.__name__: "/teams",
    # CreateTeamRequest.__name__: "/teams",
    # User.__name__: "/users",
    # CreateUserRequest.__name__: "/users",
    # AuthenticationMechanism.__name__: "/users/auth-mechanism",
    # Roles
    # Role.__name__: "/roles",
    # CreateRoleRequest.__name__: "/roles",
    # Policy.__name__: "/policies",
    # CreatePolicyRequest.__name__: "/policies",
    # Automations
    # Workflow.__name__: "/automations/workflows",
    # CreateWorkflowRequest.__name__: "/automations/workflows",
    # Services
    DatabaseService.__name__: "/services/databaseServices",
    CreateDatabaseServiceRequest.__name__: "/services/databaseServices",
    # DashboardService.__name__: "/services/dashboardServices",
    # CreateDashboardServiceRequest.__name__: "/services/dashboardServices",
    # MessagingService.__name__: "/services/messagingServices",
    # CreateMessagingServiceRequest.__name__: "/services/messagingServices",
    PipelineService.__name__: "/services/pipelineServices",
    CreatePipelineServiceRequest.__name__: "/services/pipelineServices",
    StorageService.__name__: "/services/storageServices",
    CreateStorageServiceRequest.__name__: "/services/storageServices",
    CreateFilesystemServiceRequest.__name__: "/services/filesystemServices",
    # MlModelService.__name__: "/services/mlmodelServices",
    # CreateMlModelServiceRequest.__name__: "/services/mlmodelServices",
    # MetadataService.__name__: "/services/metadataServices",
    # CreateMetadataServiceRequest.__name__: "/services/metadataServices",
    SearchService.__name__: "/services/searchServices",
    CreateSearchServiceRequest.__name__: "/services/searchServices",
    IngestionPipeline.__name__: "/services/ingestionPipelines",
    CreateIngestionPipelineRequest.__name__: "/services/ingestionPipelines",
    TestConnectionDefinition.__name__: "/services/testConnectionDefinitions",
    # Data Quality
    # TestDefinition.__name__: "/dataQuality/testDefinitions",
    # CreateTestDefinitionRequest.__name__: "/dataQuality/testDefinitions",
    # TestSuite.__name__: "/dataQuality/testSuites",
    # CreateTestSuiteRequest.__name__: "/dataQuality/testSuites",
    # TestCase.__name__: "/dataQuality/testCases",
    # CreateTestCaseRequest.__name__: "/dataQuality/testCases",
    # Analytics
    # WebAnalyticEventData.__name__: "/analytics/web/events/collect",
    # DataInsightChart.__name__: "/analytics/dataInsights/charts",
    # Kpi.__name__: "/kpi",
    # Domains & Data Products
    # Domain.__name__: "/domains",
    # CreateDomainRequest.__name__: "/domains",
    # DataProduct.__name__: "/dataProducts",
    # CreateDataProductRequest.__name__: "/dataProducts",
    IngestionCheck.__name__: "/ingestion/check",
    IngestionStatus.__name__: "/ingestion/collector/status"
}
