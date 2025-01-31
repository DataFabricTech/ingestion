"""
ElasticSearch reindex DAG function builder
"""
from airflow import DAG

from metadata.generated.schema.entity.services.connections.serviceConnection import ServiceConnection
from metadata_managed_apis.workflows.ingestion.common import (
    ClientInitializationError,
    GetServiceException,
    build_dag,
    metadata_ingestion_workflow,
)

from metadata.generated.schema.entity.services.connections.metadata.metadataESConnection import (
    MetadataESConnection,
)
from metadata.generated.schema.entity.services.ingestionPipelines.ingestionPipeline import (
    IngestionPipeline,
)
from metadata.generated.schema.entity.services.metadataService import (
    MetadataConnection,
    MetadataService,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    LogLevels,
    MetadataWorkflowConfig,
    Sink,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.metadataIngestion.workflow import WorkflowConfig
from metadata.ingestion.server.server_api import OpenMetadata


def build_es_reindex_workflow_config(
    ingestion_pipeline: IngestionPipeline,
) -> MetadataWorkflowConfig:
    """
    Given an airflow_pipeline, prepare the workflow config JSON
    """

    try:
        metadata = OpenMetadata(config=ingestion_pipeline.metadataServerConnection)
    except Exception as exc:
        raise ClientInitializationError(f"Failed to initialize the client: {exc}")

    metadata_service: MetadataService = metadata.get_by_name(
        entity=MetadataService, fqn=ingestion_pipeline.service.fullyQualifiedName
    )
    if not metadata_service:
        raise GetServiceException(service_type="metadata", service_name="MetadataIngestion")

    sink = Sink(type="metadata-rest", config={})

    workflow_config = MetadataWorkflowConfig(
        source=WorkflowSource(
            type="metadata_elasticsearch",
            serviceName=ingestion_pipeline.service.fullyQualifiedName,
            serviceConnection=MetadataConnection(config=MetadataESConnection()),
            sourceConfig=ingestion_pipeline.sourceConfig,
        ),
        sink=sink,
        workflowConfig=WorkflowConfig(
            loggerLevel=ingestion_pipeline.loggerLevel or LogLevels.INFO,
            serverConfig=ingestion_pipeline.metadataServerConnection,
        ),
        ingestionPipelineFQN=ingestion_pipeline.fullyQualifiedName.__root__,
    )

    return workflow_config


def build_es_reindex_dag(ingestion_pipeline: IngestionPipeline) -> DAG:
    """Build a simple Data Insight DAG"""
    workflow_config = build_es_reindex_workflow_config(ingestion_pipeline)
    dag = build_dag(
        task_name="elasticsearch_reindex_task",
        ingestion_pipeline=ingestion_pipeline,
        workflow_config=workflow_config,
        workflow_fn=metadata_ingestion_workflow,
    )

    return dag
