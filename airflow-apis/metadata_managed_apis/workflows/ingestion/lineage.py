"""
Metadata DAG function builder
"""

from airflow import DAG
from metadata_managed_apis.workflows.ingestion.common import (
    build_dag,
    build_source,
    build_workflow_config_property,
    metadata_ingestion_workflow,
)

from metadata.generated.schema.entity.services.ingestionPipelines.ingestionPipeline import (
    IngestionPipeline,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    MetadataWorkflowConfig,
    Sink,
)


def build_lineage_workflow_config(
    ingestion_pipeline: IngestionPipeline,
) -> MetadataWorkflowConfig:
    """
    Given an airflow_pipeline, prepare the workflow config JSON
    """

    source = build_source(ingestion_pipeline)
    source.type = f"{source.type}-lineage"  # Mark the source as lineage

    workflow_config = MetadataWorkflowConfig(
        source=source,
        sink=Sink(
            type="metadata-rest",
            config={},
        ),
        workflowConfig=build_workflow_config_property(ingestion_pipeline),
        ingestionPipelineFQN=ingestion_pipeline.fullyQualifiedName.__root__,
    )

    return workflow_config


def build_lineage_dag(ingestion_pipeline: IngestionPipeline) -> DAG:
    """
    Build a simple metadata workflow DAG
    """
    workflow_config = build_lineage_workflow_config(ingestion_pipeline)
    dag = build_dag(
        task_name="lineage_task",
        ingestion_pipeline=ingestion_pipeline,
        workflow_config=workflow_config,
        workflow_fn=metadata_ingestion_workflow,
    )

    return dag
