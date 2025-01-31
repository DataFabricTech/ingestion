"""
Profiler DAG function builder
"""
import json

from airflow import DAG
from metadata_managed_apis.utils.logger import set_operator_logger
from metadata_managed_apis.workflows.ingestion.common import build_dag, build_source

from metadata.generated.schema.entity.services.ingestionPipelines.ingestionPipeline import (
    IngestionPipeline,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    LogLevels,
    MetadataWorkflowConfig,
    Processor,
    Sink,
    WorkflowConfig,
)
from metadata.ingestion.models.encoders import show_secrets_encoder
from metadata.workflow.profiler import ProfilerWorkflow
from metadata.workflow.workflow_output_handler import print_status


def profiler_workflow(workflow_config: MetadataWorkflowConfig):
    """
    Task that creates and runs the profiler workflow.

    The workflow_config gets cooked form the incoming
    ingestionPipeline.

    This is the callable used to create the PythonOperator
    """

    set_operator_logger(workflow_config)

    config = json.loads(workflow_config.json(encoder=show_secrets_encoder))
    workflow = ProfilerWorkflow.create(config)

    workflow.execute()
    workflow.raise_from_status()
    print_status(workflow)
    workflow.stop()


def build_profiler_workflow_config(
    ingestion_pipeline: IngestionPipeline,
) -> MetadataWorkflowConfig:
    """
    Given an airflow_pipeline, prepare the workflow config JSON
    """
    workflow_config = MetadataWorkflowConfig(
        source=build_source(ingestion_pipeline),
        sink=Sink(
            type="metadata-rest",
            config={},
        ),
        processor=Processor(
            type="orm-profiler",
            config={},
        ),
        workflowConfig=WorkflowConfig(
            loggerLevel=ingestion_pipeline.loggerLevel or LogLevels.INFO,
            serverConfig=ingestion_pipeline.metadataServerConnection,
        ),
        ingestionPipelineFQN=ingestion_pipeline.fullyQualifiedName.__root__,
    )

    return workflow_config


def build_profiler_dag(ingestion_pipeline: IngestionPipeline) -> DAG:
    """
    Build a simple metadata workflow DAG
    """
    workflow_config = build_profiler_workflow_config(ingestion_pipeline)
    dag = build_dag(
        task_name="profiler_task",
        ingestion_pipeline=ingestion_pipeline,
        workflow_config=workflow_config,
        workflow_fn=profiler_workflow,
    )

    return dag
