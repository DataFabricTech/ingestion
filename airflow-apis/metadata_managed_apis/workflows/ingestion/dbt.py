# Copyright 2024 Mobigen;
# Licensed under the Apache License, Version 2.0 (the "License");
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
Metadata DAG function builder
"""

from airflow import DAG
from metadata_managed_apis.workflows.ingestion.common import (
    build_dag,
    build_source,
    build_workflow_config_property,
    metadata_ingestion_workflow,
)

try:
    pass
except ModuleNotFoundError:
    pass

from metadata.generated.schema.entity.services.ingestionPipelines.ingestionPipeline import (
    IngestionPipeline,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    MetadataWorkflowConfig,
    Sink,
)


def build_dbt_workflow_config(
    ingestion_pipeline: IngestionPipeline,
) -> MetadataWorkflowConfig:
    """
    Given an airflow_pipeline, prepare the workflow config JSON
    """

    source = build_source(ingestion_pipeline)
    source.type = f"dbt"  # Mark the source as dbt

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


def build_dbt_dag(ingestion_pipeline: IngestionPipeline) -> DAG:
    """
    Build a simple metadata workflow DAG
    """
    workflow_config = build_dbt_workflow_config(ingestion_pipeline)
    dag = build_dag(
        task_name="dbt_task",
        ingestion_pipeline=ingestion_pipeline,
        workflow_config=workflow_config,
        workflow_fn=metadata_ingestion_workflow,
    )

    return dag
