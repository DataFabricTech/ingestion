from airflow import DAG

# these are params only used in the DAG factory, not in the tasks
from metadata_managed_apis.utils.logger import workflow_logger
from metadata_managed_apis.workflows.ingestion.registry import build_registry

from metadata.generated.schema.entity.services.ingestionPipelines.ingestionPipeline import (
    IngestionPipeline,
)

logger = workflow_logger()


class WorkflowBuilder:
    """
    Generates tasks and a DAG from a config.
    :param workflow_config:  configuration for the DAG
    """

    def __init__(self, ingestion_pipeline: IngestionPipeline) -> None:
        self.airflow_pipeline = ingestion_pipeline
        self.dag_name: str = self.airflow_pipeline.name.__root__

    def build(self) -> DAG:
        """
        Generates a DAG from the DAG parameters.

        :returns: dict with dag_id and DAG object
        :type: Dict[str, Union[str, DAG]]
        """
        dag_type = self.airflow_pipeline.pipelineType.value

        build_fn = build_registry.registry.get(dag_type)
        if not build_fn:
            msg = f"Cannot find build function for {dag_type} in {build_registry.registry}"
            logger.error(msg)
            raise ValueError(msg)

        dag = build_fn(self.airflow_pipeline)
        if not isinstance(dag, DAG):
            msg = f"Invalid return type from {build_fn.__name__} when building {dag_type}."
            logger.error(msg)
            raise ValueError(msg)

        return dag
