"""
Module containing the logic to kill all DAG not finished executions
"""
from typing import List

from airflow import settings
from airflow.models import DagModel, DagRun, TaskInstance
from airflow.utils.state import DagRunState, TaskInstanceState
from flask import Response
from metadata_managed_apis.api.response import ApiResponse


def kill_all(dag_id: str) -> Response:
    """
    Validate that the DAG is registered by Airflow.
    If exists, check the DagRun
    :param dag_id: DAG to find
    :return: API Response
    """

    with settings.Session() as session:
        dag_model = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()

        if not dag_model:
            return ApiResponse.not_found(f"DAG {dag_id} not found.")

        runs: List[DagRun] = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == dag_id,
                DagRun.state.in_((DagRunState.RUNNING, DagRunState.QUEUED)),
            )
            .all()
        )

        instances: List[TaskInstance] = session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.state.notin_(
                (TaskInstanceState.SUCCESS, TaskInstanceState.FAILED)
            ),
        )

        if not runs or not instances:
            return ApiResponse.not_found(
                f"Workflow [{dag_id}] has no running or pending runs nor tasks"
            )

        for dag_run in runs:
            dag_run.set_state(DagRunState.FAILED)

        for instance in instances:
            instance.set_state(TaskInstanceState.FAILED)

        session.commit()

        return ApiResponse.success({"message": f"Workflow [{dag_id}] has been killed"})
