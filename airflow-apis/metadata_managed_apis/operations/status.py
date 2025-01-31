"""
Module containing the logic to check a DAG status
"""
import json

from airflow import settings
from airflow.models import DagModel, DagRun
from airflow.utils.state import DagRunState
from flask import Response
from metadata_managed_apis.api.response import ApiResponse, ResponseFormat


def status(dag_id: str, only_queued: str = None) -> Response:
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

        query = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == dag_id,
            )
            .order_by(DagRun.start_date.desc())
        )

        if only_queued:
            query = query.filter(DagRun.state == DagRunState.QUEUED)

        runs = query.limit(10).all()

        formatted = [
            json.loads(ResponseFormat.format_dag_run_state(dag_run).json())
            for dag_run in runs
        ]

        return ApiResponse.success(formatted)
