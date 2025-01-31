"""
Module containing the logic to trigger a DAG
"""
from typing import Optional

try:
    from airflow.api.common.trigger_dag import trigger_dag
except ImportError:
    from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.utils import timezone
from flask import Response
from metadata_managed_apis.api.response import ApiResponse


def trigger(dag_id: str, run_id: Optional[str]) -> Response:
    dag_run = trigger_dag(
        dag_id=dag_id,
        run_id=run_id,
        conf=None,
        execution_date=timezone.utcnow(),
    )
    return ApiResponse.success(
        {"message": f"Workflow [{dag_id}] has been triggered {dag_run}"}
    )
