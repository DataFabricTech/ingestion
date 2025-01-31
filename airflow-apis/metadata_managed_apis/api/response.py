import json
from typing import Optional, Union

from airflow.models import DagRun
from flask import Response

from metadata.generated.schema.entity.services.ingestionPipelines.ingestionPipeline import (
    PipelineStatus,
)
from metadata.utils.helpers import datetime_to_ts


class ApiResponse:
    """
    Helper class to respond API calls
    """

    STATUS_OK = 200
    STATUS_BAD_REQUEST = 400
    STATUS_UNAUTHORIZED = 401
    STATUS_NOT_FOUND = 404
    STATUS_SERVER_ERROR = 500

    @staticmethod
    def standard_response(status, response_obj):
        json_data = json.dumps(response_obj)
        resp = Response(json_data, status=status, mimetype="application/json")
        return resp

    @staticmethod
    def success(response_obj: Union[Optional[dict], Optional[list]] = None):
        response_body = response_obj if response_obj is not None else {}
        return ApiResponse.standard_response(ApiResponse.STATUS_OK, response_body)

    @staticmethod
    def error(status, error):
        return ApiResponse.standard_response(status, {"error": error})

    @staticmethod
    def bad_request(error):
        return ApiResponse.error(ApiResponse.STATUS_BAD_REQUEST, error)

    @staticmethod
    def not_found(error="Resource not found"):
        return ApiResponse.error(ApiResponse.STATUS_NOT_FOUND, error)

    @staticmethod
    def unauthorized(error="Not authorized to access this resource"):
        return ApiResponse.error(ApiResponse.STATUS_UNAUTHORIZED, error)

    @staticmethod
    def server_error(error="An unexpected problem occurred"):
        return ApiResponse.error(ApiResponse.STATUS_SERVER_ERROR, error)


class ResponseFormat:
    """
    Handle how to manage responses
    """

    @staticmethod
    def format_dag_run_state(dag_run: DagRun) -> PipelineStatus:
        """
        Build the pipeline status
        """
        return PipelineStatus(
            pipelineState=dag_run.get_state(),
            runId=dag_run.run_id,
            startDate=datetime_to_ts(dag_run.start_date),
            endDate=datetime_to_ts(dag_run.end_date),
            timestamp=datetime_to_ts(dag_run.execution_date),
        )
