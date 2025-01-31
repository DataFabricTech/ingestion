"""
Trigger endpoint
"""
import traceback
from typing import Callable

from flask import Blueprint, Response, request
from metadata_managed_apis.api.response import ApiResponse
from metadata_managed_apis.api.utils import get_request_arg, get_request_dag_id
from metadata_managed_apis.operations.trigger import trigger
from metadata_managed_apis.utils.logger import routes_logger

logger = routes_logger()


def get_fn(blueprint: Blueprint) -> Callable:
    """
    Return the function loaded to a route
    :param blueprint: Flask Blueprint to assign route to
    :return: routed function
    """

    # Lazy import the requirements
    # pylint: disable=import-outside-toplevel
    from airflow.api_connexion import security
    from airflow.security import permissions
    from airflow.www.app import csrf

    @blueprint.route("/trigger", methods=["POST"])
    @csrf.exempt
    @security.requires_access([(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG)])
    def trigger_dag() -> Response:
        """
        Trigger a dag run
        """
        dag_id = get_request_dag_id()

        try:
            run_id = get_request_arg(request, "run_id", raise_missing=False)
            response = trigger(dag_id, run_id)

            return response

        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.error(f"Failed to trigger dag [{dag_id}]: {exc}")
            return ApiResponse.error(
                status=ApiResponse.STATUS_SERVER_ERROR,
                error=f"Workflow [{dag_id}] has filed to trigger due to [{exc}] ",
            )

    return trigger_dag
