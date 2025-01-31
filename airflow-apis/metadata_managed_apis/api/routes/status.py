"""
Return a list of the 10 last status for the ingestion Pipeline
"""
import traceback
from typing import Callable

from flask import Blueprint, Response
from metadata_managed_apis.api.response import ApiResponse
from metadata_managed_apis.api.utils import get_arg_dag_id, get_arg_only_queued
from metadata_managed_apis.operations.status import status
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

    @blueprint.route("/status", methods=["GET"])
    @csrf.exempt
    @security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)])
    def dag_status() -> Response:
        """
        Check the status of a DAG runs
        """
        dag_id = get_arg_dag_id()
        only_queued = get_arg_only_queued()
        try:
            return status(dag_id, only_queued)

        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.error(f"Failed to get dag [{dag_id}] status: {exc}")
            return ApiResponse.error(
                status=ApiResponse.STATUS_SERVER_ERROR,
                error=f"Failed to get status for [{dag_id}] due to [{exc}] ",
            )

    return dag_status
