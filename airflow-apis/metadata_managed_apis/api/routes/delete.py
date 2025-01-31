"""
Delete the DAG in Airflow's db, as well as the python file
"""
import traceback
from typing import Callable

from flask import Blueprint, Response
from metadata_managed_apis.api.response import ApiResponse
from metadata_managed_apis.api.utils import get_arg_dag_id
from metadata_managed_apis.operations.delete import delete_dag_id
from metadata_managed_apis.utils.logger import routes_logger
from werkzeug.utils import secure_filename

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

    @blueprint.route("/delete", methods=["DELETE"])
    @csrf.exempt
    @security.requires_access(
        [(permissions.ACTION_CAN_DELETE, permissions.RESOURCE_DAG)]
    )
    def delete_dag() -> Response:
        """
        POST request to DELETE a DAG.

        Expect: POST
        {
            "workflow_name": "my_ingestion_pipeline3"
        }
        """
        dag_id = get_arg_dag_id()
        # Sanitize Path Traversal, as later on we clean files based on the DAG id
        secure_dag_id = secure_filename(dag_id)

        try:
            return delete_dag_id(secure_dag_id)

        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.error(
                f"Failed to delete dag [{dag_id}] [secured: {secure_dag_id}]: {exc}"
            )
            return ApiResponse.error(
                status=ApiResponse.STATUS_SERVER_ERROR,
                error=f"Failed to delete [{dag_id}] [secured: {secure_dag_id}] due to [{exc}] ",
            )

    return delete_dag
