"""
Health endpoint. Globally accessible
"""
from typing import Callable

from flask import Blueprint
from metadata_managed_apis.operations.health import health_response
from metadata_managed_apis.utils.logger import routes_logger

try:
    pass
except ImportError:
    pass

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

    @blueprint.route("/health-auth", methods=["GET"])
    @csrf.exempt
    @security.requires_access(
        [(permissions.ACTION_CAN_CREATE, permissions.RESOURCE_DAG)]
    )
    def health_auth():
        """
        /auth-health endpoint to check Airflow REST status without auth
        """

        return health_response()

    return health_auth
