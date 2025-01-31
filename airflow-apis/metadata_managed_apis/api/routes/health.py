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
    from airflow.www.app import csrf

    @blueprint.route("/health", methods=["GET"])
    @csrf.exempt
    def health():
        """
        /health endpoint to check Airflow REST status without auth
        """
        logger.info("metadata health")

        return health_response()

    return health
