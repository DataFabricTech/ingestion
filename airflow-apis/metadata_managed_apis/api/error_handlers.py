"""
Register error handlers
"""

from metadata_managed_apis.api.app import blueprint
from metadata_managed_apis.api.response import ApiResponse
from metadata_managed_apis.api.utils import MissingArgException
from metadata_managed_apis.utils.logger import api_logger
from werkzeug.exceptions import HTTPException

logger = api_logger()


@blueprint.app_errorhandler(Exception)
def handle_any_error(exc):
    logger.exception("Wild exception: {exc}")
    if isinstance(exc, HTTPException):
        return ApiResponse.error(exc.code, repr(exc))
    return ApiResponse.server_error(repr(exc))


@blueprint.app_errorhandler(MissingArgException)
def handle_missing_arg(exc):
    logger.exception(f"Missing Argument Exception: {exc}")
    return ApiResponse.bad_request(repr(exc))
