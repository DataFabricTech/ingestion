"""
Common health validation, for auth and non-auth endpoint
"""
import traceback

from metadata_managed_apis.utils.logger import operations_logger

try:
    from importlib.metadata import version
except ImportError:
    from importlib_metadata import version

from metadata_managed_apis.api.response import ApiResponse

logger = operations_logger()


def health_response():
    try:
        return ApiResponse.success(
            {"status": "healthy", "version": version("metadata-ingestion")}
        )
    except Exception as exc:
        msg = f"Error obtaining Airflow REST status due to [{exc}] "
        logger.debug(traceback.format_exc())
        logger.error(msg)
        return ApiResponse.error(
            status=ApiResponse.STATUS_BAD_REQUEST,
            error=msg,
        )
