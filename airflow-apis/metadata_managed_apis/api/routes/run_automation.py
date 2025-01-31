"""
Test the connection against a source system
"""
import traceback
from typing import Callable

from flask import Blueprint, Response, escape, request
from metadata_managed_apis.api.response import ApiResponse
from metadata_managed_apis.utils.logger import routes_logger
from pydantic import ValidationError

from metadata.automations.runner import execute
from metadata.ingestion.api.parser import parse_automation_workflow_gracefully
from metadata.utils.secrets.secrets_manager_factory import SecretsManagerFactory

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

    @blueprint.route("/run_automation", methods=["POST"])
    @csrf.exempt
    @security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)])
    def run_automation() -> Response:
        """
        Given a WorkflowSource Schema, create the engine
        and test the connection
        """

        json_request = request.get_json(cache=False)

        try:
            automation_workflow = parse_automation_workflow_gracefully(
                config_dict=json_request
            )

            # we need to instantiate the secret manager in case secrets are passed
            SecretsManagerFactory(
                automation_workflow.metadataServerConnection.secretsManagerProvider,
                automation_workflow.metadataServerConnection.secretsManagerLoader,
            )

            # Should this be triggered async?
            execute(automation_workflow)

            return ApiResponse.success(
                {
                    "message": f"Workflow [{escape(automation_workflow.name)}] has been triggered."
                }
            )

        except ValidationError as err:
            msg = f"Request Validation Error parsing payload: {err}"
            logger.debug(traceback.format_exc())
            logger.error(msg)
            return ApiResponse.error(
                status=ApiResponse.STATUS_BAD_REQUEST,
                error=msg,
            )

        except Exception as exc:
            msg = f"Error running automation workflow due to [{exc}] "
            logger.debug(traceback.format_exc())
            logger.error(msg)
            return ApiResponse.error(
                status=ApiResponse.STATUS_SERVER_ERROR,
                error=msg,
            )

    return run_automation
