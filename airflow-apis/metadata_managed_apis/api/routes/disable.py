# Copyright 2024 Mobigen;
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Notice!
# This software is based on https://open-metadata.org and has been modified accordingly.

"""
Disable/Pause a dag
"""
import traceback
from typing import Callable

from flask import Blueprint, Response
from metadata_managed_apis.api.response import ApiResponse
from metadata_managed_apis.api.utils import get_request_dag_id
from metadata_managed_apis.operations.state import disable_dag
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

    @blueprint.route("/disable", methods=["POST"])
    @csrf.exempt
    @security.requires_access([(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG)])
    def disable() -> Response:
        """
        Given a DAG ID, mark the dag as disabled
        """
        dag_id = get_request_dag_id()

        try:
            return disable_dag(dag_id)

        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.error(f"Failed to get last run logs for [{dag_id}]: {exc}")
            return ApiResponse.error(
                status=ApiResponse.STATUS_SERVER_ERROR,
                error=f"Failed to get last run logs for [{dag_id}] due to {exc} ",
            )

    return disable
