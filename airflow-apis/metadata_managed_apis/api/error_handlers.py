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
