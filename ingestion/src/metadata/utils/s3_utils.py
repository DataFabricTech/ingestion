# Copyright 2024 Mobigen
# Licensed under the Apache License, Version 2.0 (the "License")
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
s3 utils module
"""

import traceback
from typing import Iterable

from metadata.utils.logger import utils_logger

logger = utils_logger()


def list_s3_objects(client, **kwargs) -> Iterable:
    """
    Method to get list of s3 objects using pagination
    """
    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(**kwargs):
            yield from page.get("Contents", [])
    except Exception as exc:
        logger.debug(traceback.format_exc())
        logger.warning(f"Unexpected exception to yield s3 object: {exc}")
