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
Druid source methods.
"""

from typing import Optional

from metadata.generated.schema.entity.services.connections.database.druidConnection import (
    DruidConnection,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.server.server_api import ServerInterface
from metadata.ingestion.source.database.common_db_source import CommonDbSourceService


class DruidSource(CommonDbSourceService):
    @classmethod
    def create(
        cls, config_dict, metadata: ServerInterface, pipeline_name: Optional[str] = None
    ):
        """Create class instance"""
        config: WorkflowSource = WorkflowSource.parse_obj(config_dict)
        connection: DruidConnection = config.serviceConnection.__root__.config
        if not isinstance(connection, DruidConnection):
            raise InvalidSourceException(
                f"Expected DruidConnection, but got {connection}"
            )
        return cls(config, metadata)
