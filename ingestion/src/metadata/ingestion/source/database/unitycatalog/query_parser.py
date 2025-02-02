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
UnityCatalog Query parser module
"""
from abc import ABC
from typing import Optional

from metadata.generated.schema.entity.services.connections.database.unityCatalogConnection import (
    UnityCatalogConnection,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.server.server_api import ServerInterface
from metadata.ingestion.source.database.databricks.query_parser import (
    DatabricksQueryParserSource,
)
from metadata.ingestion.source.database.query_parser_source import QueryParserSource
from metadata.ingestion.source.database.unitycatalog.client import UnityCatalogClient
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


class UnityCatalogQueryParserSource(
    DatabricksQueryParserSource, QueryParserSource, ABC
):
    """
    UnityCatalog Query Parser Source

    This class would be inheriting all the methods
    from DatabricksQueryParserSource
    """

    filters: str

    # pylint: disable=super-init-not-called
    def __init__(self, config: WorkflowSource, metadata: ServerInterface):
        self._init_super(config=config, metadata=metadata)
        self.client = UnityCatalogClient(self.service_connection)

    @classmethod
    def create(
        cls, config_dict, metadata: ServerInterface, pipeline_name: Optional[str] = None
    ):
        """Create class instance"""
        config: WorkflowSource = WorkflowSource.parse_obj(config_dict)
        connection: UnityCatalogConnection = config.serviceConnection.__root__.config
        if not isinstance(connection, UnityCatalogConnection):
            raise InvalidSourceException(
                f"Expected UnityCatalogConnection, but got {connection}"
            )
        return cls(config, metadata)
