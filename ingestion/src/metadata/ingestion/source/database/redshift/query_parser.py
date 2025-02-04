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
Redshift usage module
"""
import re
from abc import ABC
from datetime import datetime
from typing import Optional

from metadata.generated.schema.entity.services.connections.database.redshiftConnection import (
    RedshiftConnection,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.server.server_api import ServerInterface
from metadata.ingestion.source.database.query_parser_source import QueryParserSource
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


class RedshiftQueryParserSource(QueryParserSource, ABC):
    """
    Redshift base for usage and lineage
    """

    filters: str

    @classmethod
    def create(
        cls, config_dict, metadata: ServerInterface, pipeline_name: Optional[str] = None
    ):
        config: WorkflowSource = WorkflowSource.parse_obj(config_dict)
        connection: RedshiftConnection = config.serviceConnection.__root__.config
        if not isinstance(connection, RedshiftConnection):
            raise InvalidSourceException(
                f"Expected RedshiftConnection, but got {connection}"
            )
        return cls(config, metadata)

    def get_sql_statement(self, start_time: datetime, end_time: datetime) -> str:
        """
        returns sql statement to fetch query logs
        """
        return self.sql_stmt.format(
            start_time=start_time,
            end_time=end_time,
            filters=self.get_filters(),
            result_limit=self.source_config.resultLimit,
        )

    def check_life_cycle_query(
        self, query_type: Optional[str], query_text: Optional[str]
    ) -> bool:
        """
        returns true if query is to be used for life cycle processing.

        Override if we have specific parameters
        """
        create_pattern = re.compile(r".*\s*CREATE", re.IGNORECASE)
        insert_pattern = re.compile(r".*\s*INSERT", re.IGNORECASE)
        if re.match(create_pattern, query_text) or re.match(insert_pattern, query_text):
            return True
        return False
