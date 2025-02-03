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

"""Extend the ProfilerSource class to add support for Databricks is_disconnect SQA method"""

from metadata.generated.schema.configuration.profilerConfiguration import (
    ProfilerConfiguration,
)
from metadata.generated.schema.entity.services.databaseService import DatabaseService
from metadata.generated.schema.metadataIngestion.workflow import (
    MetadataWorkflowConfig,
)
from metadata.ingestion.server.server_api import ServerInterface
from metadata.profiler.source.base.profiler_source import ProfilerSource


def is_disconnect(self, e, connection, cursor):
    """is_disconnect method for the Databricks dialect"""
    if "Invalid SessionHandle: SessionHandle" in str(e):
        return True
    return False


class DataBricksProfilerSource(ProfilerSource):
    """Databricks Profiler source"""

    def __init__(
        self,
        config: MetadataWorkflowConfig,
        database: DatabaseService,
        ometa_client: ServerInterface,
        global_profiler_config: ProfilerConfiguration,
    ):
        super().__init__(config, database, ometa_client, global_profiler_config)
        self.set_is_disconnect()

    def set_is_disconnect(self):
        """Set the is_disconnect method for the Databricks dialect"""
        from databricks.sqlalchemy import (
            DatabricksDialect,  # pylint: disable=import-outside-toplevel
        )

        DatabricksDialect.is_disconnect = is_disconnect
