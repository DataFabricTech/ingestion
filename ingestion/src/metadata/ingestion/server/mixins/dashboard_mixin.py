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
Mixin class containing Table specific methods

To be used by Metadata class
"""

from metadata.generated.schema.entity.data.dashboard import Dashboard
from metadata.generated.schema.type.usageRequest import UsageRequest
from metadata.ingestion.server.client import REST
from metadata.utils.logger import ometa_logger

logger = ometa_logger()


class OMetaDashboardMixin:
    """
    Metadata API methods related to Dashboards and Charts.

    To be inherited by Metadata
    """

    client: REST

    def publish_dashboard_usage(
        self, dashboard: Dashboard, dashboard_usage_request: UsageRequest
    ) -> None:
        """
        POST usage details for a Dashboard

        :param dashboard: Table Entity to update
        :param dashboard_usage_request: Usage data to add
        """
        resp = self.client.put(
            f"/usage/dashboard/{dashboard.id.__root__}",
            data=dashboard_usage_request.json(),
        )
        logger.debug("Published dashboard usage %s", resp)
