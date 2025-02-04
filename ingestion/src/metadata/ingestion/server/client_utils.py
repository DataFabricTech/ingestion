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
OMeta client create helpers
"""
import traceback
from typing import List

from metadata.generated.schema.entity.data.chart import Chart
from metadata.generated.schema.entity.services.connections.metadata.metadataConnection import (
    MetadataConnection,
)
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.server.server_api import ServerInterface
from metadata.utils import fqn
from metadata.utils.logger import ometa_logger

logger = ometa_logger()


def create_ometa_client(
    metadata_config: MetadataConnection,
) -> ServerInterface:
    """Create an Metadata client

    Args:
        metadata_config (MetadataConnection): OM connection config

    Returns:
        ServerInterface: an OM client
    """
    try:
        metadata = ServerInterface(metadata_config)
        metadata.health_check()
        return metadata
    except Exception as exc:
        logger.debug(traceback.format_exc())
        logger.warning(f"Wild error initialising the OMeta Client {exc}")
        raise ValueError(exc)


def get_chart_entities_from_id(
    chart_ids: List[str], metadata: ServerInterface, service_name: str
) -> List[EntityReference]:
    """
    Method to get the chart entity using get_by_name api
    """

    entities = []
    for chart_id in chart_ids:
        chart: Chart = metadata.get_by_name(
            entity=Chart,
            fqn=fqn.build(
                metadata, Chart, chart_name=str(chart_id), service_name=service_name
            ),
        )
        if chart:
            entity = EntityReference(id=chart.id, type="chart")
            entities.append(entity)
    return entities
