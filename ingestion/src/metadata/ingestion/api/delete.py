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
Delete methods
"""
import traceback
from typing import Dict, Iterable, List, Optional, Type

from metadata.generated.schema.entity.data.container import Container
from metadata.generated.schema.entity.services.ingestionPipelines.status import (
    StackTraceError,
)
from metadata.ingestion.api.models import Either
from metadata.ingestion.models.delete_entity import DeleteEntity
from metadata.ingestion.server.server_api import ServerInterface, T
from metadata.utils.logger import utils_logger

logger = utils_logger()


def delete_entity_from_source(
    metadata: ServerInterface,
    entity_type: Type[T],
    entity_source_state,
    mark_deleted_entity: bool = True,
    params: Optional[Dict[str, str]] = None,
) -> Iterable[Either[DeleteEntity]]:
    """
    Method to delete the entities
    :param metadata: OMeta client
    :param entity_type: Pydantic Entity model
    :param entity_source_state: Current state of the service
    :param mark_deleted_entity: Option to mark the entity as deleted or not
    :param params: param to fetch the entity state
    """
    try:
        entity_state = metadata.list_all_entities(entity=entity_type, params=params)
        for entity in entity_state:
            if isinstance(entity_type, Container.__class__):
                # remove double quotes from the fqn
                entity.fullyQualifiedName.__root__ = entity.fullyQualifiedName.__root__.replace(f"\"{str(entity.name.__root__)}\"", str(entity.name.__root__))
                if str(entity.fullyQualifiedName.__root__) not in entity_source_state:
                    yield Either(
                        right=DeleteEntity(
                            entity=entity, mark_deleted_entities=mark_deleted_entity
                        )
                    )
                continue
            if str(entity.fullyQualifiedName.__root__) not in entity_source_state:
                yield Either(
                    right=DeleteEntity(
                        entity=entity,
                        mark_deleted_entities=mark_deleted_entity,
                    )
                )
    except Exception as exc:
        yield Either(
            left=StackTraceError(
                name="Delete Entity",
                error=f"Error deleting {entity_type.__class__}: {exc}",
                stackTrace=traceback.format_exc(),
            )
        )


def delete_entity_by_name(
    metadata: ServerInterface,
    entity_type: Type[T],
    entity_names: List[str],
    mark_deleted_entity: bool = True,
) -> Iterable[Either[DeleteEntity]]:
    """
    Method to delete the entites contained on a given list
    :param metadata: OMeta client
    :param entity_type: Pydantic Entity model
    :param entity_names: List of FullyQualifiedNames of the entities to be deleted
    :param mark_deleted_entity: Option to mark the entity as deleted or not
    """
    try:
        for entity_name in entity_names:
            entity = metadata.get_by_name(entity=entity_type, fqn=entity_name)
            if entity:
                yield Either(
                    right=DeleteEntity(
                        entity=entity, mark_deleted_entities=mark_deleted_entity
                    )
                )
    except Exception as exc:
        yield Either(
            left=StackTraceError(
                name="Delete Entity",
                error=f"Error deleting {entity_type.__class__}: {exc}",
                stackTrace=traceback.format_exc(),
            )
        )
