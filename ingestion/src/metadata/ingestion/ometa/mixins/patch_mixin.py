#  Copyright 2021 Collate
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
Mixin class containing PATCH specific methods

To be used by OpenMetadata class
"""
import json
import traceback
from copy import deepcopy
from typing import Dict, List, Optional, Type, TypeVar, Union

from pydantic import BaseModel

from metadata.generated.schema.entity.automations.workflow import (
    Workflow as AutomationWorkflow,
)
from metadata.generated.schema.entity.automations.workflow import WorkflowStatus
from metadata.generated.schema.entity.data.container import Container
from metadata.generated.schema.entity.data.table import Column, Table, TableConstraint
from metadata.generated.schema.entity.domains.domain import Domain
from metadata.generated.schema.entity.services.connections.testConnectionResult import (
    TestConnectionResult,
)
from metadata.generated.schema.tests.testCase import TestCase, TestCaseParameterValue
from metadata.generated.schema.type.basic import EntityLink
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.generated.schema.type.lifeCycle import LifeCycle
from metadata.generated.schema.type.tagLabel import TagLabel
from metadata.ingestion.api.models import Entity
from metadata.ingestion.models.patch_request import build_patch
from metadata.ingestion.models.table_metadata import ColumnDescription, ColumnTag
from metadata.ingestion.ometa.client import REST
from metadata.ingestion.ometa.mixins.patch_mixin_utils import (
    OMetaPatchMixinBase,
    PatchField,
    PatchOperation,
    PatchPath,
)
from metadata.ingestion.ometa.utils import model_str
from metadata.utils.deprecation import deprecated
from metadata.utils.logger import get_log_name, ometa_logger

logger = ometa_logger()

T = TypeVar("T", bound=BaseModel)

OWNER_TYPES: List[str] = ["user", "team"]


def update_column_tags(
    columns: List[Column],
    column_tag: ColumnTag,
    operation: PatchOperation,
) -> None:
    """
    Inplace update for the incoming column list
    """
    for col in columns:
        if (
            str(col.fullyQualifiedName.__root__).lower()
            == column_tag.column_fqn.lower()
        ):
            if operation == PatchOperation.REMOVE:
                for tag in col.tags:
                    if tag.tagFQN == column_tag.tag_label.tagFQN:
                        col.tags.remove(tag)
            else:
                if col.tags is None:
                    col.tags = []
                col.tags.append(column_tag.tag_label)
            break

        if col.children:
            update_column_tags(col.children, column_tag, operation)


def update_column_description(
    columns: List[Column],
    column_descriptions: List[ColumnDescription],
    force: bool = False,
) -> None:
    """
    Inplace update for the incoming column list
    """
    col_dict = {col.column_fqn.lower(): col.description for col in column_descriptions}
    for col in columns:
        # For dbt the column names in OM and dbt are not always in the same case.
        # We'll match the column names in case insensitive way
        desc_column = col_dict.get(col.fullyQualifiedName.__root__.lower())
        if desc_column:
            if col.description and not force:
                # If the description is already present and force is not passed,
                # description will not be overridden
                continue

            col.description = desc_column.__root__

        if col.children:
            update_column_description(col.children, column_descriptions, force)


class OMetaPatchMixin(OMetaPatchMixinBase):
    """
    OpenMetadata API methods related to Tables.

    To be inherited by OpenMetadata
    """

    client: REST

    def patch(
        self,
        entity: Type[T],
        source: T,
        destination: T,
        allowed_fields: Optional[Dict] = None,
        restrict_update_fields: Optional[List] = None,
        array_entity_fields: Optional[List] = None,
    ) -> Optional[T]:
        """
        Given an Entity type and Source entity and Destination entity,
        generate a JSON Patch and apply it.

        Args
            entity (T): Entity Type
            source: Source payload which is current state of the source in OpenMetadata
            destination: payload with changes applied to the source.
            allowed_fields: List of field names to filter from source and destination models
            restrict_update_fields: List of field names which will only support add operation

        Returns
            Updated Entity
        """
        try:
            patch = build_patch(
                source=source,
                destination=destination,
                allowed_fields=allowed_fields,
                restrict_update_fields=restrict_update_fields,
                array_entity_fields=array_entity_fields,
            )

            if not patch:
                return None

            res = self.client.patch(
                path=f"{self.get_suffix(entity)}/{model_str(source.id)}",
                data=str(patch),
            )
            return entity(**res)

        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.error(f"Error trying to PATCH {get_log_name(source)}: {exc}")

        return None

    def patch_description(
        self,
        entity: Type[T],
        source: T,
        description: str,
        force: bool = False,
    ) -> Optional[T]:
        """
        Given an Entity type and ID, JSON PATCH the description.

        Args
            entity (T): Entity Type
            source: source entity object
            description: new description to add
            force: if True, we will patch any existing description. Otherwise, we will maintain
                the existing data.
        Returns
            Updated Entity
        """
        if isinstance(source, TestCase):
            instance: Optional[T] = self._fetch_entity_if_exists(
                entity=entity,
                entity_id=source.id,
                fields=["testDefinition", "testSuite"],
            )
        else:
            instance: Optional[T] = self._fetch_entity_if_exists(
                entity=entity, entity_id=source.id
            )

        if not instance:
            return None

        if instance.description and not force:
            # If the description is already present and force is not passed,
            # description will not be overridden
            return None

        # https://docs.pydantic.dev/latest/usage/exporting_models/#modelcopy
        destination = source.copy(deep=True)
        destination.description = description

        return self.patch(entity=entity, source=source, destination=destination)

    def patch_table_constraints(
        self,
        table: Table,
        constraints: List[TableConstraint],
    ) -> Optional[T]:
        """Given an Entity ID, JSON PATCH the table constraints of table

        Args
            source_table: Origin table
            description: new description to add
            table_constraints: table constraints to add

        Returns
            Updated Entity
        """
        instance: Table = self._fetch_entity_if_exists(
            entity=Table, entity_id=table.id, fields=["tableConstraints"]
        )

        if not instance:
            return None

        table.tableConstraints = instance.tableConstraints

        destination = table.copy(deep=True)
        destination.tableConstraints = constraints

        return self.patch(entity=Table, source=table, destination=destination)

    def patch_test_case_definition(
        self,
        test_case: TestCase,
        entity_link: str,
        test_case_parameter_values: Optional[List[TestCaseParameterValue]] = None,
        compute_passed_failed_row_count: Optional[bool] = False,
    ) -> Optional[TestCase]:
        """Given a test case and a test case definition JSON PATCH the test case

        Args
            test_case: test case object
            test_case_definition: test case definition to add
        """
        source: TestCase = self._fetch_entity_if_exists(
            entity=TestCase, entity_id=test_case.id, fields=["testDefinition", "testSuite"]  # type: ignore
        )  # type: ignore

        if not source:
            return None

        destination = source.copy(deep=True)

        destination.entityLink = EntityLink(__root__=entity_link)
        if test_case_parameter_values:
            destination.parameterValues = test_case_parameter_values
        if compute_passed_failed_row_count != source.computePassedFailedRowCount:
            destination.computePassedFailedRowCount = compute_passed_failed_row_count

        return self.patch(entity=TestCase, source=source, destination=destination)

    def patch_tags(
        self,
        entity: Type[T],
        source: T,
        tag_labels: List[TagLabel],
        operation: Union[
            PatchOperation.ADD, PatchOperation.REMOVE
        ] = PatchOperation.ADD,
    ) -> Optional[T]:
        """
        Given an Entity type and ID, JSON PATCH the tag.

        Args
            entity (T): Entity Type
            source: Source entity object
            tag_label: TagLabel to add or remove
            operation: Patch Operation to add or remove the tag.
        Returns
            Updated Entity
        """
        instance: Optional[T] = self._fetch_entity_if_exists(
            entity=entity, entity_id=source.id, fields=["tags"]
        )
        if not instance:
            return None

        # Initialize empty tag list or the last updated tags
        source.tags = instance.tags or []
        destination = source.copy(deep=True)

        tag_fqns = {label.tagFQN.__root__ for label in tag_labels}

        if operation == PatchOperation.REMOVE:
            for tag in destination.tags:
                if tag.tagFQN.__root__ in tag_fqns:
                    destination.tags.remove(tag)
        else:
            destination.tags.extend(tag_labels)

        return self.patch(entity=entity, source=source, destination=destination)

    def patch_tag(
        self,
        entity: Type[T],
        source: T,
        tag_label: TagLabel,
        operation: Union[
            PatchOperation.ADD, PatchOperation.REMOVE
        ] = PatchOperation.ADD,
    ) -> Optional[T]:
        """Will be deprecated in 1.3"""
        logger.warning("patch_tag will be deprecated in 1.3. Use `patch_tags` instead.")
        return self.patch_tags(
            entity=entity, source=source, tag_labels=[tag_label], operation=operation
        )

    def patch_owner(
        self,
        entity: Type[T],
        source: T,
        owner: EntityReference = None,
        force: bool = False,
    ) -> Optional[T]:
        """
        Given an Entity type and ID, JSON PATCH the owner. If not owner Entity type and
        not owner ID are provided, the owner is removed.

        Args
            entity (T): Entity Type of the entity to be patched
            entity_id: ID of the entity to be patched
            owner: Entity Reference of the owner. If None, the owner will be removed
            force: if True, we will patch any existing owner. Otherwise, we will maintain
                the existing data.
        Returns
            Updated Entity
        """
        instance: Optional[T] = self._fetch_entity_if_exists(
            entity=entity, entity_id=source.id, fields=["owner"]
        )

        if not instance:
            return None

        # Don't change existing data without force
        if instance.owner and not force:
            # If a owner is already present and force is not passed,
            # owner will not be overridden
            return None

        destination = deepcopy(instance)
        destination.owner = owner

        return self.patch(entity=entity, source=instance, destination=destination)

    def patch_column_tags(
        self,
        table: Table,
        column_tags: List[ColumnTag],
        operation: Union[
            PatchOperation.ADD, PatchOperation.REMOVE
        ] = PatchOperation.ADD,
    ) -> Optional[T]:
        """Given an Entity ID, JSON PATCH the tag of the column

        Args
            entity_id: ID
            tag_label: TagLabel to add or remove
            column_name: column to update
            operation: Patch Operation to add or remove
        Returns
            Updated Entity
        """
        instance: Optional[Table] = self._fetch_entity_if_exists(
            entity=Table, entity_id=table.id, fields=["tags", "columns"]
        )

        if not instance:
            return None

        # Make sure we run the patch against the last updated data from the API
        table.columns = instance.columns

        destination = table.copy(deep=True)
        for column_tag in column_tags or []:
            update_column_tags(destination.columns, column_tag, operation)

        patched_entity = self.patch(entity=Table, source=table, destination=destination)
        if patched_entity is None:
            logger.debug(
                f"Empty PATCH result. Either everything is up to date or the "
                f"column names are  not in [{table.fullyQualifiedName.__root__}]"
            )

        return patched_entity

    def patch_container_column_tags(
            self,
            container: Container,
            column_tags: List[ColumnTag],
            operation: Union[
                PatchOperation.ADD, PatchOperation.REMOVE
            ] = PatchOperation.ADD,
    ) -> Optional[T]:
        """Given an Entity ID, JSON PATCH the tag of the column

        Args
            entity_id: ID
            tag_label: TagLabel to add or remove
            column_name: column to update
            operation: Patch Operation to add or remove
        Returns
            Updated Entity
        """
        instance: Optional[Container] = self._fetch_entity_if_exists(
            entity=Container, entity_id=container.id, fields=["dataModel"]
        )

        if not instance:
            return None

        # Make sure we run the patch against the last updated data from the API
        container.dataModel.columns = instance.dataModel.columns

        destination = container.copy(deep=True)
        for column_tag in column_tags or []:
            update_column_tags(destination.dataModel.columns, column_tag, operation)

        patched_entity = self.patch(entity=Container, source=container, destination=destination)
        if patched_entity is None:
            logger.debug(
                f"Empty PATCH result. Either everything is up to date or the "
                f"column names are  not in [{container.fullyQualifiedName.__root__}]"
            )

        return patched_entity

    @deprecated(message="Use metadata.patch_column_tags instead", release="1.3.1")
    def patch_column_tag(
        self,
        table: Table,
        column_fqn: str,
        tag_label: TagLabel,
        operation: Union[
            PatchOperation.ADD, PatchOperation.REMOVE
        ] = PatchOperation.ADD,
    ) -> Optional[T]:
        """Will be deprecated in 1.3"""
        return self.patch_column_tags(
            table=table,
            column_tags=[ColumnTag(column_fqn=column_fqn, tag_label=tag_label)],
            operation=operation,
        )

    @deprecated(
        message="Use metadata.patch_column_descriptions instead", release="1.3.1"
    )
    def patch_column_description(
        self,
        table: Table,
        column_fqn: str,
        description: str,
        force: bool = False,
    ) -> Optional[T]:
        """Given an Table , Column FQN, JSON PATCH the description of the column

        Args
            src_table: origin Table object
            column_fqn: FQN of the column to update
            description: new description to add
            force: if True, we will patch any existing description. Otherwise, we will maintain
                the existing data.
        Returns
            Updated Entity
        """
        return self.patch_column_descriptions(
            table=table,
            column_descriptions=[
                ColumnDescription(column_fqn=column_fqn, description=description)
            ],
            force=force,
        )

    def patch_column_descriptions(
        self,
        table: Table,
        column_descriptions: List[ColumnDescription],
        force: bool = False,
    ) -> Optional[T]:
        """Given an Table , Column Descriptions, JSON PATCH the description of the column

        Args
            src_table: origin Table object
            column_descriptions: List of ColumnDescription object
            force: if True, we will patch any existing description. Otherwise, we will maintain
                the existing data.
        Returns
            Updated Entity
        """
        instance: Optional[Table] = self._fetch_entity_if_exists(
            entity=Table, entity_id=table.id
        )

        if not instance or not column_descriptions:
            return None

        # Make sure we run the patch against the last updated data from the API
        table.columns = instance.columns

        destination = table.copy(deep=True)
        update_column_description(destination.columns, column_descriptions, force)

        patched_entity = self.patch(entity=Table, source=table, destination=destination)
        if patched_entity is None:
            logger.debug(
                f"Empty PATCH result. Either everything is up to date or "
                f"columns are not matching for [{table.fullyQualifiedName.__root__}]"
            )

        return patched_entity

    def patch_automation_workflow_response(
        self,
        automation_workflow: AutomationWorkflow,
        test_connection_result: TestConnectionResult,
        workflow_status: WorkflowStatus,
    ) -> None:
        """
        Given an AutomationWorkflow, JSON PATCH the status and response.
        """
        result_data: Dict = {
            PatchField.PATH: PatchPath.RESPONSE,
            PatchField.VALUE: test_connection_result.dict(),
            PatchField.OPERATION: PatchOperation.ADD,
        }

        # for deserializing into json convert enum object to string
        result_data[PatchField.VALUE]["status"] = result_data[PatchField.VALUE][
            "status"
        ].value

        status_data: Dict = {
            PatchField.PATH: PatchPath.STATUS,
            PatchField.OPERATION: PatchOperation.ADD,
            PatchField.VALUE: workflow_status.value,
        }

        try:
            self.client.patch(
                path=f"{self.get_suffix(AutomationWorkflow)}/{model_str(automation_workflow.id)}",
                data=json.dumps([result_data, status_data]),
            )
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.error(
                f"Error trying to PATCH status for automation workflow [{model_str(automation_workflow)}]: {exc}"
            )

    def patch_life_cycle(
        self, entity: Entity, life_cycle: LifeCycle
    ) -> Optional[Entity]:
        """
        Patch life cycle data for a entity

        :param entity: Entity to update the life cycle for
        :param life_cycle_data: Life Cycle data to add
        """
        try:
            destination = entity.copy(deep=True)
            destination.lifeCycle = life_cycle
            return self.patch(
                entity=type(entity), source=entity, destination=destination
            )
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.warning(
                f"Error trying to Patch life cycle data for {entity.fullyQualifiedName.__root__}: {exc}"
            )
            return None

    def patch_domain(self, entity: Entity, domain: Domain) -> Optional[Entity]:
        """Patch domain data for an Entity"""
        try:
            destination: Entity = entity.copy(deep=True)
            destination.domain = EntityReference(id=domain.id, type="domain")
            return self.patch(
                entity=type(entity), source=entity, destination=destination
            )
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.warning(
                f"Error trying to Patch Domain for {entity.fullyQualifiedName.__root__}: {exc}"
            )
            return None
