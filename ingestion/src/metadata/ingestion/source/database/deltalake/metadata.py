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
Deltalake source methods.
"""
import re
import traceback
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from pyspark.sql.utils import AnalysisException, ParseException

from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from metadata.generated.schema.api.data.createDatabaseSchema import (
    CreateDatabaseSchemaRequest,
)
from metadata.generated.schema.api.data.createQuery import CreateQueryRequest
from metadata.generated.schema.api.data.createStoredProcedure import (
    CreateStoredProcedureRequest,
)
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
from metadata.generated.schema.entity.data.table import Column, Table, TableType
from metadata.generated.schema.entity.services.connections.database.deltaLakeConnection import (
    DeltaLakeConnection,
)
from metadata.generated.schema.entity.services.ingestionPipelines.status import (
    StackTraceError,
)
from metadata.generated.schema.metadataIngestion.databaseServiceMetadataPipeline import (
    DatabaseServiceMetadataPipeline,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.models.ometa_classification import OMetaTagAndClassification
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.connections import get_connection
from metadata.ingestion.source.database.column_type_parser import ColumnTypeParser
from metadata.ingestion.source.database.database_service import DatabaseServiceSource
from metadata.ingestion.source.database.stored_procedures_mixin import QueryByProcedure
from metadata.utils import fqn
from metadata.utils.constants import DEFAULT_DATABASE
from metadata.utils.filters import filter_by_schema, filter_by_table
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


class SparkTableType(Enum):
    MANAGED = "MANAGED"
    TEMPORARY = "TEMPORARY"
    VIEW = "VIEW"
    EXTERNAL = "EXTERNAL"


TABLE_TYPE_MAP = {
    SparkTableType.MANAGED.value: TableType.Regular,
    SparkTableType.VIEW.value: TableType.View,
    SparkTableType.EXTERNAL.value: TableType.External,
}


ARRAY_CHILD_START_INDEX = 6
ARRAY_CHILD_END_INDEX = -1


class MetaStoreNotFoundException(Exception):
    """
    Metastore is not passed thorugh file or url
    """


class DeltalakeSource(DatabaseServiceSource):
    """
    Implements the necessary methods to extract
    Database metadata from Deltalake Source
    """

    def __init__(
        self,
        config: WorkflowSource,
        metadata: OpenMetadata,
    ):
        super().__init__()
        self.config = config
        self.source_config: DatabaseServiceMetadataPipeline = (
            self.config.sourceConfig.config
        )
        self.metadata = metadata
        self.service_connection = self.config.serviceConnection.__root__.config
        self.spark = get_connection(self.service_connection)

        self.table_type_map = {
            TableType.External.value.lower(): TableType.External.value,
            TableType.View.value.lower(): TableType.View.value,
            TableType.SecureView.value.lower(): TableType.SecureView.value,
            TableType.Iceberg.value.lower(): TableType.Iceberg.value,
        }
        self.array_datatype_replace_map = {"(": "<", ")": ">", "=": ":", "<>": ""}
        self.table_constraints = None
        self.database_source_state = set()

        self.connection_obj = self.spark
        self.test_connection()

    @classmethod
    def create(
        cls, config_dict, metadata: OpenMetadata, pipeline_name: Optional[str] = None
    ):
        config: WorkflowSource = WorkflowSource.parse_obj(config_dict)
        connection: DeltaLakeConnection = config.serviceConnection.__root__.config
        if not isinstance(connection, DeltaLakeConnection):
            raise InvalidSourceException(
                f"Expected DeltaLakeConnection, but got {connection}"
            )
        return cls(config, metadata)

    def get_database_names(self) -> Iterable[str]:
        """
        Default case with a single database.

        It might come informed - or not - from the source.

        Sources with multiple databases should overwrite this and
        apply the necessary filters.
        """

        yield self.service_connection.databaseName or DEFAULT_DATABASE

    def yield_database(
        self, database_name: str
    ) -> Iterable[Either[CreateDatabaseRequest]]:
        """
        From topology.
        Prepare a database request and pass it to the sink
        """
        yield Either(
            right=CreateDatabaseRequest(
                name=database_name,
                service=self.context.get().database_service,
            )
        )

    def get_database_schema_names(self) -> Iterable[str]:
        """
        return schema names
        """
        schemas = self.spark.catalog.listDatabases()
        for schema in schemas:
            schema_fqn = fqn.build(
                self.metadata,
                entity_type=DatabaseSchema,
                service_name=self.context.get().database_service,
                database_name=self.context.get().database,
                schema_name=schema.name,
            )
            if filter_by_schema(
                self.config.sourceConfig.config.schemaFilterPattern,
                schema_fqn
                if self.config.sourceConfig.config.useFqnForFiltering
                else schema.name,
            ):
                self.status.filter(schema_fqn, "Schema Filtered Out")
                continue
            yield schema.name

    def yield_database_schema(
        self, schema_name: str
    ) -> Iterable[Either[CreateDatabaseSchemaRequest]]:
        """
        From topology.
        Prepare a database schema request and pass it to the sink
        """
        yield Either(
            right=CreateDatabaseSchemaRequest(
                name=schema_name,
                database=fqn.build(
                    metadata=self.metadata,
                    entity_type=Database,
                    service_name=self.context.get().database_service,
                    database_name=self.context.get().database,
                ),
            )
        )

    def get_tables_name_and_type(self) -> Optional[Iterable[Tuple[str, str]]]:
        """
        Handle table and views.

        Fetches them up using the context information and
        the inspector set when preparing the db.

        :return: tables or views, depending on config
        """
        schema_name = self.context.get().database_schema
        for table in self.spark.catalog.listTables(dbName=schema_name):
            try:
                table_name = table.name
                table_fqn = fqn.build(
                    self.metadata,
                    entity_type=Table,
                    service_name=self.context.get().database_service,
                    database_name=self.context.get().database,
                    schema_name=self.context.get().database_schema,
                    table_name=table.name,
                )
                if filter_by_table(
                    self.source_config.tableFilterPattern,
                    table_fqn if self.source_config.useFqnForFiltering else table.name,
                ):
                    self.status.filter(
                        table_fqn,
                        "Table Filtered Out",
                    )
                    continue
                if (
                    self.source_config.includeTables
                    and table.tableType
                    and table.tableType != SparkTableType.VIEW.value
                ):
                    # We will skip ingesting any TMP table
                    if table.tableType == SparkTableType.TEMPORARY.value:
                        logger.debug(f"Skipping temporary table {table.name}")
                        continue

                    self.context.get().table_description = table.description
                    yield table_name, TABLE_TYPE_MAP.get(
                        table.tableType, TableType.Regular
                    )

                if (
                    self.source_config.includeViews
                    and table.tableType
                    and table.tableType == SparkTableType.VIEW.value
                ):
                    self.context.get().table_description = table.description
                    yield table_name, TableType.View

            except Exception as exc:
                logger.debug(traceback.format_exc())
                logger.warning(f"Unexpected exception for table [{table}]: {exc}")
                self.status.warnings.append(f"{self.config.serviceName}.{table.name}")

    def yield_table(
        self, table_name_and_type: Tuple[str, TableType]
    ) -> Iterable[Either[CreateTableRequest]]:
        """
        From topology.
        Prepare a table request and pass it to the sink
        """
        table_name, table_type = table_name_and_type
        schema_name = self.context.get().database_schema
        try:
            columns = self.get_columns(schema_name, table_name)
            view_definition = (
                self._fetch_view_schema(table_name)
                if table_type == TableType.View
                else None
            )

            table_request = CreateTableRequest(
                name=table_name,
                tableType=table_type,
                description=self.context.get().table_description,
                columns=columns,
                tableConstraints=None,
                databaseSchema=fqn.build(
                    metadata=self.metadata,
                    entity_type=DatabaseSchema,
                    service_name=self.context.get().database_service,
                    database_name=self.context.get().database,
                    schema_name=schema_name,
                ),
                schemaDefinition=view_definition,
            )

            yield Either(right=table_request)
            self.register_record(table_request=table_request)
        except Exception as exc:
            yield Either(
                left=StackTraceError(
                    name=table_name,
                    error=f"Unexpected exception to yield table [{table_name}]: {exc}",
                    stackTrace=traceback.format_exc(),
                )
            )

    def prepare(self):
        """Nothing to prepare"""

    def _fetch_view_schema(self, view_name: str) -> Optional[Dict]:
        try:
            describe_output = self.spark.sql(f"describe extended {view_name}").collect()
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.warning(
                f"Unexpected exception to fetch view schema [{view_name}]: {exc}"
            )
            return None
        view_detail = {}
        col_details = False

        for row in describe_output:
            row_dict = row.asDict()
            if col_details:
                view_detail[row_dict["col_name"]] = row_dict["data_type"]
            if "# Detailed Table" in row_dict["col_name"]:
                col_details = True
        return view_detail.get("View Text")

    def _check_col_length(self, datatype, col_raw_type):
        if datatype and datatype.upper() in {"CHAR", "VARCHAR", "BINARY", "VARBINARY"}:
            try:
                return col_raw_type.length if col_raw_type.length else 1
            except AttributeError:
                return 1
        return None

    def _get_display_data_type(self, row):
        display_data_type = repr(row["data_type"]).lower()
        for original, new in self.array_datatype_replace_map.items():
            display_data_type = display_data_type.replace(original, new)
        return display_data_type

    def _get_col_info(self, row):
        parsed_string = (
            ColumnTypeParser._parse_datatype_string(  # pylint: disable=protected-access
                row["data_type"]
            )
        )
        if parsed_string:
            parsed_string["dataLength"] = self._check_col_length(
                parsed_string["dataType"], row["data_type"]
            )
            if row["data_type"] == "array":
                array_data_type_display = self._get_display_data_type(row)
                parsed_string["dataTypeDisplay"] = array_data_type_display
                # Parse Primitive Datatype string
                # if Datatype is Arrya(int) -> Parse int
                parsed_string[
                    "arrayDataType"
                ] = ColumnTypeParser._parse_primitive_datatype_string(  # pylint: disable=protected-access
                    array_data_type_display[
                        ARRAY_CHILD_START_INDEX:ARRAY_CHILD_END_INDEX
                    ]
                )[
                    "dataType"
                ]
            column = Column(name=row["col_name"], **parsed_string)
        else:
            col_type = re.search(r"^\w+", row["data_type"]).group(0)
            charlen = re.search(r"\(([\d]+)\)", row["data_type"])
            if charlen:
                charlen = int(charlen.group(1))
            if (
                col_type.upper() in {"CHAR", "VARCHAR", "VARBINARY", "BINARY"}
                and charlen is None
            ):
                charlen = 1

            column = Column(
                name=row["col_name"],
                description=row.get("comment"),
                dataType=col_type,
                dataLength=charlen,
                displayName=row["data_type"],
            )
        return column

    def get_columns(self, schema: str, table: str) -> List[Column]:
        """
        Method to handle table columns
        """
        field_dict: Dict[str, Any] = {}
        table_name = f"{schema}.{table}"
        try:
            raw_columns = self.spark.sql(f"describe {table_name}").collect()
            for field in self.spark.table(f"{table_name}").schema:
                field_dict[field.name] = field
        except (AnalysisException, ParseException) as exc:
            logger.debug(traceback.format_exc())
            logger.warning(
                f"Unexpected exception getting columns for [{table_name}]: {exc}"
            )
            return []
        parsed_columns: List[Column] = []
        partition_cols = False
        for row in raw_columns:
            col_name = row["col_name"]
            if col_name == "" or "#" in col_name:
                partition_cols = True
                continue
            if not partition_cols:
                column = self._get_col_info(row)
                parsed_columns.append(column)

        return parsed_columns

    def yield_view_lineage(self) -> Iterable[Either[AddLineageRequest]]:
        yield from []

    def yield_tag(
        self, schema_name: str
    ) -> Iterable[Either[OMetaTagAndClassification]]:
        """We don't pick up tags from Delta"""

    def get_stored_procedures(self) -> Iterable[Any]:
        """Not implemented"""

    def yield_stored_procedure(
        self, stored_procedure: Any
    ) -> Iterable[Either[CreateStoredProcedureRequest]]:
        """Not implemented"""

    def get_stored_procedure_queries(self) -> Iterable[QueryByProcedure]:
        """Not Implemented"""

    def yield_procedure_lineage_and_queries(
        self,
    ) -> Iterable[Either[Union[AddLineageRequest, CreateQueryRequest]]]:
        """Not Implemented"""
        yield from []

    def close(self):
        """No client to close"""
