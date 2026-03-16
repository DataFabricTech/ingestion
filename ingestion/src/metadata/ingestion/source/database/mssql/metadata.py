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
"""MSSQL source module"""
import traceback
from typing import Dict, Iterable, List, Optional, Set

from sqlalchemy.dialects.mssql.base import MSDialect, ischema_names

# from metadata.generated.schema.api.data.createStoredProcedure import (
#     CreateStoredProcedureRequest,
# )
from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
# from metadata.generated.schema.entity.data.storedProcedure import StoredProcedureCode
from metadata.generated.schema.entity.services.connections.database.mssqlConnection import (
    MssqlConnection,
)
# from metadata.generated.schema.entity.services.ingestionPipelines.status import (
#     StackTraceError,
# )
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
# from metadata.generated.schema.type.basic import EntityName
# from metadata.ingestion.api.models import Either
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.database.common_db_source import CommonDbSourceService
# from metadata.ingestion.source.database.mssql.models import (
#     STORED_PROC_LANGUAGE_MAP,
#     MssqlStoredProcedure,
# )
from metadata.ingestion.source.database.mssql.queries import (
    MSSQL_GET_DATABASE,
    MSSQL_GET_STORED_PROCEDURE_QUERIES,
    MSSQL_GET_STORED_PROCEDURES,
)
from metadata.ingestion.source.database.mssql.utils import (
    get_columns,
    get_foreign_keys,
    get_pk_constraint,
    get_table_comment,
    get_table_names,
    get_unique_constraints,
    get_view_definition,
    get_view_names,
)
from metadata.ingestion.source.database.multi_db_source import MultiDBSource
# from metadata.ingestion.source.database.stored_procedures_mixin import (
#     QueryByProcedure,
#     StoredProcedureMixin,
# )
from metadata.utils import fqn
from metadata.utils.filters import filter_by_database, filter_by_schema
# from metadata.utils.helpers import get_start_and_end
from metadata.utils.logger import ingestion_logger
from metadata.utils.sqa_utils import update_mssql_ischema_names
from metadata.utils.sqlalchemy_utils import (
    get_all_table_comments,
    get_all_view_definitions,
)

logger = ingestion_logger()

# The ntext, text, and image data types will be removed in a future version of SQL Server.
# Avoid using these data types in new development work, and plan to modify applications that currently use them.
# Use nvarchar(max), varchar(max), and varbinary(max) instead.
# ref: https://learn.microsoft.com/en-us/sql/t-sql/data-types/ntext-text-and-image-transact-sql?view=sql-server-ver16
ischema_names = update_mssql_ischema_names(ischema_names)

MSDialect.get_table_comment = get_table_comment
MSDialect.get_view_definition = get_view_definition
MSDialect.get_all_view_definitions = get_all_view_definitions
MSDialect.get_all_table_comments = get_all_table_comments
MSDialect.get_columns = get_columns
MSDialect.get_pk_constraint = get_pk_constraint
MSDialect.get_unique_constraints = get_unique_constraints
MSDialect.get_foreign_keys = get_foreign_keys
MSDialect.get_table_names = get_table_names
MSDialect.get_view_names = get_view_names


# class MssqlSource(StoredProcedureMixin, CommonDbSourceService, MultiDBSource):
class MssqlSource(CommonDbSourceService, MultiDBSource):
    """
    Implements the necessary methods to extract
    Database metadata from MSSQL Source
    """

    _SYSTEM_SCHEMAS: Set[str] = {
        "sys",
        "information_schema",
        "guest",
        "db_owner",
        "db_accessadmin",
        "db_securityadmin",
        "db_ddladmin",
        "db_backupoperator",
        "db_datareader",
        "db_datawriter",
        "db_denydatareader",
        "db_denydatawriter",
    }

    def _is_system_schema(self, schema_name: str) -> bool:
        if not schema_name:
            return True
        return schema_name.lower() in self._SYSTEM_SCHEMAS

    def _is_schema_included_in_filter(self, schema_name: str) -> bool:
        """
        schemaFilterPattern.includes에 명시된 스키마인지 확인.
        include에 있으면 시스템 스키마라도 수집 대상으로 허용하기 위함.
        """
        if (
            not self.source_config.schemaFilterPattern
            or not self.source_config.schemaFilterPattern.includes
        ):
            return False
        name = (
            fqn.build(
                self.metadata,
                entity_type=DatabaseSchema,
                service_name=self.context.get().database_service,
                database_name=self.context.get().database,
                schema_name=schema_name,
            )
            if self.source_config.useFqnForFiltering
            else schema_name
        )
        return not filter_by_schema(self.source_config.schemaFilterPattern, name)

    @classmethod
    def create(cls, config_dict, metadata: OpenMetadata):
        """Create class instance"""
        config: WorkflowSource = WorkflowSource.parse_obj(config_dict)
        connection: MssqlConnection = config.serviceConnection.__root__.config
        if not isinstance(connection, MssqlConnection):
            raise InvalidSourceException(
                f"Expected MssqlConnection, but got {connection}"
            )
        return cls(config, metadata)

    def get_raw_database_schema_names(self) -> Iterable[str]:
        """
        Exclude MSSQL system schemas (sys, information_schema, guest, db_* 등).
        단, schemaFilterPattern.includes에 명시된 스키마는 시스템 스키마여도 수집.
        """
        if self.service_connection.__dict__.get("databaseSchema"):
            schema_name = self.service_connection.databaseSchema
            if schema_name and (
                not self._is_system_schema(schema_name)
                or self._is_schema_included_in_filter(schema_name)
            ):
                yield schema_name
            return

        for schema_name in self.inspector.get_schema_names():
            if not schema_name:
                continue
            if self._is_system_schema(schema_name):
                if self._is_schema_included_in_filter(schema_name):
                    yield schema_name
                continue
            yield schema_name

    def get_configured_database(self) -> Optional[str]:
        if not self.service_connection.ingestAllDatabases:
            return self.service_connection.database
        return None

    def get_database_names_raw(self) -> Iterable[str]:
        yield from self._execute_database_query(MSSQL_GET_DATABASE)

    def get_database_names(self) -> Iterable[str]:
        if not self.config.serviceConnection.__root__.config.ingestAllDatabases:
            configured_db = self.config.serviceConnection.__root__.config.database
            self.set_inspector(database_name=configured_db)
            yield configured_db
        else:
            for new_database in self.get_database_names_raw():
                database_fqn = fqn.build(
                    self.metadata,
                    entity_type=Database,
                    service_name=self.context.database_service,
                    database_name=new_database,
                )

                if filter_by_database(
                    self.source_config.databaseFilterPattern,
                    database_fqn
                    if self.source_config.useFqnForFiltering
                    else new_database,
                ):
                    self.status.filter(database_fqn, "Database Filtered Out")
                    continue

                try:
                    self.set_inspector(database_name=new_database)
                    yield new_database
                except Exception as exc:
                    logger.debug(traceback.format_exc())
                    logger.error(
                        f"Error trying to connect to database {new_database}: {exc}"
                    )

    # def get_stored_procedures(self) -> Iterable[MssqlStoredProcedure]:
    #     """List Snowflake stored procedures"""
    #     if self.source_config.includeStoredProcedures:
    #         results = self.engine.execute(
    #             MSSQL_GET_STORED_PROCEDURES.format(
    #                 database_name=self.context.database,
    #                 schema_name=self.context.database_schema,
    #             )
    #         ).all()
    #         for row in results:
    #             try:
    #                 stored_procedure = MssqlStoredProcedure.parse_obj(dict(row))
    #                 yield stored_procedure
    #             except Exception as exc:
    #                 logger.error()
    #                 self.status.failed(
    #                     error=StackTraceError(
    #                         name=dict(row).get("name", "UNKNOWN"),
    #                         error=f"Error parsing Stored Procedure payload: {exc}",
    #                         stackTrace=traceback.format_exc(),
    #                     )
    #                 )

    # def yield_stored_procedure(
    #     self, stored_procedure: MssqlStoredProcedure
    # ) -> Iterable[Either[CreateStoredProcedureRequest]]:
    #     """Prepare the stored procedure payload"""
    #
    #     try:
    #         stored_procedure_request = CreateStoredProcedureRequest(
    #             name=EntityName(__root__=stored_procedure.name),
    #             description=None,
    #             storedProcedureCode=StoredProcedureCode(
    #                 language=STORED_PROC_LANGUAGE_MAP.get(stored_procedure.language),
    #                 code=stored_procedure.definition,
    #             ),
    #             databaseSchema=fqn.build(
    #                 metadata=self.metadata,
    #                 entity_type=DatabaseSchema,
    #                 service_name=self.context.database_service,
    #                 database_name=self.context.database,
    #                 schema_name=self.context.database_schema,
    #             ),
    #         )
    #         yield Either(right=stored_procedure_request)
    #         self.register_record_stored_proc_request(stored_procedure_request)
    #
    #     except Exception as exc:
    #         yield Either(
    #             left=StackTraceError(
    #                 name=stored_procedure.name,
    #                 error=f"Error yielding Stored Procedure [{stored_procedure.name}] due to [{exc}]",
    #                 stackTrace=traceback.format_exc(),
    #             )
    #         )

    # def get_stored_procedure_queries_dict(self) -> Dict[str, List[QueryByProcedure]]:
    #     """
    #     Return the dictionary associating stored procedures to the
    #     queries they triggered
    #     """
    #     start, _ = get_start_and_end(self.source_config.queryLogDuration)
    #     query = MSSQL_GET_STORED_PROCEDURE_QUERIES.format(
    #         start_date=start,
    #     )
    #
    #     queries_dict = self.procedure_queries_dict(
    #         query=query,
    #     )
    #
    #     return queries_dict
