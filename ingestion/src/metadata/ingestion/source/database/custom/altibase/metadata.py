from sqlalchemy.inspection import inspect

from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()

from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.database.common_custom_db_source import CommonCustomDbSourceService
from metadata.generated.schema.entity.services.connections.database.customDatabaseConnection import CustomDatabaseConnection
from metadata.ingestion.source.database.custom.altibase.altibaseConnection import AltibaseConnection
from metadata.ingestion.source.database.custom.altibase.connection import get_connection
from metadata.ingestion.source.database.custom.altibase.utils import (
    get_table_names, get_columns
)

from sqlalchemy_altibase.base import AltibaseDialect

AltibaseDialect.get_table_names = get_table_names
AltibaseDialect.get_columns = get_columns


class AltibaseSource(CommonCustomDbSourceService):

    @classmethod
    def create(cls, config_dict, metadata: OpenMetadata):
        config = WorkflowSource.parse_obj(config_dict)
        connection: CustomDatabaseConnection = config.serviceConnection.__root__.config
        if not isinstance(connection, CustomDatabaseConnection):
            raise InvalidSourceException(
                f"Expected OracleConnection, but got {connection}"
            )
        return cls(config, metadata)

    def prepare(self):
        self.inspector = inspect(self.engine)

    def get_connection(self, service_connection: CustomDatabaseConnection):
        conn = AltibaseConnection(**service_connection.connectionOptions.__root__)
        return get_connection(conn)

    def test_connection(self):
        return True


