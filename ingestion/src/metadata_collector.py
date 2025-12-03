from typing import Union

from metadata.generated.schema.entity.services.pipelineService import PipelineServiceType
from metadata.generated.schema.entity.services.databaseService import DatabaseServiceType
from metadata.generated.schema.entity.services.searchService import SearchServiceType
from metadata.generated.schema.entity.services.storageService import StorageServiceType

from services.database.custom.tibero.source import TiberoSource
from services.database.postgres.source import PostgresSource
from services.database.mysql.source import MysqlSource
from services.database.oracle.source import OracleSource
from services.database.hive.source import HiveSource
from services.database.mssql.source import MssqlSource
from services.storage.s3.source import S3Source
# from services.filesystem.linux.source import LinuxSource
from services.database.custom.altibase.source import AltibaseSource
from services.storage.minio.source import MinioSource
from services.database.mariadb.source import MariadbSource
from services.database.druid.source import DruidSource
from services.database.trino.source import TrinoSource

from metadata.utils.logger import ingestion_logger

from utils.process_config import config
from utils.metadata_process import MetadataProcess

logger = ingestion_logger()
logger.setLevel("INFO")

metadata_process = MetadataProcess()

source_factory_map = {
    DatabaseServiceType.Tibero: lambda **kwargs: TiberoSource(**kwargs),
    DatabaseServiceType.Postgres: lambda **kwargs: PostgresSource(**kwargs),
    DatabaseServiceType.Mysql: lambda **kwargs: MysqlSource(**kwargs),
    DatabaseServiceType.Oracle: lambda **kwargs: OracleSource(**kwargs),
    DatabaseServiceType.Hive: lambda **kwargs: HiveSource(**kwargs),
    DatabaseServiceType.Mssql: lambda **kwargs: MssqlSource(**kwargs),
    StorageServiceType.S3: lambda **kwargs: S3Source(**kwargs),
    DatabaseServiceType.Altibase: lambda **kwargs: AltibaseSource(**kwargs),
    StorageServiceType.MinIO: lambda **kwargs: MinioSource(**kwargs),
    DatabaseServiceType.MariaDB: lambda **kwargs: MariadbSource(**kwargs),
    DatabaseServiceType.Druid: lambda **kwargs: DruidSource(**kwargs),
    DatabaseServiceType.Trino: lambda **kwargs: TrinoSource(**kwargs),
}

def get_source(system_id, service_type: Union[PipelineServiceType, DatabaseServiceType, SearchServiceType, StorageServiceType, str],
               sink_type, sink_host, sink_port,
               source_host, source_port, source_user, source_password, source_database, source_catalog,
               source_filter):

    if service_type in DatabaseServiceType.__members__:
        service_type = DatabaseServiceType(service_type)
    elif service_type in ("Tibero", "Altibase"):
        pass
    elif service_type in StorageServiceType.__members__:
        service_type = StorageServiceType(service_type)
    else:
        raise Exception(f"system_type invalid. {service_type}")

    logger.info(f"filter : {source_filter}")

    source_factory = source_factory_map.get(service_type)

    return source_factory(
        system_id = system_id,
        service_type=service_type,
        sink_type=sink_type,
        sink_host=sink_host,
        sink_port=sink_port,
        source_host=source_host,
        source_port=source_port,
        source_user=source_user,
        source_password=source_password,
        source_filter=source_filter,
        source_database=source_database,
        source_catalog=source_catalog
    )


def metadata_collector_execute(system_id, sink="file", filter_include_dict=None, filter_exclude_dict=None, threads=None):

    system_id, system_type, source_host, source_port, source_user, source_password, source_database, source_catalog, source_filter = metadata_process.get_config("ingestion", system_id, filter_include_dict, filter_exclude_dict, threads)

    sink_host = config.sink_host
    sink_port = config.sink_port

    source = get_source(system_id, system_type, sink, sink_host, sink_port, source_host, source_port, source_user, source_password, source_database, source_catalog, source_filter)

    # db 상태 업데이트
    metadata_process.set_meta_system_status(system_id, "INGESTION")

    from services.common.metadata import MetadataExecutor
    MetadataExecutor.execute(source)



