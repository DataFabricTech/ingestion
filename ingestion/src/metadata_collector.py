from typing import Union

from metadata.generated.schema.entity.services.pipelineService import PipelineServiceType
from metadata.generated.schema.entity.services.databaseService import DatabaseServiceType
from metadata.generated.schema.entity.services.searchService import SearchServiceType
from metadata.generated.schema.entity.services.storageService import StorageServiceType

from metadata.utils.logger import ingestion_logger
from utils.security_manager import SecurityManager
from utils.process_config import config
from utils.metadata_process import MetadataProcess


logger = ingestion_logger()
logger.setLevel("INFO")

metadata_process = MetadataProcess()

def get_config(system_id, filter_include_dict, filter_exclude_dict):

    system_info = metadata_process.get_meta_system_info(system_id)
    system_id = system_info['system_id']
    host = system_info['host'] if 'host' in system_info else None
    port = system_info['port'] if 'port' in system_info else None
    system_type = system_info['system_type'] if 'system_type' in system_info else None
    database = system_info['database'] if 'database' in system_info else None
    schema = system_info['schema'] if 'schema' in system_info else None
    user = system_info['login'] if 'login' in system_info else None
    password = system_info['password'] if 'password' in system_info else None


    # system_info['filter_config'] : 25.07.21 사용안함
    source_filter = metadata_process.get_source_filter('ingestion', host, port, system_type, database, schema, None, filter_include_dict, filter_exclude_dict)

    if password is not None and len(password) > 0:
        password = SecurityManager.decodeWithcryptkey(config.crypt_key, password)

    return system_id, system_type, host, port, user, password, database, source_filter

def get_source(system_id, service_type: Union[PipelineServiceType, DatabaseServiceType, SearchServiceType, StorageServiceType, str],
# def get_source(service_type: Union[PipelineServiceType, DatabaseServiceType, SearchServiceType, str],
               sink_type, sink_host, sink_port,
               source_host, source_port, source_user, source_password, source_database, source_filter):

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

    if service_type in DatabaseServiceType.__members__:
        service_type = DatabaseServiceType(service_type)
    elif service_type in ("Tibero", "Altibase"):
        pass
    elif service_type in StorageServiceType.__members__:
        service_type = StorageServiceType(service_type)
    else:
        raise Exception(f"system_type invalid. {service_type}")

    logger.info(f"filter : {source_filter}")

    if service_type == DatabaseServiceType.Tibero:
        source = TiberoSource(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host, sink_port=sink_port,
                              source_host=source_host, source_port=source_port, source_user=source_user, source_password=source_password, source_filter=source_filter)
    elif service_type == DatabaseServiceType.Postgres:
        source = PostgresSource(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host, sink_port=sink_port,
                                source_host=source_host, source_port=source_port, source_user=source_user, source_password=source_password, source_database=source_database, source_filter=source_filter)
    elif service_type == DatabaseServiceType.Mysql:
        source = MysqlSource(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host, sink_port=sink_port,
                             source_host=source_host, source_port=source_port, source_user=source_user, source_password=source_password, source_filter=source_filter)
    elif service_type == DatabaseServiceType.Oracle:
        source = OracleSource(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host,  sink_port=sink_port,
                              source_host=source_host, source_port=source_port, source_user=source_user, source_password=source_password, source_database=source_database, source_filter=source_filter)
    elif service_type == DatabaseServiceType.Hive:
        source = HiveSource(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host, sink_port=sink_port,
                            source_host=source_host, source_port=source_port, source_user=source_user, source_password=source_password, source_database=source_database, source_filter=source_filter)
    elif service_type == DatabaseServiceType.Mssql:
        source = MssqlSource(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host, sink_port=sink_port,
                             source_host=source_host, source_port=source_port, source_user=source_user, source_password=source_password, source_database=source_database, source_filter=source_filter)
    elif service_type == StorageServiceType.S3:
        source = S3Source(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host, sink_port=sink_port,
                          source_user=source_user, source_password=source_password, source_filter=source_filter)
    # elif service_type == FilesystemServiceType.Linux:
    #     source = LinuxSource(service_type=service_type, sink_type=sink_type, sink_host=sink_host, sink_port=sink_port,
    #                          source_host=source_host, source_port=source_port,source_user=source_user, source_password=source_password, source_filter=source_filter)
    elif service_type == DatabaseServiceType.Altibase:
        source = AltibaseSource(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host, sink_port=sink_port,
                                source_host=source_host, source_port=source_port, source_user=source_user, source_password=source_password, source_filter=source_filter)
    elif service_type == StorageServiceType.MinIO:
        source = MinioSource(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host, sink_port=sink_port,
                             source_host=source_host, source_port=source_port, source_user=source_user, source_password=source_password, source_filter=source_filter)
    elif service_type == DatabaseServiceType.MariaDB:
        source = MariadbSource(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host, sink_port=sink_port,
                             source_host=source_host, source_port=source_port, source_user=source_user, source_password=source_password, source_filter=source_filter)
    elif service_type == DatabaseServiceType.Druid:
        source = DruidSource(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host, sink_port=sink_port,
                               source_host=source_host, source_port=source_port, source_user=source_user, source_password=source_password, source_filter=source_filter)
    elif service_type == DatabaseServiceType.Trino:
        source = TrinoSource(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host, sink_port=sink_port,
                             source_host=source_host, source_port=source_port, source_user=source_user, source_database=source_database, source_filter=source_filter)

    return source


def metadata_collector_execute(system_id, sink="file", filter_include_dict=None, filter_exclude_dict=None):

    system_id, system_type, source_host, source_port, source_user, source_password, source_database, source_filter = get_config(system_id, filter_include_dict, filter_exclude_dict)

    # if (source_host is None) or (source_port is None) or (source_user is None):
    #     raise Exception('source config invalid.')

    sink_host = config.sink_host
    sink_port = config.sink_port

    source = get_source(system_id, system_type, sink, sink_host, sink_port, source_host, source_port, source_user, source_password, source_database, source_filter)

    # db 상태 업데이트
    metadata_process.set_meta_system_status(system_id, "INGESTION")

    from services.common.metadata import MetadataExecutor
    MetadataExecutor.execute(source)



