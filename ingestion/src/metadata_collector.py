from typing import Union

from metadata.generated.schema.entity.services.pipelineService import PipelineServiceType
from metadata.generated.schema.entity.services.databaseService import DatabaseServiceType
from metadata.generated.schema.entity.services.searchService import SearchServiceType
from metadata.generated.schema.entity.services.storageService import StorageServiceType
from metadata.generated.schema.entity.services.filesystemService import FilesystemServiceType

from metadata.utils.logger import ingestion_logger
from utils.security_manager import SecurityManager
from utils.process_config import config
from utils.request_manager import RequestManager

logger = ingestion_logger()
logger.setLevel("INFO")


def get_source_filter(service_type, database, schema, sourceFileter):

    #profile phy
    # return {'type': 'Profiler'}

    if sourceFileter == None or sourceFileter == '':
        database_filter = {
            "type": "DatabaseMetadata",
            "markDeletedTables": False,
            "markDeletedStoredProcedures": False,
            "includeTables": True,
            "includeViews": False,
            "includeTags": False,
            "includeStoredProcedures": False,
            "queryLogDuration": 1,
            "queryParsingTimeoutLimit": 300,
            "useFqnForFiltering": False,
            "schemaFilterPattern": {
                "includes": [],
                "excludes": []
            },
            "tableFilterPattern": {
                "includes": [],
                "excludes": []
            },
            "databaseFilterPattern": {
                "includes": [],
                "excludes": []
            }
        }

        if service_type in [DatabaseServiceType.Oracle.value, DatabaseServiceType.Postgres.value,  DatabaseServiceType.Mssql.value]:
            database_filter["databaseFilterPattern"] = {
                "includes": [database],
                "excludes": []
            }
            database_filter["schemaFilterPattern"] = {
                "includes": [schema],
                "excludes": []
            }

        if service_type in [DatabaseServiceType.Hive.value, DatabaseServiceType.Mysql.value]:
            database_filter["databaseFilterPattern"] = {
                "includes": [database],
                "excludes": []
            }

        #custom
        if service_type == "Tibero":
            database_filter["schemaFilterPattern"] = {
                "includes": [database],
                "excludes": []
            }
            database_filter["databaseFilterPattern"] = {
                "includes": [schema],
                "excludes": []
            }

        return database_filter
    else:
        return sourceFileter

def get_meta_system_info(system_id):

    request_manager = RequestManager()
    response = request_manager.request_get(url=f"{config.catalog_api_base_url}{config.get_meta_system_info_api}?system_id={system_id}")
    system_info = response.json()

    hostport = system_info['host']
    if system_info['port'] != None:
        hostport = f"{system_info['host']}:{system_info['port']}"

    source_filter = get_source_filter(system_info['system_type'], system_info['database'], system_info['schema'], system_info['filter_config'])

    password = SecurityManager.decodeWithcryptkey(config.crypt_key, system_info['password'])

    return system_info['system_id'], system_info['system_type'], hostport, system_info['login'], password, system_info['database'], source_filter

def set_meta_system_status(system_id, status):

    url = f"{config.catalog_api_base_url}{config.set_meta_ingestion_status_api}?system_id={system_id}&status={status}"
    request_manager = RequestManager()
    request_manager.request_put(url=url)


def get_source(system_id, service_type: Union[PipelineServiceType, DatabaseServiceType, SearchServiceType, StorageServiceType, FilesystemServiceType, str],
# def get_source(service_type: Union[PipelineServiceType, DatabaseServiceType, SearchServiceType, str],
               sink_type, sink_host, sink_port,
               source_hostport, source_user, source_password, source_database, source_filter):

    from services.database.custom.tibero.source import TiberoSource
    from services.database.postgres.source import PostgresSource
    from services.database.mysql.source import MysqlSource
    from services.database.oracle.source import OracleSource
    from services.database.hive.source import HiveSource
    from services.database.mssql.source import MssqlSource
    from services.storage.s3.source import S3Source
    # from services.filesystem.linux.source import LinuxSource

    if service_type in DatabaseServiceType.__members__:
        service_type = DatabaseServiceType(service_type)
    elif service_type in ("Tibero"):
        pass
    elif service_type in StorageServiceType.__members__:
        service_type = StorageServiceType(service_type)
    elif service_type in FilesystemServiceType.__members__:
        service_type = FilesystemServiceType(service_type)
    else:
        raise Exception(f"system_type invalid. {service_type}")

    logger.info(f"filter : {source_filter}")

    if service_type == "Tibero": #custom
        source = TiberoSource(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host, sink_port=sink_port,
                              source_hostport=source_hostport, source_user=source_user, source_password=source_password, source_filter=source_filter)
    elif service_type == DatabaseServiceType.Postgres:
        source = PostgresSource(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host, sink_port=sink_port,
                                source_hostport=source_hostport, source_user=source_user, source_password=source_password, source_database=source_database, source_filter=source_filter)
    elif service_type == DatabaseServiceType.Mysql:
        source = MysqlSource(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host, sink_port=sink_port,
                             source_hostport=source_hostport, source_user=source_user, source_password=source_password, source_filter=source_filter)
    elif service_type == DatabaseServiceType.Oracle:
        source = OracleSource(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host,  sink_port=sink_port,
                              source_hostport=source_hostport, source_user=source_user, source_password=source_password, source_database=source_database, source_filter=source_filter)
    elif service_type == DatabaseServiceType.Hive:
        source = HiveSource(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host, sink_port=sink_port,
                            source_hostport=source_hostport, source_user=source_user, source_password=source_password, source_database=source_database, source_filter=source_filter)
    elif service_type == DatabaseServiceType.Mssql:
        source = MssqlSource(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host, sink_port=sink_port,
                             source_hostport=source_hostport, source_user=source_user, source_password=source_password, source_database=source_database, source_filter=source_filter)
    elif service_type == StorageServiceType.S3:
        source = S3Source(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host, sink_port=sink_port,
                          source_user=source_user, source_password=source_password, source_filter=source_filter)
    # elif service_type == FilesystemServiceType.Linux:
    #     source = LinuxSource(service_type=service_type, sink_type=sink_type, sink_host=sink_host, sink_port=sink_port,
    #                          source_hostport=source_hostport,source_user=source_user, source_password=source_password, source_filter=source_filter)

    return source


def metadata_collector_execute(system_id, sink="file"):

    system_id, system_type, source_hostport, source_user, source_password, source_database, source_filter = get_meta_system_info(system_id)

    if (source_hostport is None) or (source_user is None):
        raise Exception('source config invalid.')

    if sink in ['file', 'db']:
        sink_host = "localhost"
        sink_port = 8585
    elif sink == 'metadata-rest':
        sink_host = config.sink_host
        sink_port = config.sink_port
    else:
        raise Exception('sink_type invalid. file, db, metadata-rest')

    source = get_source(system_id, system_type, sink, sink_host, sink_port, source_hostport, source_user, source_password, source_database, source_filter)

    # db 상태 업데이트
    set_meta_system_status(system_id, "INGESTION")

    from services.common.metadata import MetadataExecutor
    MetadataExecutor.execute(source)

    #profile phy
    # from profiling.common.profiler import ProfilerExecutor
    # ProfilerExecutor.execute(source)


