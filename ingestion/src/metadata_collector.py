from typing import Union

from metadata.generated.schema.entity.services.pipelineService import PipelineServiceType
from metadata.generated.schema.entity.services.databaseService import DatabaseServiceType
from metadata.generated.schema.entity.services.searchService import SearchServiceType
from metadata.generated.schema.entity.services.storageService import StorageServiceType
from metadata.generated.schema.entity.services.filesystemService import FilesystemServiceType

from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()
logger.setLevel("INFO")

import sys

def getSourceFilter(sourceFileter):

    #profile phy
    # return {'type': 'Profiler'}

    if sourceFileter == None:
        return {
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
    else:
        return sourceFileter

def getMetaSystemInfo(system_name):
    from app.utils.client import SqlalchemyOrmClient, postgresql_url
    client = SqlalchemyOrmClient(postgresql_url(host='<INTERNAL_HOST>', user='<DB_USER>', passwd='<DB_PASSWORD>', db='<DB_NAME>'), charset='utf-8', sql_log=True)
    client.__enter__()
    result = client.select_one(f"select <columns> from <table> where system_name = '{system_name}'")
    client.__exit__(None, None, None)

    hostport = result[2]
    if result[3] != None:
        hostport = f"{result[2]}:{result[3]}"

    sourceFilter = getSourceFilter(result[7])

    return result[0], result[1], hostport, result[4], result[5], result[6], sourceFilter


def get_source(system_id, service_type: Union[PipelineServiceType, DatabaseServiceType, SearchServiceType, StorageServiceType, FilesystemServiceType, str],
# def get_source(service_type: Union[PipelineServiceType, DatabaseServiceType, SearchServiceType, str],
               sink_type, sink_host,
               source_hostport, source_user, source_password, source_database, source_filter):

    from services.database.custom.tibero.source import TiberoSource
    from services.database.postgres.source import PostgresSource
    from services.database.mysql.source import MysqlSource
    from services.database.oracle.source import OracleSource
    from services.database.hive.source import HiveSource
    from services.database.mssql.source import MssqlSource
    from services.storage.s3.source import S3Source
    from services.filesystem.linux.source import LinuxSource

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
        source = TiberoSource(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host,
                              source_hostport=source_hostport, source_user=source_user, source_password=source_password, source_filter=source_filter)
    elif service_type == DatabaseServiceType.Postgres:
        source = PostgresSource(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host, source_hostport=source_hostport,
                                source_user=source_user, source_password=source_password, source_database=source_database, source_filter=source_filter)
    elif service_type == DatabaseServiceType.Mysql:
        source = MysqlSource(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host,
                             source_hostport=source_hostport, source_user=source_user, source_password=source_password, source_filter=source_filter)
    elif service_type == DatabaseServiceType.Oracle:
        source = OracleSource(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host, source_hostport=source_hostport,
                              source_user=source_user, source_password=source_password, source_database=source_database, source_filter=source_filter)
    elif service_type == DatabaseServiceType.Hive:
        source = HiveSource(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host,
                            source_hostport=source_hostport, source_user=source_user, source_password=source_password, source_database=source_database, source_filter=source_filter)
    elif service_type == DatabaseServiceType.Mssql:
        source = MssqlSource(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host,
                             source_hostport=source_hostport, source_user=source_user, source_password=source_password, source_database=source_database, source_filter=source_filter)
    elif service_type == StorageServiceType.S3:
        source = S3Source(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host,
                          source_user=source_user, source_password=source_password, source_filter=source_filter)
    elif service_type == FilesystemServiceType.Linux:
        source = LinuxSource(service_type=service_type, sink_type=sink_type, sink_host=sink_host,
                             source_hostport=source_hostport,source_user=source_user, source_password=source_password, source_filter=source_filter)

    return source




def metadataExecute(system_name, sink="file", sink_host="localhost"):

    system_id, system_type, source_hostport, source_user, source_password, source_database, source_filter = getMetaSystemInfo(system_name)
    print(system_id, system_type, source_hostport, source_user, source_password, source_database, source_filter)

    if (source_hostport is None) or (source_user is None):
        raise Exception('source config invalid.')

    if sink not in ['file', 'db', 'metadata-rest']:
        raise Exception('sink_type invalid. file, db, metadata-rest')

    source = get_source(system_id, system_type, sink, sink_host, source_hostport, source_user, source_password, source_database, source_filter)

    from services.common.metadata import MetadataExecutor
    MetadataExecutor.execute(source)

    #profile phy
    # from profiling.common.profiler import ProfilerExecutor
    # ProfilerExecutor.execute(source)



if __name__ == "__main__":
    pass

    metadataExecute(system_name=sys.argv[1],
                    sink='metadata-rest',
                    sink_host='<INTERNAL_HOST>')

    # metadataExecute(system_name='Test-Oracle',
    #                 sink='metadata-rest',
    #                 sink_host='localhost')

    # metadataExecute(system_name=sys.argv[1], sink="file", sink_host="localhost")
    # metadataExecute(system_name="Test-Oracle", sink="file", sink_host="localhost")
