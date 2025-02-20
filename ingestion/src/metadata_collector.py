from typing import Union

from metadata.generated.schema.entity.services.pipelineService import PipelineServiceType
from metadata.generated.schema.entity.services.databaseService import DatabaseServiceType
from metadata.generated.schema.entity.services.searchService import SearchServiceType
from metadata.generated.schema.entity.services.storageService import StorageServiceType
from metadata.generated.schema.entity.services.filesystemService import FilesystemServiceType
from metadata.generated.schema.entity.data.ingestion import MetadataSystemInfo
from metadata.ingestion.ometa.client import REST, ClientConfig
from metadata.ingestion.ometa.routes import ROUTES

from metadata.utils.logger import ingestion_logger
from utils.security_manager import SecurityManager

logger = ingestion_logger()
logger.setLevel("INFO")


def get_source_filter(service_type, database, sourceFileter):

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

        if service_type in [DatabaseServiceType.Oracle.value]:
            database_filter["schemaFilterPattern"] = {
                "includes": [database],
                "excludes": []
            }
        if service_type == DatabaseServiceType.Postgres.value:
            database_filter["databaseFilterPattern"] = {
                "includes": [database],
                "excludes": []
            }
            database_filter["schemaFilterPattern"] = {
                "includes": ['public'],
                "excludes": []
            }
        if service_type == DatabaseServiceType.Mssql.value: # 해당 DATABASE 만 수집되도록 수정 필요
            database_filter["databaseFilterPattern"] = {
                "includes": ['<DB_NAME>'],
                "excludes": []
            }
            database_filter["schemaFilterPattern"] = {
                "includes": ['<DB_USER>'],
                "excludes": []
            }


        return database_filter
    else:
        return sourceFileter


# def update_ingestion_status(self, system_id, status, err_message=None):
#
#     data = IngestionStatus(
#         system_id = system_id,
#         status = status,
#         err_description = err_message,
#         user = 'METADATA COLLECTOR')
# resp = self.client.get(f"{self.get_suffix(entity)}/{path}{fields_str}")
# if not resp:
#     raise EmptyPayloadException(
#         f"Got an empty response when trying to GET from {self.get_suffix(entity)}/{path}{fields_str}"
#     )
#     try:
#         self.client.put(
#             ROUTES.get(data.__class__.__name__), data=data.json(encoder=show_secrets_encoder)
#         )
#     except Exception as exc:
#         logger.error(f"Error trying to PUT to {ROUTES.get(data.__class__.__name__)}, {data.json()}, {exc}")
#
#     logger.info(f"ingestion status update [{system_id}]")


def get_meta_system_info(system_id, host, port):

    # headers = {"accept":"application/json"}
    # catalog_api_config: ClientConfig = ClientConfig(
    #     base_url=f"http://{host}:{port}/api",
    #     api_version="v1",
    #     # access_token="no_token",
    #     auth_token_mode=None,
    #     # auth_header="Authorization",
    #     extra_headers=headers,
    #     # auth_token=self._auth_provider.get_access_token,
    #     # verify=get_verify_ssl(self.config.sslConfig),
    # )
    # api_client = REST(catalog_api_config)
    # params = {"system_id":system_id}
    # fields_str = f"?system_id={system_id}"
    # response = api_client.get(path=f"/system/meta/system/info{fields_str}")
    #
    # print(response)
    # return response
    from app.utils.client import SqlalchemyOrmClient, postgresql_url
    client = SqlalchemyOrmClient(postgresql_url(host='<INTERNAL_HOST>', user='<DB_USER>', passwd='<DB_PASSWORD>', db='<DB_NAME>'), charset='utf-8', sql_log=True)
    client.__enter__()
    result = client.select_one(f"select <columns> from <table> where system_id = '{system_id}'")
    client.__exit__(None, None, None)

    hostport = result[2]
    if result[3] != None:
        hostport = f"{result[2]}:{result[3]}"

    source_filter = get_source_filter(result[1], result[6], result[7])

    password = SecurityManager.decodeWithcryptkey(result[5])

    # return result[0], result[1], hostport, result[4], password, result[6], source_filter
    # source_filter = get_source_filter('Mssql', '<DB_NAME>', None)
    # source_filter = get_source_filter('Postgres', 'data_catalog', None)
    source_filter = get_source_filter('Oracle', '<DB_USER>', None)
    print(source_filter)
    #<columns>
    # return 'phy-mssql', 'Mssql', '<INTERNAL_HOST>:1433', '<DB_USER>', '<DB_PASSWORD>!', '<DB_NAME>', source_filter
    # return 'test', 'Postgres', '<INTERNAL_HOST>:5432', 'data_catalog', '<DB_PASSWORD>', 'data_catalog', source_filter
    return 'test', 'Oracle', '<INTERNAL_HOST>:1521', '<DB_USER>', '<DB_PASSWORD>', '<DB_USER>', source_filter

def set_meta_system_status(system_id, status):
    from app.utils.client import SqlalchemyOrmClient, postgresql_url
    client = SqlalchemyOrmClient(postgresql_url(host='<INTERNAL_HOST>', user='<DB_USER>', passwd='<DB_PASSWORD>', db='<DB_NAME>'), charset='utf-8', sql_log=True)
    client.__enter__()
    client.execute(f"update <table> set status = '{status}' where system_id = '{system_id}'")
    client.__exit__(None, None, None)

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


def metadata_collector_execute(system_id, sink="file", sink_host="localhost", sink_port=8585):

    system_id, system_type, source_hostport, source_user, source_password, source_database, source_filter = get_meta_system_info(system_id, sink_host, sink_port)

    if (source_hostport is None) or (source_user is None):
        raise Exception('source config invalid.')

    if sink not in ['file', 'db', 'metadata-rest']:
        raise Exception('sink_type invalid. file, db, metadata-rest')

    source = get_source(system_id, system_type, sink, sink_host, sink_port, source_hostport, source_user, source_password, source_database, source_filter)

    # db 상태 업데이트
    set_meta_system_status(system_id, "INGESTION")

    from services.common.metadata import MetadataExecutor
    MetadataExecutor.execute(source)

    #profile phy
    # from profiling.common.profiler import ProfilerExecutor
    # ProfilerExecutor.execute(source)


