from utils.process_config import config
from utils.request_manager import RequestManager
from utils.security_manager import SecurityManager

from metadata.generated.schema.entity.services.databaseService import DatabaseServiceType
from metadata.generated.schema.entity.services.storageService import StorageServiceType

class MetadataProcess:

    def get_meta_system_info(self, system_id):

        request_manager = RequestManager()
        response = request_manager.request_get(url=f"{config.catalog_api_base_url}{config.get_meta_system_info_api}?system_id={system_id}")
        return response.json()

    def set_meta_system_status(self, system_id, status):

        url = f"{config.catalog_api_base_url}{config.set_meta_ingestion_status_api}?system_id={system_id}&status={status}"
        request_manager = RequestManager()
        request_manager.request_put(url=url)

    def is_data_catalog_storage(self, host, port):
        return config.minio_url == f"{host}:{port}"

    def get_database_filter(self):
        filter = {
            "type": "DatabaseMetadata",
            "markDeletedTables": False,
            "markDeletedStoredProcedures": False,
            "includeTables": True,
            "includeViews": True,
            "includeTags": False,
            "includeStoredProcedures": False,
            "queryLogDuration": 1,
            "queryParsingTimeoutLimit": 300,
            "useFqnForFiltering": False,
            "databaseFilterPattern": {
                "includes": [],
                "excludes": []
            },
            "schemaFilterPattern": {
                "includes": [],
                "excludes": []
            },
            "tableFilterPattern": {
                "includes": [],
                "excludes": []
            }
        }

        return filter

    def get_storage_filter(self, collector_type, is_data_catalog_storage):
        if is_data_catalog_storage:
            bucket_exclude_pattern = ["datacatalog", "meta-data-sample", "dataextract"] # data catalog 사용 bucket
        else:
            bucket_exclude_pattern = []

        if "ingestion" == collector_type:
            source_filter = {
                "type": "StorageMetadata",
                "bucketFilterPattern": {
                    "includes": [],
                    "excludes": bucket_exclude_pattern
                },
                "containerFilterPattern": {
                    "includes": [],
                    "excludes": []
                },
                "useFqnForFiltering": True
            }
        elif "profile" == collector_type:
            source_filter = {
                "type": "StorageProfiler",
                "bucketFilterPattern": {
                    "includes": [],
                    "excludes": bucket_exclude_pattern
                },
                "containerFilterPattern": {
                    "includes": [],
                    "excludes": []
                },
                "useFqnForFiltering": True,
                "generateSampleData": True,
                "computeMetrics": True,
                "processPiiSensitive": False,
                "confidence": 80.0,
                "profileSampleType": "PERCENTAGE",
                "profileSample": 60,
                "sampleDataCount": 70,
                "threadCount": 5.0,
                "timeoutSeconds": 43200
            }
        return source_filter

    def get_database_type_filter(self, filter_dict):
        database_filter = []
        schema_filter = []
        table_filter = []
        if filter_dict is not None:
            if "database" in filter_dict:
                database_filter = filter_dict["database"]
            if "schema" in filter_dict:
                schema_filter = filter_dict["schema"]
            if "table" in filter_dict:
                table_filter = filter_dict["table"]

        return database_filter, schema_filter, table_filter

    def get_storage_type_filter(self, filter_dict):
        bucket_filter = []
        object_filter = []
        if filter_dict is not None:
            if "bucket" in filter_dict:
                bucket_filter = filter_dict["bucket"]
            if "object" in filter_dict:
                object_filter = filter_dict["object"]

        return bucket_filter, object_filter

    def get_source_filter(self, collector_type, host, port, service_type, database, schema, source_fileter_for_db, filter_include_dict, filter_exclude_dict):

        source_filter = None
        # database
        if service_type in DatabaseServiceType.__members__:
            source_filter = self.get_database_filter()
            include_database_filter, include_schema_filter, include_table_filter = self.get_database_type_filter(filter_include_dict)
            exclude_database_filter, exclude_schema_filter, exclude_table_filter = self.get_database_type_filter(filter_exclude_dict)

            if service_type in [DatabaseServiceType.Oracle.value, DatabaseServiceType.Postgres.value,  DatabaseServiceType.Mssql.value]:
                if database is not None:
                    include_database_filter.append(database)
                if schema is not None:
                    include_schema_filter.append(schema)

            if service_type in [DatabaseServiceType.Hive.value, DatabaseServiceType.Mysql.value, DatabaseServiceType.MariaDB.value]:
                # 해당 system들은 schema를 database 개념으로 사용하기 때문에 database로 설정된 값을 schema에 적용:25.08.04
                if database is not None:
                    include_schema_filter.append(database)

            #custom
            if service_type in ["Tibero"]:
                if schema is not None:
                    include_database_filter.append(schema)
                if database is not None:
                    include_schema_filter.append(database)

            # custom : altibase 는 schema만 입력(database는 필터 동작 X)
            if service_type in ["Altibase"]:
                if schema is not None:
                    include_schema_filter.append(schema)


            source_filter["databaseFilterPattern"]["includes"].extend(include_database_filter)
            source_filter["databaseFilterPattern"]["excludes"].extend(exclude_database_filter)

            source_filter["schemaFilterPattern"]["includes"].extend(include_schema_filter)
            source_filter["schemaFilterPattern"]["excludes"].extend(exclude_schema_filter)

            source_filter["tableFilterPattern"]["includes"].extend(include_table_filter)
            source_filter["tableFilterPattern"]["excludes"].extend(exclude_table_filter)

        #storage
        if service_type in StorageServiceType.__members__:
            source_filter = self.get_storage_filter(collector_type, self.is_data_catalog_storage(host, port))
            include_bucket_filter, include_object_filter = self.get_storage_type_filter(filter_include_dict)
            exclude_bucket_filter, exclude_object_filter = self.get_storage_type_filter(filter_exclude_dict)

            source_filter["bucketFilterPattern"]["includes"].extend(include_bucket_filter)
            source_filter["bucketFilterPattern"]["excludes"].extend(exclude_bucket_filter)

            source_filter["containerFilterPattern"]["includes"].extend(include_object_filter)
            source_filter["containerFilterPattern"]["excludes"].extend(exclude_object_filter)


        if source_filter is None:
            raise Exception(f"filter config invalid. {service_type}, {host}")

        return source_filter

    def get_config(self, collector_type, system_id, filter_include_dict, filter_exclude_dict):

        system_info = self.get_meta_system_info(system_id)
        system_id = system_info['system_id']
        host = system_info['host'] if 'host' in system_info else None
        port = system_info['port'] if 'port' in system_info else None
        system_type = system_info['system_type'] if 'system_type' in system_info else None
        database = system_info['database'] if 'database' in system_info else None
        schema = system_info['schema'] if 'schema' in system_info else None
        user = system_info['login'] if 'login' in system_info else None
        password = system_info['password'] if 'password' in system_info else None
        catalog = system_info['catalog'] if 'catalog' in system_info else None

        # system_info['filter_config'] : 25.07.21 사용안함
        source_filter = self.get_source_filter(collector_type, host, port, system_type, database, schema, None, filter_include_dict, filter_exclude_dict)

        if password is not None and len(password) > 0:
            password = SecurityManager.decodeWithcryptkey(config.crypt_key, password)

        return system_id, system_type, host, port, user, password, database, catalog, source_filter
