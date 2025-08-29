from typing import Union

from metadata.generated.schema.entity.services.storageService import StorageServiceType

from metadata.utils.logger import ingestion_logger
from utils.process_config import config
from utils.metadata_process import MetadataProcess

metadata_process = MetadataProcess()

logger = ingestion_logger()
logger.setLevel("INFO")

def get_source(system_id, service_type: Union[StorageServiceType],
               sink_type, sink_host, sink_port,
               source_host, source_port, source_user, source_password, source_database, source_filter):

    from services.storage.minio.source import MinioSource

    if service_type in StorageServiceType.__members__:
        service_type = StorageServiceType(service_type)
    else:
        raise Exception(f"system_type invalid. {service_type}")

    logger.info(f"filter : {source_filter}")

    if service_type == StorageServiceType.MinIO:
        source = MinioSource(system_id = system_id, service_type=service_type, sink_type=sink_type, sink_host=sink_host, sink_port=sink_port,
                             source_host=source_host, source_port=source_port, source_user=source_user, source_password=source_password, source_filter=source_filter,
                             processor_type="orm-profiler")
    return source


def metadata_profiler_execute(system_id, sink="file", filter_include_dict=None, filter_exclude_dict=None):

    system_id, system_type, source_host, source_port, source_user, source_password, source_database, source_catalog, source_filter = metadata_process.get_config("profile", system_id, filter_include_dict, filter_exclude_dict)

    if (source_host is None) or (source_port is None) or (source_user is None):
        raise Exception('source config invalid.')

    sink_host = config.sink_host
    sink_port = config.sink_port

    source = get_source(system_id, system_type, sink, sink_host, sink_port, source_host, source_port, source_user, source_password, source_database, source_filter)

    # db 상태 업데이트
    # metadata_process.set_meta_system_status(system_id, "INGESTION")

    from services.common.profiler import ProfilerExecutor
    ProfilerExecutor.execute(source)



