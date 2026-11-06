from metadata.generated.schema.metadataIngestion.workflow import (
    WorkflowConfig, OpenMetadataWorkflowConfig, Source, Sink
)

from metadata.generated.schema.entity.services.pipelineService import PipelineServiceType
from metadata.generated.schema.entity.services.databaseService import DatabaseServiceType
from metadata.generated.schema.entity.services.searchService import SearchServiceType
from metadata.generated.schema.entity.data.ingestion import CollectorStatus

from typing import Union
from abc import ABC, abstractmethod

class CommonSource(ABC):

    def __init__(self,
                 service_type: Union[PipelineServiceType, DatabaseServiceType, SearchServiceType, str],
                 sink_type='file',
                 sink_host='localhost',
                 sink_port=8585,
                 source_host=None,
                 source_port=None,
                 source_user=None,
                 source_password=None,
                 source_database=None,
                 source_filter=None,
                 system_id=None,
                 processor_type=None,
                 source_catalog=None,
                 es_protocol=None,
                 es_ssl_password=None,
                 es_jks_path=None):

        if system_id is None:
            raise ValueError("system_id 는 필수 값 입니다.")

        if service_type == DatabaseServiceType.Trino and source_catalog is None:
            raise ValueError("trino collector에는 source_catalog 는 필수 값 입니다.")

        self.service_type = service_type
        self.sink_type = sink_type
        self.sink_host = sink_host
        self.sink_port = sink_port
        self.source_host = source_host
        self.source_port = source_port
        self.source_user = source_user
        self.source_password = source_password
        self.source_database = source_database
        self.source_filter = source_filter
        self.system_id = system_id
        self.processor_type = processor_type
        self.source_catalog = source_catalog
        self.es_protocol = es_protocol
        self.es_ssl_password = es_ssl_password
        self.es_jks_path = es_jks_path

    def getMetadataWorkflowConfig(self) -> OpenMetadataWorkflowConfig:
        service_type_str = self.service_type if type(self.service_type) == str else self.service_type.value
        if self.sink_type == "file":
            sink = Sink(type="file", config={"filename": f"json/{service_type_str}_metadata.json"})
        # elif self.sink_type == "db":
        #     sink = Sink(type="db",
        #                 config=dict(host='<INTERNAL_HOST>',
        #                             user="data_catalog",
        #                             passwd="openmetadata_password",
        #                             db="openmetadata_db"))

                        # config=dict(host=self.sink_host,
                        #             user="openmetadata_user",
                        #             passwd="openmetadata_password",
                        #             db="openmetadata_db"))
        else:
            sink = Sink(type="metadata-rest", config={})

        if self.processor_type is not None:
            processor = {
                "type": self.processor_type,
                "config": {}
            }
        else:
            processor = None

        config = OpenMetadataWorkflowConfig(
            source=self._getSource(),
            sink=sink,
            workflowConfig=self.__getWorkflowConfig(),
            processor=processor
        )
        return config

    def __getWorkflowConfig(self) -> WorkflowConfig:
        from metadata.generated.schema.metadataIngestion.workflow import LogLevels
        from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
            OpenMetadataConnection,
        )

        if self.sink_type in ["file", "db"]:
            server_config = OpenMetadataConnection(**{
                "hostPort": f"http://{self.sink_host}:{self.sink_port}/api",  # 필수항목만 기재
            })
        else:
            # if self.sink_host == 'localhost':
            #     jwtToken = "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJlbWFpbCI6ImluZ2VzdGlvbi1ib3RAb3Blbm1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3MDc4MDQ5MjcsImV4cCI6bnVsbH0.qL5qwh-QAk2YdfBFAX9rDgMQLdVC9Ge1i_I7h3GS7hUV_gZfJcqe5RMp0K5ONT5cu8KIuSDK13Jk-nlQmXxGL9ppYD7nMuTiDwyGbEH_dxSSupihi1kE4UcbWsGuHnfQhQYfYUdNtabCn2fY3Y6r0PGQiXWSp3e94LTKHbvZljikxvu8jBoh5QCc2q1WvQGn41HJaGxQ0HK2b0twQq9G52v3-F0O1WgfC8yDykS5gOof2N45Fbx4ilDDWEmsTEz6mtLJ5j_n7b0tpq4hD3gSoNZcPtva8sclFepe2MB46_LELiEphZfQHEVNeBPHYBX03WRUOxoh2gGnY3CDIIugOw"
            # elif self.sink_host == '<INTERNAL_HOST>':
            #     jwtToken = "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJlbWFpbCI6ImluZ2VzdGlvbi1ib3RAb3Blbm1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3MDc4MTMyMjEsImV4cCI6bnVsbH0.E5UJHrIDRBzldqXP6WzoCOgCSW90YYjmWvQNWYXyInMX8decD4vwemRJdfH5jz7hLvrQNw8zOXpnMEQNoA-OiblcHehpQACUfb4QVbbmE7XMQ_N8h1W2x5dhzWe-u23Lndx0UvVPYc6ksZ2V6PktU7lA-aYpttYQ3J15APSETY_SD5EPuMZHXqHy8DUGDi7C8Y7wARmVWmb_ai-6u9t3HbrbkzhMXk4sLUAn5PQ5okfintNNGItCEfQNe-RNC-mgER3AF1piD72qjVFk8ykJ35GoH72omhXP3FnIdunCi7p0OVi0CQdFhsUXW2-gABOffhn7IHtM19PY9cckV0Psnw"
            #
            # jwtToken = "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJlbWFpbCI6ImluZ2VzdGlvbi1ib3RAb3Blbm1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3MDc4MDQ5MjcsImV4cCI6bnVsbH0.qL5qwh-QAk2YdfBFAX9rDgMQLdVC9Ge1i_I7h3GS7hUV_gZfJcqe5RMp0K5ONT5cu8KIuSDK13Jk-nlQmXxGL9ppYD7nMuTiDwyGbEH_dxSSupihi1kE4UcbWsGuHnfQhQYfYUdNtabCn2fY3Y6r0PGQiXWSp3e94LTKHbvZljikxvu8jBoh5QCc2q1WvQGn41HJaGxQ0HK2b0twQq9G52v3-F0O1WgfC8yDykS5gOof2N45Fbx4ilDDWEmsTEz6mtLJ5j_n7b0tpq4hD3gSoNZcPtva8sclFepe2MB46_LELiEphZfQHEVNeBPHYBX03WRUOxoh2gGnY3CDIIugOw"

            server_config = OpenMetadataConnection(**{
                "clusterName": "openmetadata",
                "type": "OpenMetadata",
                "hostPort": f"http://{self.sink_host}:{self.sink_port}/api",
                # "hostPort": f"http://{self.sink_host}:8000/api",
                "authProvider": "no-auth",
                # "authProvider": "openmetadata",
                # "verifySSL": "no-ssl",
                # "sslConfig": None,
                # "securityConfig": {
                #     "jwtToken": jwtToken
                # },
                # "secretsManagerProvider": "noop",
                # "secretsManagerLoader": "noop",
                "apiVersion": "v1",
                "includeTopics": True,
                "includeTables": True,
                "includeDashboards": True,
                "includePipelines": True,
                "includeMlModels": True,
                "includeUsers": True,
                "includeTeams": True,
                "includeGlossaryTerms": True,
                "includeTags": True,
                "includePolicy": True,
                "includeMessagingServices": True,
                "enableVersionValidation": True,
                "includeDatabaseServices": True,
                "includePipelineServices": True,
                "limitRecords": 1000,
                "forceEntityOverwriting": False,
                "storeServiceConnection": True,
                "elasticsSearch": None,
                "supportsDataInsightExtraction": True,
                "supportsElasticSearchReindexingExtraction": True,
                "extraHeaders": None
            })

        workflowConfig = WorkflowConfig(loggerLevel=LogLevels.DEBUG, openMetadataServerConfig=server_config)
        return workflowConfig

    @abstractmethod
    def _getSource(self) -> Source:
        pass

    # @staticmethod
    def check_result_status(self, status):
        if status == 1:
            return CollectorStatus.INGESTION_FAILED
        else:
            return CollectorStatus.INGESTION_COMPLETED
