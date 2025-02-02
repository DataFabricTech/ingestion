# Copyright 2024 Mobigen
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Notice!
# This software is based on https://open-metadata.org and has been modified accordingly.

"""
Kafka source ingestion
"""
from typing import Optional

from metadata.generated.schema.entity.services.connections.messaging.kafkaConnection import (
    KafkaConnection,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.server.server_api import ServerInterface
from metadata.ingestion.source.messaging.common_broker_source import CommonBrokerSource
from metadata.utils.ssl_manager import SSLManager


class KafkaSource(CommonBrokerSource):
    def __init__(self, config: WorkflowSource, metadata: ServerInterface):
        self.ssl_manager = None
        service_connection = config.serviceConnection.__root__.config
        if service_connection.schemaRegistrySSL:

            self.ssl_manager = SSLManager(
                ca=service_connection.schemaRegistrySSL.__root__.caCertificate,
                key=service_connection.schemaRegistrySSL.__root__.sslKey,
                cert=service_connection.schemaRegistrySSL.__root__.sslCertificate,
            )
            service_connection = self.ssl_manager.setup_ssl(
                config.serviceConnection.__root__.config.sslConfig
            )
        super().__init__(config, metadata)

    @classmethod
    def create(
        cls, config_dict, metadata: ServerInterface, pipeline_name: Optional[str] = None
    ):
        config: WorkflowSource = WorkflowSource.parse_obj(config_dict)
        connection: KafkaConnection = config.serviceConnection.__root__.config
        if not isinstance(connection, KafkaConnection):
            raise InvalidSourceException(
                f"Expected KafkaConnection, but got {connection}"
            )
        return cls(config, metadata)
