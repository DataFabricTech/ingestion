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

import os
from unittest import TestCase, mock

from metadata.generated.schema.entity.services.connections.metadata.metadataConnection import (
    MetadataConnection,
)
from metadata.generated.schema.security.secrets.secretsManagerClientLoader import (
    SecretsManagerClientLoader,
)
from metadata.generated.schema.security.secrets.secretsManagerProvider import (
    SecretsManagerProvider,
)
from metadata.ingestion.server.auth_provider import MetadataAuthenticationProvider
from metadata.ingestion.server.server_api import ServerInterface
from metadata.utils.secrets.aws_secrets_manager import AWSSecretsManager
from metadata.utils.secrets.db_secrets_manager import DBSecretsManager
from metadata.utils.singleton import Singleton


class OMetaSecretManagerTest(TestCase):
    metadata: ServerInterface
    aws_server_config: MetadataConnection
    local_server_config: MetadataConnection

    @classmethod
    def setUp(cls) -> None:
        Singleton.clear_all()
        cls.local_server_config = MetadataConnection(
            hostPort="http://localhost:8585/api",
            enableVersionValidation=False,
        )
        cls.aws_server_config = MetadataConnection(
            hostPort="http://localhost:8585/api",
            secretsManagerProvider=SecretsManagerProvider.aws,
            secretsManagerLoader=SecretsManagerClientLoader.noop,
            enableVersionValidation=False,
        )

    def test_ometa_with_local_secret_manager(self):
        self._init_local_secret_manager()
        assert type(self.metadata.secrets_manager_client) is DBSecretsManager
        assert type(self.metadata._auth_provider) is MetadataAuthenticationProvider

    @mock.patch.dict(os.environ, {"AWS_DEFAULT_REGION": "us-east-2"}, clear=True)
    def test_ometa_with_aws_secret_manager(self):
        self._init_aws_secret_manager()
        assert type(self.metadata.secrets_manager_client) is AWSSecretsManager
        assert type(self.metadata._auth_provider) is MetadataAuthenticationProvider

    def _init_local_secret_manager(self):
        self.metadata = ServerInterface(self.local_server_config)

    def _init_aws_secret_manager(self):
        self.metadata = ServerInterface(self.aws_server_config)
