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

import unittest
from unittest.mock import patch

from metadata.clients.azure_client import AzureClient
from metadata.generated.schema.security.credentials.azureCredentials import (
    AzureCredentials,
)


class TestAzureClient(unittest.TestCase):
    @patch("azure.identity.ClientSecretCredential")
    @patch("azure.identity.DefaultAzureCredential")
    def test_create_client(
        self,
        mock_default_credential,
        mock_client_secret_credential,
    ):
        # Test with ClientSecretCredential
        credentials = AzureCredentials(
            clientId="clientId", clientSecret="clientSecret", tenantId="tenantId"
        )
        instance = AzureClient(credentials)
        instance.create_client()

        mock_client_secret_credential.assert_called_once()
        mock_client_secret_credential.reset_mock()

        credentials = AzureCredentials(
            clientId="clientId",
        )
        instance = AzureClient(credentials)

        instance.create_client()

        mock_default_credential.assert_called_once()

    @patch("azure.storage.blob.BlobServiceClient")
    def test_create_blob_client(self, mock_blob_service_client):
        credentials = AzureCredentials(
            clientId="clientId", clientSecret="clientSecret", tenantId="tenantId"
        )
        with self.assertRaises(ValueError):
            AzureClient(credentials=credentials).create_blob_client()

        credentials.accountName = "accountName"
        AzureClient(credentials=credentials).create_blob_client()
        mock_blob_service_client.assert_called_once()

    @patch("azure.keyvault.secrets.SecretClient")
    def test_create_secret_client(self, mock_secret_client):
        credentials = AzureCredentials(
            clientId="clientId", clientSecret="clientSecret", tenantId="tenantId"
        )
        with self.assertRaises(ValueError):
            AzureClient(credentials=credentials).create_secret_client()

        credentials.vaultName = "vaultName"
        AzureClient(credentials=credentials).create_secret_client()
        mock_secret_client.assert_called_once()


if __name__ == "__main__":
    unittest.main()
