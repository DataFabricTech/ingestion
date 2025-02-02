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
Secrets manager implementation for local secrets manager
"""
from metadata.generated.schema.security.secrets.secretsManagerProvider import (
    SecretsManagerProvider,
)
from metadata.utils.secrets.secrets_manager import SecretsManager


class DBSecretsManager(SecretsManager):
    """
    Will return the string as received. The client does not need to pick
    it up from any external resource.
    """

    provider: str = SecretsManagerProvider.db.name

    def get_string_value(self, secret_id: str) -> str:
        return secret_id
