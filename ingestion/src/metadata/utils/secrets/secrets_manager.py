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
Secrets manager interface
"""
from abc import abstractmethod

from metadata.utils.logger import ingestion_logger
from metadata.utils.singleton import Singleton

logger = ingestion_logger()


class SecretsManager(metaclass=Singleton):
    """
    Abstract class implemented by different secrets' manager providers.

    It contains a set of auxiliary methods for adding missing fields which have been encrypted in the secrets' manager
    providers.
    """

    @abstractmethod
    def get_string_value(self, secret_id: str) -> str:
        """
        :param secret_id: The secret id to retrieve
        :return: The value of the secret
        """
