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
Iceberg FileSystem base module.
"""
from abc import ABC, abstractmethod
from typing import Union

from metadata.generated.schema.security.credentials.awsCredentials import AWSCredentials
from metadata.generated.schema.security.credentials.azureCredentials import (
    AzureCredentials,
)

FileSystemConfig = Union[AWSCredentials, AzureCredentials, None]


class IcebergFileSystemBase(ABC):
    @classmethod
    @abstractmethod
    def get_fs_params(cls, fs_config: FileSystemConfig) -> dict:
        """Returns a Catalog for given catalog_config."""
