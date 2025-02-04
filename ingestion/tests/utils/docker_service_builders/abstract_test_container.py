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

"""abstract test container for integration tests"""

from abc import ABC, abstractmethod

from testcontainers.core.container import DockerContainer


class AbstractTestContainer(ABC):
    @abstractmethod
    def get_connection_url(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def get_config(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def get_source_config(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def start(self):
        raise NotImplementedError

    @abstractmethod
    def stop(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def container(self) -> DockerContainer:
        raise NotImplementedError

    @property
    @abstractmethod
    def connector_type(self) -> str:
        raise NotImplementedError
