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
Metadata data insight producer interface. This is an abstract class that
defines the interface for all data insight producers.
"""

from abc import ABC, abstractmethod

from metadata.ingestion.server.server_api import ServerInterface


class ProducerInterface(ABC):
    def __init__(self, metadata: ServerInterface):
        """instantiate a producer object"""
        self.metadata = metadata

    @abstractmethod
    def fetch_data(self, limit, fields, entities_cache=None):
        """fetch data from source"""
        raise NotImplementedError
