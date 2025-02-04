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
Multi DB Source Abstract class
"""

from abc import ABC, abstractmethod
from typing import Iterable, Optional


class MultiDBSource(ABC):
    @abstractmethod
    def get_configured_database(self) -> Optional[str]:
        """
        Method to return the name of default configured database if available
        """

    @abstractmethod
    def get_database_names_raw(self) -> Iterable[str]:
        """
        Method to return the name of all databases.
        """

    def _execute_database_query(self, query: str) -> Iterable[str]:
        results = self.connection.execute(query)  # pylint: disable=no-member
        for res in results:
            row = list(res)
            yield row[0]
