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

"""test container builder class"""

from typing import List

from .abstract_test_container import AbstractTestContainer
from .database_container.mysql_test_container import MySQLTestContainer
from .database_container.postgres_test_container import PostgresTestContainer


class ContainerBuilder:
    def __init__(self) -> None:
        self.containers: List[AbstractTestContainer] = []

    def run_mysql_container(self):
        """build mysql container"""
        container = MySQLTestContainer()
        self.containers.append(container)
        return container

    def run_postgres_container(self):
        """build mysql container"""
        container = PostgresTestContainer()
        self.containers.append(container)
        return container

    def stop_all_containers(self):
        """stop all containers"""
        for container in self.containers:
            container.stop()
