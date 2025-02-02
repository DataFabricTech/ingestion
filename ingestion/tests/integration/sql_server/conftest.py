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
import shutil

import pytest
from testcontainers.mssql import SqlServerContainer


@pytest.fixture(scope="session")
def sql_server_container(tmp_path_factory):
    msqql_container = SqlServerContainer("mcr.microsoft.com/mssql/server:2017-latest")
    data_dir = tmp_path_factory.mktemp("data")
    shutil.copy(
        os.path.join(os.path.dirname(__file__), "data", "AdventureWorks2017.bak"),
        str(data_dir),
    )
    with open(data_dir / "install.sql", "w") as f:
        f.write(
            """
USE [master]
RESTORE DATABASE [AdventureWorks]
    FROM DISK = '/data/AdventureWorks2017.bak'
        WITH MOVE 'AdventureWorks2017' TO '/var/opt/mssql/data/AdventureWorks.mdf',
        MOVE 'AdventureWorks2017_log' TO '/var/opt/mssql/data/AdventureWorks_log.ldf'
GO
        """
        )

    msqql_container.volumes = {str(data_dir): {"bind": "/data"}}
    with msqql_container as container:
        docker_container = container.get_wrapped_container()
        res = docker_container.exec_run(
            [
                "/opt/mssql-tools/bin/sqlcmd",
                "-S",
                "localhost",
                "-U",
                container.username,
                "-P",
                container.password,
                "-d",
                "master",
                "-i",
                "/data/install.sql",
            ]
        )
        if res[0] != 0:
            raise Exception("Failed to create mssql database:" + res[1].decode("utf-8"))
        yield container
