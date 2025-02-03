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
import tarfile
import zipfile
from subprocess import CalledProcessError

import pytest
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def postgres_container(tmp_path_factory):
    data_dir = tmp_path_factory.mktemp("data")
    dvd_rental_zip = os.path.join(os.path.dirname(__file__), "data", "dvdrental.zip")
    zipfile.ZipFile(dvd_rental_zip, "r").extractall(str(data_dir))
    with tarfile.open(data_dir / "dvdrental_data.tar", "w") as tar:
        tar.add(data_dir / "dvdrental.tar", arcname="dvdrental.tar")

    container = PostgresContainer("postgres:15", dbname="dvdrental")
    container._command = [
        "-c",
        "shared_preload_libraries=pg_stat_statements",
        "-c",
        "pg_stat_statements.max=10000",
        "-c",
        "pg_stat_statements.track=all",
    ]

    with container as container:
        docker_container = container.get_wrapped_container()
        docker_container.exec_run(["mkdir", "/data"])
        docker_container.put_archive(
            "/data/", open(data_dir / "dvdrental_data.tar", "rb")
        )
        for query in (
            "CREATE USER postgres SUPERUSER;",
            "CREATE EXTENSION pg_stat_statements;",
        ):
            res = docker_container.exec_run(
                ["psql", "-U", container.username, "-d", container.dbname, "-c", query]
            )
            if res[0] != 0:
                raise CalledProcessError(
                    returncode=res[0], cmd=res, output=res[1].decode("utf-8")
                )
        res = docker_container.exec_run(
            [
                "pg_restore",
                "-U",
                container.username,
                "-d",
                container.dbname,
                "/data/dvdrental.tar",
            ]
        )
        if res[0] != 0:
            raise CalledProcessError(
                returncode=res[0], cmd=res, output=res[1].decode("utf-8")
            )
        yield container
