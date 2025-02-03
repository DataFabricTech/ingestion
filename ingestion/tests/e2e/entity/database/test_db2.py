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

"""Test Db2 database ingestion."""

import pytest

from ...configs.connectors.database.db2 import Db2Connector
from ...configs.connectors.model import (
    ConnectorIngestionTestConfig,
    ConnectorTestConfig,
    ConnectorValidationTestConfig,
    IngestionFilterConfig,
    IngestionTestConfig,
    ValidationTestConfig,
)


@pytest.mark.parametrize(
    "setUpClass",
    [
        {
            "connector_obj": Db2Connector(
                ConnectorTestConfig(
                    ingestion=ConnectorIngestionTestConfig(
                        metadata=IngestionTestConfig(
                            database=IngestionFilterConfig(includes=["testdb"]),
                        ),  # type: ignore
                    ),
                    validation=ConnectorValidationTestConfig(
                        profiler=ValidationTestConfig(
                            database="testdb", schema_="sampledata", table="customer"
                        )  # type: ignore
                    ),
                )
            )
        }
    ],
    indirect=True,
)
@pytest.mark.usefixtures("setUpClass")
class TestDb2Connector:
    """We need to validate dependency can be installed in the test env."""
