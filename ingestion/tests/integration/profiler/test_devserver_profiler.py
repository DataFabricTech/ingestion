#  Copyright 2024 Collate
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""
Test the SQL profiler using a Postgres and a MySQL container.
We load a simple user table in each service and run the profiler on it.
To run this we need OpenMetadata server up and running.
No sample data is required beforehand
"""

import json
from typing import List

from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.entity.services.databaseService import DatabaseService
from metadata.workflow.metadata import MetadataWorkflow
from metadata.workflow.profiler import ProfilerWorkflow
from tests.integration.integration_base import (
    METADATA_INGESTION_CONFIG_TEMPLATE,
    PROFILER_INGESTION_CONFIG_TEMPLATE,
    int_admin_ometa,
)


class TestDevServerProfiler:
    service_type = "postgres"
    service_name = "dev-postgres-test"
    openmetadata_url = "http://192.168.105.51:8585/api"

    # init
    def __init__(self):
        self.setup()

    def setup(self):
        self.metadata = int_admin_ometa(url=self.openmetadata_url)
        try:
            config = METADATA_INGESTION_CONFIG_TEMPLATE.format(
                type=self.service_type,
                service_name=self.service_name,
                service_config=json.dumps(
                    {
                        "authType": {"password": "fabric12#$"},
                        "hostPort": f"192.168.106.12:5432",
                        "username": "postgres",
                        "type": "Postgres",
                        "database": "datafabric",
                    }
                ),
                source_config=json.dumps(
                    {
                        "schemaFilterPattern": {
                            "includes": ["public"],
                        }
                    },
                ),
                hostport=self.openmetadata_url,
            )
            ingestion_workflow = MetadataWorkflow.create(
                json.loads(config),
            )
            ingestion_workflow.execute()
            ingestion_workflow.raise_from_status()
            ingestion_workflow.stop()
        except Exception as e:
            raise e

    def tear_down(self):
        db_entities = self.metadata.get_by_name(DatabaseService, self.service_name)
        for db_entity in db_entities:
            self.metadata.delete(DatabaseService, db_entity.id, True, True)
        self._clean_up_settings()

    def _clean_up_settings(self):
        """Reset profiler settings"""
        # profiler_configuration = ProfilerConfiguration(metricConfiguration=[])
        #
        # settings = Settings(
        #     config_type=SettingType.profilerConfiguration,
        #     config_value=profiler_configuration,
        # )
        # cls.metadata.create_or_update_settings(settings)

    def test_profiler_workflow(self):
        """test a simple profiler workflow on a table in each service and validate the profile is created"""
        try:
            config = PROFILER_INGESTION_CONFIG_TEMPLATE.format(
                type=self.service_type,
                service_config=json.dumps(
                    {
                        "authType": {"password": "fabric12#$"},
                        "hostPort": f"192.168.106.12:5432",
                        "username": "postgres",
                        "type": "Postgres",
                        "database": "datafabric",
                    }
                ),
                Profiler="Profiler",
                service_name="dev-postgres-test",
                hostport=self.openmetadata_url,
            )
            profiler_workflow = ProfilerWorkflow.create(
                json.loads(config),
            )
            profiler_workflow.execute()
            profiler_workflow.raise_from_status()
            profiler_workflow.stop()
        except Exception as e:
            self.fail(
                f"Profiler workflow failed for dev-postgres-test with error {e}"
            )

        tables: List[Table] = self.metadata.list_all_entities(Table)
        for table in tables:
            if table.name.__root__ != "users":
                continue
            table = self.metadata.get_latest_table_profile(table.fullyQualifiedName)
            columns = table.columns
            self.assertIsNotNone(table.profile)
            for column in columns:
                self.assertIsNotNone(column.profile)


if __name__ == "__main__":
    profiler = TestDevServerProfiler()

    profiler.test_profiler_workflow()

    profiler.tear_down()
