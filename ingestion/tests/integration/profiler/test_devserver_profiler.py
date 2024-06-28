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
from unittest import TestCase

from metadata.generated.schema.configuration.profilerConfiguration import (
    MetricConfigurationDefinition,
    MetricType,
    ProfilerConfiguration,
)
from metadata.generated.schema.entity.data.table import DataType, Table
from metadata.generated.schema.entity.services.databaseService import DatabaseService
from metadata.generated.schema.settings.settings import Settings, SettingType
from metadata.workflow.metadata import MetadataWorkflow
from metadata.workflow.profiler import ProfilerWorkflow

from ...utils.docker_service_builders.test_container_builder import ContainerBuilder
from ..integration_base import (
    METADATA_INGESTION_CONFIG_TEMPLATE,
    PROFILER_INGESTION_CONFIG_TEMPLATE,
    int_admin_ometa,
)


class TestDevServerProfiler(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.metadata = int_admin_ometa(url="http://192.168.105.51:8585/api")
        try:
            config = METADATA_INGESTION_CONFIG_TEMPLATE.format(
                type="postgres",
                service_name="dev-postgres-test",
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
            )
            ingestion_workflow = MetadataWorkflow.create(
                json.loads(config),
            )
            ingestion_workflow.execute()
            ingestion_workflow.raise_from_status()
            ingestion_workflow.stop()
        except Exception as e:
            raise e

    @classmethod
    def tearDownClass(cls):
        db_entities = cls.metadata.get_by_name(DatabaseService, "dev-postgres-test")
        for db_entity in db_entities:
            cls.metadata.delete(DatabaseService, db_entity.id, True, True)
        cls._clean_up_settings()

    @classmethod
    def _clean_up_settings(cls):
        """Reset profiler settings"""
        profiler_configuration = ProfilerConfiguration(metricConfiguration=[])

        settings = Settings(
            config_type=SettingType.profilerConfiguration,
            config_value=profiler_configuration,
        )
        cls.metadata.create_or_update_settings(settings)

    def test_profiler_workflow(self):
        """test a simple profiler workflow on a table in each service and validate the profile is created"""
        try:
            config = PROFILER_INGESTION_CONFIG_TEMPLATE.format(
                type="postgres",
                service_config=json.dumps(
                    {
                        "authType": {"password": "fabric12#$"},
                        "hostPort": f"192.168.106.12:5432",
                        "username": "postgres",
                        "type": "Postgres",
                        "database": "datafabric",
                    }
                ),
                service_name="dev-postgres-test",
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

    def test_profiler_workflow_w_globale_config(self):
        """test a simple profiler workflow with a globale profiler configuration"""
        # add metric level settings
        profiler_configuration = ProfilerConfiguration(
            metricConfiguration=[
                MetricConfigurationDefinition(
                    dataType=DataType.INT,
                    disabled=False,
                    metrics=[MetricType.valuesCount, MetricType.distinctCount],
                ),
                MetricConfigurationDefinition(
                    dataType=DataType.VARCHAR, disabled=True, metrics=None
                ),
            ]
        )

        settings = Settings(
            config_type=SettingType.profilerConfiguration,
            config_value=profiler_configuration,
        )
        self.metadata.create_or_update_settings(settings)

        try:
            config = PROFILER_INGESTION_CONFIG_TEMPLATE.format(
                type="postgres",
                service_config=json.dumps(
                    {
                        "authType": {"password": "fabric12#$"},
                        "hostPort": f"192.168.106.12:5432",
                        "username": "postgres",
                        "type": "Postgres",
                        "database": "datafabric",
                    }
                ),
                service_name="dev-postgres-test",
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
                if column.dataType == DataType.INT:
                    self.assertIsNone(column.profile.mean)
                    self.assertIsNotNone(column.profile.valuesCount)
                    self.assertIsNotNone(column.profile.distinctCount)
                if column.dataType == DataType.STRING:
                    self.assertIsNone(column.profile.mean)
                    self.assertIsNone(column.profile.valuesCount)
                    self.assertIsNone(column.profile.distinctCount)
