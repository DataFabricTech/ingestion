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
import logging

from metadata.generated.schema.entity.data.container import Container
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.workflow.profiler import ProfilerWorkflow
from tests.integration.integration_base import (
    PROFILER_INGESTION_CONFIG_TEMPLATE,
    int_admin_ometa, TEST2,
)

logger = logging.getLogger(__name__)


def get_source_config():
    return json.dumps(
        {
            "config": {"type": "StorageMetadata"},
        }
    )


def get_service_config():
    return json.dumps(
        {
            "type": "MinIO",
            "minioConfig": {"accessKeyId": "fabric", "secretKey": "fabric12##",
                            "endPointURL": "http://192.168.106.12:9000"},
            "bucketNames": ["fabric"],
        }
    )


service_type = "minio"
service_name = "test"
openmetadata_url = "http://192.168.105.51:8585/api"


class MinioProfiler:
    def __init__(self):
        self.metadata: OpenMetadata = int_admin_ometa(url=openmetadata_url)

    def tear_down(self):
        # storage_service = self.metadata.get_by_name(StorageService, self.service_name)
        # for entity in storage_service:
        #     self.metadata.delete(StorageService, entity.id, True, True)
        logger.info("tear_down")
        self.clean_up()

    @staticmethod
    def clean_up():
        """Reset profiler settings"""
        # profiler_configuration = ProfilerConfiguration(metricConfiguration=[])
        #
        # settings = Settings(
        #     config_type=SettingType.profilerConfiguration,
        #     config_value=profiler_configuration,
        # )
        # cls.metadata.create_or_update_settings(settings)
        logger.info("clean_up")

    def test_profiler_workflow(self):
        """test a simple profiler workflow on a table in each service and validate the profile is created"""
        try:
            # config = PROFILER_INGESTION_CONFIG_TEMPLATE.format(
            #     type=service_type,
            #     service_name=service_name,
            #     service_config=get_service_config(),
            #     hostport=openmetadata_url,
            #     Profiler="StorageProfiler",
            # )

            profiler_workflow = ProfilerWorkflow.create(
                json.loads(TEST2),
            )
            profiler_workflow.execute()
            profiler_workflow.raise_from_status()
            profiler_workflow.stop()
        except Exception as e:
            logger.exception(e)

        containers = self.metadata.list_all_entities(entity=Container, limit=1000)
        for container in containers:
            logger.debug(f"container: {container}")
        #     self.metadata.get_latest_table_profile()
        # #     self.assertIsNotNone(table.profile)
        #     for column in columns:
        #         self.assertIsNotNone(column.profile)
        #
        # tables: List[Table] = self.metadata.list_all_entities(Table)
        # for table in tables:
        #     if table.name.__root__ != "users":
        #         continue
        #     table = self.metadata.get_latest_table_profile(table.fullyQualifiedName)
        #     columns = table.columns
        #     self.assertIsNotNone(table.profile)
        #     for column in columns:
        #         if column.dataType == DataType.INT:
        #             self.assertIsNone(column.profile.mean)
        #             self.assertIsNotNone(column.profile.valuesCount)
        #             self.assertIsNotNone(column.profile.distinctCount)
        #         if column.dataType == DataType.STRING:
        #             self.assertIsNone(column.profile.mean)
        #             self.assertIsNone(column.profile.valuesCount)
        #             self.assertIsNone(column.profile.distinctCount)


if __name__ == "__main__":
    profiler = MinioProfiler()

    profiler.test_profiler_workflow()

    profiler.tear_down()
