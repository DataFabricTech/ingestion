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
Test Datalake Profiler workflow

To run this we need Metadata server up and running.

No sample data is required beforehand
"""

import os
from copy import deepcopy
from pathlib import Path
from unittest import TestCase

import boto3
import botocore
from moto import mock_s3

from metadata.generated.schema.entity.data.table import ColumnProfile, Table
from metadata.generated.schema.entity.services.connections.metadata.metadataConnection import (
    MetadataConnection,
)
from metadata.generated.schema.entity.services.databaseService import DatabaseService
from metadata.generated.schema.security.client.metadataJWTClientConfig import (
    MetadataJWTClientConfig,
)
from metadata.ingestion.server.server_api import ServerInterface
from metadata.utils.time_utils import (
    get_beginning_of_day_timestamp_mill,
    get_end_of_day_timestamp_mill,
)
from metadata.workflow.metadata import MetadataWorkflow
from metadata.workflow.profiler import ProfilerWorkflow
from metadata.workflow.workflow_output_handler import print_status

SERVICE_NAME = Path(__file__).stem
REGION = "us-west-1"
BUCKET_NAME = "MyBucket"
INGESTION_CONFIG = {
    "source": {
        "type": "datalake",
        "serviceName": SERVICE_NAME,
        "serviceConnection": {
            "config": {
                "type": "Datalake",
                "configSource": {
                    "securityConfig": {
                        "awsAccessKeyId": "fake_access_key",
                        "awsSecretAccessKey": "fake_secret_key",
                        "awsRegion": REGION,
                    }
                },
                "bucketName": f"{BUCKET_NAME}",
            }
        },
        "sourceConfig": {"config": {"type": "DatabaseMetadata"}},
    },
    "sink": {"type": "metadata-rest", "config": {}},
    "workflowConfig": {
        "serverConfig": {
            "hostPort": "http://localhost:8585/api",
            "authProvider": "metadata",
            "securityConfig": {
                "jwtToken": "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhZG1pbiIsImlzQm90IjpmYWxzZSwiaXNzIjoib3Blbi1tZXRhZGF0YS5vcmciLCJpYXQiOjE2NjM5Mzg0NjIsImVtYWlsIjoiYWRtaW5Ab3Blbm1ldGFkYXRhLm9yZyJ9.tS8um_5DKu7HgzGBzS1VTA5uUjKWOCU0B_j08WXBiEC0mr0zNREkqVfwFDD-d24HlNEbrqioLsBuFRiwIWKc1m_ZlVQbG7P36RUxhuv2vbSp80FKyNM-Tj93FDzq91jsyNmsQhyNv_fNr3TXfzzSPjHt8Go0FMMP66weoKMgW2PbXlhVKwEuXUHyakLLzewm9UMeQaEiRzhiTMU3UkLXcKbYEJJvfNFcLwSl9W8JCO_l0Yj3ud-qt_nQYEZwqW6u5nfdQllN133iikV4fM5QZsMCnm8Rq1mvLR0y9bmJiD7fwM1tmJ791TUWqmKaTnP49U493VanKpUAfzIiOiIbhg"
            },
        }
    },
}


@mock_s3
class DatalakeProfilerTestE2E(TestCase):
    """datalake profiler E2E test"""

    @classmethod
    def setUpClass(cls) -> None:
        server_config = MetadataConnection(
            hostPort="http://localhost:8585/api",
            authProvider="metadata",
            securityConfig=MetadataJWTClientConfig(
                jwtToken="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhZG1pbiIsImlzQm90IjpmYWxzZSwiaXNzIjoib3Blbi1tZXRhZGF0YS5vcmciLCJpYXQiOjE2NjM5Mzg0NjIsImVtYWlsIjoiYWRtaW5Ab3Blbm1ldGFkYXRhLm9yZyJ9.tS8um_5DKu7HgzGBzS1VTA5uUjKWOCU0B_j08WXBiEC0mr0zNREkqVfwFDD-d24HlNEbrqioLsBuFRiwIWKc1m_ZlVQbG7P36RUxhuv2vbSp80FKyNM-Tj93FDzq91jsyNmsQhyNv_fNr3TXfzzSPjHt8Go0FMMP66weoKMgW2PbXlhVKwEuXUHyakLLzewm9UMeQaEiRzhiTMU3UkLXcKbYEJJvfNFcLwSl9W8JCO_l0Yj3ud-qt_nQYEZwqW6u5nfdQllN133iikV4fM5QZsMCnm8Rq1mvLR0y9bmJiD7fwM1tmJ791TUWqmKaTnP49U493VanKpUAfzIiOiIbhg"
            ),
        )  # type: ignore
        cls.metadata = ServerInterface(server_config)

    def setUp(self) -> None:
        # Mock our S3 bucket and ingest a file
        boto3.DEFAULT_SESSION = None
        self.client = boto3.client(
            "s3",
            region_name=REGION,
        )

        # check that we are not running our test against a real bucket
        try:
            s3 = boto3.resource(
                "s3",
                region_name=REGION,
                aws_access_key_id="fake_access_key",
                aws_secret_access_key="fake_secret_key",
            )
            s3.meta.client.head_bucket(Bucket=BUCKET_NAME)
        except botocore.exceptions.ClientError:
            pass
        else:
            err = f"{BUCKET_NAME} should not exist."
            raise EnvironmentError(err)
        self.client.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": REGION},
        )
        current_dir = os.path.dirname(__file__)
        resources_dir = os.path.join(current_dir, "resources")

        resources_paths = [
            os.path.join(path, filename)
            for path, _, files in os.walk(resources_dir)
            for filename in files
        ]

        self.s3_keys = []

        for path in resources_paths:
            key = os.path.relpath(path, resources_dir)
            self.s3_keys.append(key)
            self.client.upload_file(Filename=path, Bucket=BUCKET_NAME, Key=key)

        # Ingest our S3 data
        ingestion_workflow = MetadataWorkflow.create(INGESTION_CONFIG)
        ingestion_workflow.execute()
        ingestion_workflow.raise_from_status()
        print_status(ingestion_workflow)
        ingestion_workflow.stop()

    def test_datalake_profiler_workflow(self):
        workflow_config = deepcopy(INGESTION_CONFIG)
        workflow_config["source"]["sourceConfig"]["config"].update(
            {
                "type": "Profiler",
            }
        )
        workflow_config["processor"] = {
            "type": "orm-profiler",
            "config": {},
        }

        profiler_workflow = ProfilerWorkflow.create(workflow_config)
        profiler_workflow.execute()
        status = profiler_workflow.result_status()
        profiler_workflow.stop()

        assert status == 0

        table_profile = self.metadata.get_profile_data(
            f'{SERVICE_NAME}.default.MyBucket."profiler_test_.csv"',
            get_beginning_of_day_timestamp_mill(),
            get_end_of_day_timestamp_mill(),
        )

        column_profile = self.metadata.get_profile_data(
            f'{SERVICE_NAME}.default.MyBucket."profiler_test_.csv".first_name',
            get_beginning_of_day_timestamp_mill(),
            get_end_of_day_timestamp_mill(),
            profile_type=ColumnProfile,
        )

        assert table_profile.entities
        assert column_profile.entities

    def test_values_partitioned_datalake_profiler_workflow(self):
        """Test partitioned datalake profiler workflow"""
        workflow_config = deepcopy(INGESTION_CONFIG)
        workflow_config["source"]["sourceConfig"]["config"].update(
            {
                "type": "Profiler",
            }
        )
        workflow_config["processor"] = {
            "type": "orm-profiler",
            "config": {
                "tableConfig": [
                    {
                        "fullyQualifiedName": f'{SERVICE_NAME}.default.MyBucket."profiler_test_.csv"',
                        "partitionConfig": {
                            "enablePartitioning": "true",
                            "partitionColumnName": "first_name",
                            "partitionIntervalType": "COLUMN-VALUE",
                            "partitionValues": ["John"],
                        },
                    }
                ]
            },
        }

        profiler_workflow = ProfilerWorkflow.create(workflow_config)
        profiler_workflow.execute()
        status = profiler_workflow.result_status()
        profiler_workflow.stop()

        assert status == 0

        table = self.metadata.get_by_name(
            entity=Table,
            fqn=f'{SERVICE_NAME}.default.MyBucket."profiler_test_.csv"',
            fields=["tableProfilerConfig"],
        )

        profile = self.metadata.get_latest_table_profile(
            table.fullyQualifiedName
        ).profile

        assert profile.rowCount == 1.0

    def test_datetime_partitioned_datalake_profiler_workflow(self):
        """Test partitioned datalake profiler workflow"""
        workflow_config = deepcopy(INGESTION_CONFIG)
        workflow_config["source"]["sourceConfig"]["config"].update(
            {
                "type": "Profiler",
            }
        )
        workflow_config["processor"] = {
            "type": "orm-profiler",
            "config": {
                "tableConfig": [
                    {
                        "fullyQualifiedName": f'{SERVICE_NAME}.default.MyBucket."profiler_test_.csv"',
                        "partitionConfig": {
                            "enablePartitioning": "true",
                            "partitionColumnName": "birthdate",
                            "partitionIntervalType": "TIME-UNIT",
                            "partitionIntervalUnit": "YEAR",
                            "partitionInterval": 35,
                        },
                    }
                ],
            },
        }

        profiler_workflow = ProfilerWorkflow.create(workflow_config)
        profiler_workflow.execute()
        status = profiler_workflow.result_status()
        profiler_workflow.stop()

        assert status == 0

        table = self.metadata.get_by_name(
            entity=Table,
            fqn=f'{SERVICE_NAME}.default.MyBucket."profiler_test_.csv"',
            fields=["tableProfilerConfig"],
        )

        profile = self.metadata.get_latest_table_profile(
            table.fullyQualifiedName
        ).profile

        assert profile.rowCount == 2.0

    def test_integer_range_partitioned_datalake_profiler_workflow(self):
        """Test partitioned datalake profiler workflow"""
        workflow_config = deepcopy(INGESTION_CONFIG)
        workflow_config["source"]["sourceConfig"]["config"].update(
            {
                "type": "Profiler",
            }
        )
        workflow_config["processor"] = {
            "type": "orm-profiler",
            "config": {
                "tableConfig": [
                    {
                        "fullyQualifiedName": f'{SERVICE_NAME}.default.MyBucket."profiler_test_.csv"',
                        "profileSample": 100,
                        "partitionConfig": {
                            "enablePartitioning": "true",
                            "partitionColumnName": "age",
                            "partitionIntervalType": "INTEGER-RANGE",
                            "partitionIntegerRangeStart": 35,
                            "partitionIntegerRangeEnd": 44,
                        },
                    }
                ],
            },
        }

        profiler_workflow = ProfilerWorkflow.create(workflow_config)
        profiler_workflow.execute()
        status = profiler_workflow.result_status()
        profiler_workflow.stop()

        assert status == 0

        table = self.metadata.get_by_name(
            entity=Table,
            fqn=f'{SERVICE_NAME}.default.MyBucket."profiler_test_.csv"',
            fields=["tableProfilerConfig"],
        )

        profile = self.metadata.get_latest_table_profile(
            table.fullyQualifiedName
        ).profile

        assert profile.rowCount == 2.0

    def test_datalake_profiler_workflow_with_custom_profiler_config(self):
        """Test custom profiler config return expected sample and metric computation"""
        profiler_metrics = [
            "MIN",
            "MAX",
            "MEAN",
            "MEDIAN",
        ]
        id_metrics = ["MIN", "MAX"]
        non_metric_values = ["name", "timestamp"]

        workflow_config = deepcopy(INGESTION_CONFIG)
        workflow_config["source"]["sourceConfig"]["config"].update(
            {
                "type": "Profiler",
            }
        )
        workflow_config["processor"] = {
            "type": "orm-profiler",
            "config": {
                "profiler": {
                    "name": "ingestion_profiler",
                    "metrics": profiler_metrics,
                },
                "tableConfig": [
                    {
                        "fullyQualifiedName": f'{SERVICE_NAME}.default.MyBucket."profiler_test_.csv"',
                        "columnConfig": {
                            "includeColumns": [
                                {"columnName": "id", "metrics": id_metrics},
                                {"columnName": "age"},
                            ]
                        },
                    }
                ],
            },
        }

        profiler_workflow = ProfilerWorkflow.create(workflow_config)
        profiler_workflow.execute()
        status = profiler_workflow.result_status()
        profiler_workflow.stop()

        assert status == 0

        table = self.metadata.get_by_name(
            entity=Table,
            fqn=f'{SERVICE_NAME}.default.MyBucket."profiler_test_.csv"',
            fields=["tableProfilerConfig"],
        )

        id_profile = self.metadata.get_profile_data(
            f'{SERVICE_NAME}.default.MyBucket."profiler_test_.csv".id',
            get_beginning_of_day_timestamp_mill(),
            get_end_of_day_timestamp_mill(),
            profile_type=ColumnProfile,
        ).entities

        latest_id_profile = max(id_profile, key=lambda o: o.timestamp.__root__)

        id_metric_ln = 0
        for metric_name, metric in latest_id_profile:
            if metric_name.upper() in id_metrics:
                assert metric is not None
                id_metric_ln += 1
            else:
                assert metric is None if metric_name not in non_metric_values else True

        assert id_metric_ln == len(id_metrics)

        age_profile = self.metadata.get_profile_data(
            f'{SERVICE_NAME}.default.MyBucket."profiler_test_.csv".age',
            get_beginning_of_day_timestamp_mill(),
            get_end_of_day_timestamp_mill(),
            profile_type=ColumnProfile,
        ).entities

        latest_age_profile = max(age_profile, key=lambda o: o.timestamp.__root__)

        age_metric_ln = 0
        for metric_name, metric in latest_age_profile:
            if metric_name.upper() in profiler_metrics:
                assert metric is not None
                age_metric_ln += 1
            else:
                assert metric is None if metric_name not in non_metric_values else True

        assert age_metric_ln == len(profiler_metrics)

        latest_exc_timestamp = latest_age_profile.timestamp.__root__
        first_name_profile = self.metadata.get_profile_data(
            f'{SERVICE_NAME}.default.MyBucket."profiler_test_.csv".first_name_profile',
            get_beginning_of_day_timestamp_mill(),
            get_end_of_day_timestamp_mill(),
            profile_type=ColumnProfile,
        ).entities

        assert not [
            p
            for p in first_name_profile
            if p.timestamp.__root__ == latest_exc_timestamp
        ]

        sample_data = self.metadata.get_sample_data(table)
        assert sorted([c.__root__ for c in sample_data.sampleData.columns]) == sorted(
            ["id", "age"]
        )

    def tearDown(self):
        s3 = boto3.resource(
            "s3",
            region_name=REGION,
        )
        bucket = s3.Bucket(BUCKET_NAME)
        for key in bucket.objects.all():
            key.delete()
        bucket.delete()

        service_id = str(
            self.metadata.get_by_name(
                entity=DatabaseService, fqn=SERVICE_NAME
            ).id.__root__
        )

        self.metadata.delete(
            entity=DatabaseService,
            entity_id=service_id,
            recursive=True,
            hard_delete=True,
        )
