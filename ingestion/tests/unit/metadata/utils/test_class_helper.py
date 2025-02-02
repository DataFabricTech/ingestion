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

import pytest

from metadata.generated.schema.entity.services.dashboardService import DashboardService
from metadata.generated.schema.entity.services.databaseService import DatabaseService
from metadata.generated.schema.entity.services.messagingService import MessagingService
from metadata.generated.schema.entity.services.metadataService import MetadataService
from metadata.generated.schema.entity.services.mlmodelService import MlModelService
from metadata.generated.schema.entity.services.pipelineService import PipelineService
from metadata.generated.schema.entity.services.serviceType import ServiceType
from metadata.utils.class_helper import (
    get_service_class_from_service_type,
    get_service_type_from_source_type,
)


@pytest.mark.parametrize(
    ("source_type", "expected_service_type"),
    [
        ("looker", ServiceType.Dashboard),
        ("mysql", ServiceType.Database),
        ("kafka", ServiceType.Messaging),
        ("amundsen", ServiceType.Metadata),
        ("mlflow", ServiceType.MlModel),
        ("airflow", ServiceType.Pipeline),
        ("clickhouse_usage", ServiceType.Database),
        ("redshift-usage", ServiceType.Database),
        ("metadata_elasticsearch", ServiceType.Metadata),
    ],
)
def test_get_service_type_from_source_type(
    source_type: str, expected_service_type: ServiceType
):
    actual_service_type = get_service_type_from_source_type(source_type)
    assert actual_service_type == expected_service_type


@pytest.mark.parametrize(
    ("service_type", "expected_service_class"),
    [
        (ServiceType.Dashboard, DashboardService),
        (ServiceType.Database, DatabaseService),
        (ServiceType.Messaging, MessagingService),
        (ServiceType.Metadata, MetadataService),
        (ServiceType.MlModel, MlModelService),
        (ServiceType.Pipeline, PipelineService),
    ],
)
def test_get_service_class_from_service_type(
    service_type: ServiceType, expected_service_class: object
):
    actual_service_class = get_service_class_from_service_type(service_type)
    assert actual_service_class == expected_service_class
