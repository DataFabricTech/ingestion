#  Copyright 2021 Collate
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
Default simple profiler to use
"""
from typing import List, Optional

from metadata.generated.schema.entity.services.connections.storage.minioConnection import MinioType
from metadata.generated.schema.entity.services.storageService import StorageService
from sqlalchemy.orm import DeclarativeMeta

from metadata.generated.schema.configuration.profilerConfiguration import (
    ProfilerConfiguration,
)
from metadata.generated.schema.entity.data.table import ColumnProfilerConfig
from metadata.generated.schema.entity.services.databaseService import DatabaseService
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.profiler.interface.profiler_interface import ProfilerInterface
from metadata.profiler.metrics.core import Metric, add_props
from metadata.profiler.metrics.registry import Metrics
from metadata.profiler.processor.core import Profiler


def get_default_metrics(
        table: DeclarativeMeta,
        ometa_client: Optional[OpenMetadata] = None,
        db_service: Optional[DatabaseService] = None,
) -> List[Metric]:
    return [
        # Table Metrics
        Metrics.ROW_COUNT.value,
        add_props(table=table)(Metrics.COLUMN_COUNT.value),
        add_props(table=table)(Metrics.COLUMN_NAMES.value),
        # We'll use the ometa_client & db_service in case we need to fetch info to ES
        add_props(table=table, ometa_client=ometa_client, db_service=db_service)(
            Metrics.SYSTEM.value
        ),
        # Column Metrics
        Metrics.MEDIAN.value,
        Metrics.FIRST_QUARTILE.value,
        Metrics.THIRD_QUARTILE.value,
        Metrics.MEAN.value,
        Metrics.COUNT.value,
        Metrics.DISTINCT_COUNT.value,
        Metrics.DISTINCT_RATIO.value,
        Metrics.MIN.value,
        Metrics.MAX.value,
        Metrics.NULL_COUNT.value,
        Metrics.NULL_RATIO.value,
        Metrics.STDDEV.value,
        Metrics.SUM.value,
        Metrics.UNIQUE_COUNT.value,
        Metrics.UNIQUE_RATIO.value,
        Metrics.IQR.value,
        Metrics.HISTOGRAM.value,
        Metrics.NON_PARAMETRIC_SKEW.value,
    ]


def get_default_metrics_for_storage_service(
        table: DeclarativeMeta,
        ometa_client: Optional[OpenMetadata] = None,
        storage_service: Optional[StorageService] = None,
) -> List[Metric]:
    return [
        # Table Data Metrics
        Metrics.ROW_COUNT.value,
        add_props(table=table)(Metrics.COLUMN_COUNT.value),
        add_props(table=table)(Metrics.COLUMN_NAMES.value),
        add_props(table=table, ometa_client=ometa_client, storage_service=storage_service)(
            Metrics.SYSTEM.value
        ),
        # Column Metrics
        Metrics.MEDIAN.value,
        Metrics.FIRST_QUARTILE.value,
        Metrics.THIRD_QUARTILE.value,
        Metrics.MEAN.value,
        Metrics.COUNT.value,
        Metrics.DISTINCT_COUNT.value,
        Metrics.DISTINCT_RATIO.value,
        Metrics.MIN.value,
        Metrics.MAX.value,
        Metrics.NULL_COUNT.value,
        Metrics.NULL_RATIO.value,
        Metrics.STDDEV.value,
        Metrics.SUM.value,
        Metrics.UNIQUE_COUNT.value,
        Metrics.UNIQUE_RATIO.value,
        Metrics.IQR.value,
        Metrics.HISTOGRAM.value,
        Metrics.NON_PARAMETRIC_SKEW.value,
    ]


class DefaultProfiler(Profiler):
    """
    Pre-built profiler with a simple
    set of metrics that we can use as
    a default.
    """

    def __init__(
            self,
            profiler_interface: ProfilerInterface,
            include_columns: Optional[List[ColumnProfilerConfig]] = None,
            exclude_columns: Optional[List[str]] = None,
            global_profiler_configuration: Optional[ProfilerConfiguration] = None,
    ):
        if profiler_interface.service_connection_config.type == MinioType.MinIO:
            _metrics = get_default_metrics_for_storage_service(
                table=profiler_interface.table,
                ometa_client=profiler_interface.ometa_client,
            )
        else:
            _metrics = get_default_metrics(
                table=profiler_interface.table, ometa_client=profiler_interface.ometa_client
            )

        super().__init__(
            *_metrics,
            profiler_interface=profiler_interface,
            include_columns=include_columns,
            exclude_columns=exclude_columns,
            global_profiler_configuration=global_profiler_configuration,
        )
