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
Count Duplicates Composed Metric definition
"""
# pylint: disable=duplicate-code

from typing import Any, Dict, Optional, Tuple

from metadata.generated.schema.configuration.profilerConfiguration import MetricType
from metadata.profiler.metrics.core import ComposedMetric
from metadata.profiler.metrics.static.count import Count
from metadata.profiler.metrics.static.distinct_count import DistinctCount


class DuplicateCount(ComposedMetric):
    """
    Given the total count and the distinct count,
    compute the number of rows that are duplicates
    """

    @classmethod
    def name(cls):
        return MetricType.duplicateCount.value

    @classmethod
    def required_metrics(cls) -> Tuple[str, ...]:
        return Count.name(), DistinctCount.name()

    @property
    def metric_type(self):
        return int

    def fn(self, res: Dict[str, Any]) -> Optional[float]:
        """
        Safely compute duplicate count based on the profiler
        results of other Metrics
        """
        count = res.get(Count.name())
        unique_count = res.get(DistinctCount.name())

        if count is not None and unique_count is not None:
            return count - unique_count

        return None
