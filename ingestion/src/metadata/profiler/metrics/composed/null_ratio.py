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
Null Ratio Composed Metric definition
"""
# pylint: disable=duplicate-code

from typing import Any, Dict, Optional, Tuple

from metadata.generated.schema.configuration.profilerConfiguration import MetricType
from metadata.profiler.metrics.core import ComposedMetric
from metadata.profiler.metrics.static.count import Count
from metadata.profiler.metrics.static.null_count import NullCount


class NullRatio(ComposedMetric):
    """
    Given the total count and null count,
    compute the null ratio
    """

    @classmethod
    def name(cls):
        return MetricType.nullProportion.value

    @classmethod
    def required_metrics(cls) -> Tuple[str, ...]:
        return Count.name(), NullCount.name()

    @property
    def metric_type(self):
        """
        Override default metric_type definition as
        we now don't care about the column
        """
        return float

    def fn(self, res: Dict[str, Any]) -> Optional[float]:
        """
        Safely compute null ratio based on the profiler
        results of other Metrics
        """
        import pandas as pd

        res_count = res.get(Count.name())
        res_null = res.get(NullCount.name())

        if not pd.isnull(res_count) and not pd.isnull(res_null):
            result = res_null / (res_null + res_count)
            return None if pd.isnull(result) else result
        return None
