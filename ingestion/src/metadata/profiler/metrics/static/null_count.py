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
Null Count Metric definition
"""
# pylint: disable=duplicate-code


from sqlalchemy import case, column

from metadata.generated.schema.configuration.profilerConfiguration import MetricType
from metadata.profiler.metrics.core import StaticMetric, _label
from metadata.profiler.orm.functions.sum import SumFn


class NullCount(StaticMetric):
    """
    NULL COUNT Metric

    Given a column, return the null count.

    We are building a CASE WHEN structure:
    ```
    SUM(
        CASE is not null THEN 1
        ELSE 0
    )
    ```
    """

    @classmethod
    def name(cls):
        return MetricType.nullCount.value

    @property
    def metric_type(self):
        return int

    @_label
    def fn(self):
        """sqlalchemy function"""
        return SumFn(
            case([(column(self.col.name, self.col.type).is_(None), 1)], else_=0)
        )

    def df_fn(self, dfs=None):
        """pandas function"""
        return sum(df[self.col.name].isnull().sum() for df in dfs)
