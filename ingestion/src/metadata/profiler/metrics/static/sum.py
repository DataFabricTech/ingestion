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
SUM Metric definition
"""
# pylint: disable=duplicate-code

from sqlalchemy import column

from metadata.profiler.metrics.core import StaticMetric, _label
from metadata.profiler.orm.functions.length import LenFn
from metadata.profiler.orm.functions.sum import SumFn
from metadata.profiler.orm.registry import is_concatenable, is_quantifiable

from metadata.utils.logger import profiler_logger

logger = profiler_logger()


class Sum(StaticMetric):
    """
    SUM Metric

    Given a column, return the sum of its values.

    Only works for quantifiable types
    """

    @classmethod
    def name(cls):
        return "sum"

    @_label
    def fn(self):
        """sqlalchemy function"""
        if is_quantifiable(self.col.type):
            return SumFn(column(self.col.name, self.col.type))

        if is_concatenable(self.col.type):
            return SumFn(LenFn(column(self.col.name, self.col.type)))

        return None

    def df_fn(self, dfs=None):
        """pandas function"""

        if is_quantifiable(self.col.type):
            # return sum(df[self.col.name].sum() for df in dfs)
            return_value = 0
            try:
                for df in dfs:
                    if is_quantifiable(df[self.col.name].dtype):
                        return None
                    return_value += df[self.col.name].sum()
            except Exception as e:
                logger.error(f"sum value ERROR column name : {self.col.name}, error : {e}")
                return None

            return return_value
        return None
