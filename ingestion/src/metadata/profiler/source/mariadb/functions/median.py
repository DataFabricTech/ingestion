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

"""Median function for MariaDB"""

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import FunctionElement

from metadata.profiler.metrics.core import CACHE


class MariaDBMedianFn(FunctionElement):
    inherit_cache = CACHE


@compiles(MariaDBMedianFn)
def _(elements, compiler, **kwargs):  # pylint: disable=unused-argument
    col = compiler.process(elements.clauses.clauses[0])
    percentile = elements.clauses.clauses[2].value
    # According to the documentation available at https://mariadb.com/kb/en/median/#description,
    # the PERCENTILE_CONT function can be utilized to calculate the median. Therefore, it is
    # being used in this context.
    return f"PERCENTILE_CONT({percentile:.2f}) WITHIN GROUP (ORDER BY {col}) OVER()"
