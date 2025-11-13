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
Define Median function
"""
# Keep SQA docs style defining custom constructs
# pylint: disable=duplicate-code
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import func
from sqlalchemy.sql.functions import FunctionElement

from metadata.profiler.metrics.core import CACHE
from metadata.profiler.orm.registry import Dialects
from metadata.utils.logger import profiler_logger

logger = profiler_logger()


# --------------
# Date Functions
# --------------
class DateAddFn(FunctionElement):
    inherit_cache = CACHE


@compiles(DateAddFn)
def _(elements, compiler, **kwargs):
    """generic date and datetime function"""
    interval = elements.clauses.clauses[0].value
    interval_unit = compiler.process(elements.clauses.clauses[1], **kwargs)
    return f"CAST(CURRENT_DATE - interval '{interval}' {interval_unit}  AS DATE)"


@compiles(DateAddFn, Dialects.Oracle)
def _(elements, compiler, **kwargs):
    """generic date and datetime function"""
    interval = elements.clauses.clauses[0].value
    interval_unit = compiler.process(elements.clauses.clauses[1], **kwargs)
    return f"TO_DATE(CURRENT_DATE - INTERVAL '{interval}' {interval_unit})"


@compiles(DateAddFn, Dialects.MSSQL)
@compiles(DateAddFn, Dialects.Snowflake)
def _(elements, compiler, **kwargs):
    """date function for mssql and snowflake"""
    interval, interval_unit = [
        compiler.process(element, **kwargs) for element in elements.clauses
    ]
    return f"CAST(DATEADD({interval_unit},-{interval},GETDATE()) AS DATE)"


@compiles(DateAddFn, Dialects.IbmDbSa)
def _(elements, compiler, **kwargs):
    """Date function for IBM DB connections"""
    interval, interval_unit = [
        compiler.process(element, **kwargs) for element in elements.clauses
    ]
    return f"CAST({func.current_date()} - {interval} {interval_unit} AS DATE)"


@compiles(DateAddFn, Dialects.Redshift)
def _(elements, compiler, **kwargs):
    """Redshift datetime function"""
    interval, interval_unit = [
        compiler.process(element, **kwargs) for element in elements.clauses
    ]
    return f"DATEADD({interval_unit}, -{interval}, {func.current_date()})"


@compiles(DateAddFn, Dialects.SQLite)
def _(elements, compiler, **kwargs):
    """SQLite timestamp and datetime function"""
    interval = elements.clauses.clauses[0].value
    interval_unit = elements.clauses.clauses[1].text
    return f"DATE({func.current_date()}, '-{interval} {interval_unit}')"


# ------------------
# Datetime Functions
# ------------------
class DatetimeAddFn(FunctionElement):
    inherit_cache = CACHE


@compiles(DatetimeAddFn)
def _(elements, compiler, **kwargs):
    """generic date and datetime function"""
    return generic_function(elements, compiler, **kwargs)


@compiles(DatetimeAddFn, Dialects.MySQL)
def _(elements, compiler, **kwargs):
    """MySQL date and datetime function"""
    return mysql_function(elements, compiler, **kwargs)


@compiles(DatetimeAddFn, Dialects.IbmDbSa)
def _(elements, compiler, **kwargs):
    """Ibm DB datetime function reuses the generic implementation"""
    return generic_function(elements, compiler, **kwargs)


@compiles(DatetimeAddFn, Dialects.MSSQL)
@compiles(DatetimeAddFn, Dialects.Snowflake)
def _(elements, compiler, **kwargs):
    """MSSQL, Snowflake datetime function"""
    return mssql_snflk_function(elements, compiler, **kwargs)


@compiles(DatetimeAddFn, Dialects.Redshift)
def _(elements, compiler, **kwargs):
    """Redshift datetime function"""
    return redshift_function(elements, compiler, **kwargs)


@compiles(DatetimeAddFn, Dialects.SQLite)
def _(elements, compiler, **kwargs):
    """SQLite datetime function"""
    return sqlite_function(elements, compiler, **kwargs)


# -------------------
# Timestamp Functions
# -------------------
class TimestampAddFn(FunctionElement):
    inherit_cache = CACHE


@compiles(TimestampAddFn)
def _(elements, compiler, **kwargs):
    """Generic timestamp function"""
    return generic_function(elements, compiler, **kwargs)


@compiles(TimestampAddFn, Dialects.MySQL)
def _(elements, compiler, **kwargs):
    """MySQL timestamp function"""
    return mysql_function(elements, compiler, **kwargs)


@compiles(TimestampAddFn, Dialects.IbmDbSa)
def _(elements, compiler, **kwargs):
    """Ibm DB timestamp function reuses the generic implementation"""
    return generic_function(elements, compiler, **kwargs)


@compiles(TimestampAddFn, Dialects.MSSQL)
@compiles(TimestampAddFn, Dialects.Snowflake)
def _(elements, compiler, **kwargs):
    """MSSQL and Snowflake timestamp function"""
    return mssql_snflk_function(elements, compiler, **kwargs)


@compiles(TimestampAddFn, Dialects.Redshift)
def _(elements, compiler, **kwargs):
    """Redshift timestamp function"""
    return redshift_function(elements, compiler, **kwargs)


@compiles(TimestampAddFn, Dialects.SQLite)
def _(elements, compiler, **kwargs):
    """SQLite timestamp function"""
    return sqlite_function(elements, compiler, **kwargs)


# -----------------------------------
# Shared timestamp/datetime Functions
# -----------------------------------
def generic_function(elements, compiler, **kwargs):
    """generic date and datetime function"""
    interval = elements.clauses.clauses[0].value
    interval_unit = compiler.process(elements.clauses.clauses[1], **kwargs)
    return (
        f"CAST(CURRENT_TIMESTAMP - interval '{interval}' {interval_unit} AS TIMESTAMP)"
    )


def mysql_function(elements, compiler, **kwargs):
    """MySQL timestamp and datetime function"""
    interval = elements.clauses.clauses[0].value
    interval_unit = compiler.process(elements.clauses.clauses[1], **kwargs)
    return (
        f"CAST(CURRENT_TIMESTAMP - interval '{interval}' {interval_unit} AS DATETIME)"
    )


def sqlite_function(elements, compiler, **kwargs):  # pylint: disable=unused-argument
    """SQLite timestamp and datetime function"""
    interval = elements.clauses.clauses[0].value
    interval_unit = elements.clauses.clauses[1].text
    return f"DATE({func.current_timestamp()}, '-{interval} {interval_unit}')"


def redshift_function(elements, compiler, **kwargs):
    """Redshift timestamp and datetime function"""
    interval, interval_unit = [
        compiler.process(element, **kwargs) for element in elements.clauses
    ]
    return (
        f"DATEADD({interval_unit}, -{interval}, {func.current_timestamp()}::timestamp)"
    )


def mssql_snflk_function(elements, compiler, **kwargs):
    """MSSQL and Snowflake timestamp and datetime function"""
    interval, interval_unit = [
        compiler.process(element, **kwargs) for element in elements.clauses
    ]
    return f"DATEADD({interval_unit}, -{interval}, {func.current_timestamp()})"
