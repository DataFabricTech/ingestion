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
Snowflake usage module
"""
from metadata.ingestion.source.database.snowflake.queries import SNOWFLAKE_SQL_STATEMENT
from metadata.ingestion.source.database.snowflake.query_parser import (
    SnowflakeQueryParserSource,
)
from metadata.ingestion.source.database.usage_source import UsageSource


class SnowflakeUsageSource(SnowflakeQueryParserSource, UsageSource):
    """
    Snowflake class for Usage
    """

    sql_stmt = SNOWFLAKE_SQL_STATEMENT

    filters = """
        AND QUERY_TYPE NOT IN ('ROLLBACK','CREATE_USER','CREATE_ROLE','CREATE_NETWORK_POLICY',
        'ALTER_ROLE','ALTER_NETWORK_POLICY','ALTER_ACCOUNT','DROP_SEQUENCE','DROP_USER',
        'DROP_ROLE','DROP_NETWORK_POLICY','REVOKE','UNLOAD','USE','ALTER_SESSION',
        'COPY','COMMIT','CREATE_TABLE','PUT_FILES','GET_FILES', 'CREATE_TABLE_AS_SELECT','SHOW', 'DESCRIBE')
    """

    life_cycle_filters = [
        "DROP",
        "DELETE",
        "TRUNCATE_TABLE",
        "UPDATE",
        "ALTER",
        "INSERT",
        "MERGE",
    ]
