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
SQL Queries used during ingestion
"""

import textwrap

DORIS_GET_SCHEMA_COLUMN_INFO = textwrap.dedent(
    """
SELECT COLUMN_NAME,COLUMN_DEFAULT,IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,
NUMERIC_PRECISION,NUMERIC_SCALE,COLUMN_TYPE,COLUMN_KEY,COLUMN_COMMENT,ORDINAL_POSITION
from information_schema.`columns` t
where TABLE_SCHEMA = :schema
AND TABLE_NAME = :table_name
    """
)

DORIS_SHOW_FULL_COLUMNS = textwrap.dedent(
    """
SHOW FULL COLUMNS FROM {}.{}
    """
)

DORIS_GET_TABLE_NAMES = textwrap.dedent(
    """
    select TABLE_NAME as name, `ENGINE` as engine
    from INFORMATION_SCHEMA.tables 
    where TABLE_SCHEMA = :schema
    """
)

DORIS_TABLE_COMMENTS = textwrap.dedent(
    """
SELECT TABLE_COMMENT
FROM information_schema.tables
WHERE TABLE_SCHEMA = :schema
AND TABLE_NAME = :table_name
"""
)

DORIS_VIEW_DEFINITIONS = textwrap.dedent(
    """
select
	TABLE_NAME as view_name,
	TABLE_SCHEMA as schema,
	'' as view_def
from information_schema.tables where engine in ['MaterializedView', 'View']
"""
)

DORIS_PARTITION_DETAILS = textwrap.dedent(
    """
SHOW PARTITIONS FROM {}.{}
    """
)
