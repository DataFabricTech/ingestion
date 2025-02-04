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
Hive Metastore Dialect Mixin
"""
from sqlalchemy.engine import reflection

from metadata.ingestion.source.database.hive.utils import get_columns
from metadata.utils.sqlalchemy_utils import (
    get_all_table_comments,
    get_all_view_definitions,
)


# pylint: disable=unused-argument
class HiveMetaStoreDialectMixin:
    """
    Mixin class
    """

    def get_columns(self, connection, table_name, schema=None, **kw):
        return get_columns(self, connection, table_name, schema, **kw)

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # Hive has no support for foreign keys.
        return []

    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        # Hive has no support for unique keys.
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        # Hive has no support for primary keys.
        return []

    @reflection.cache
    def get_all_view_definitions(self, connection, query):
        get_all_view_definitions(self, connection, query)

    @reflection.cache
    def get_all_table_comments(self, connection, query):
        get_all_table_comments(self, connection, query)
