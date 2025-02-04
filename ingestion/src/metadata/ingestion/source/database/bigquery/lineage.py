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
Handle big query lineage extraction
"""
from metadata.ingestion.source.database.bigquery.queries import BIGQUERY_STATEMENT
from metadata.ingestion.source.database.bigquery.query_parser import (
    BigqueryQueryParserSource,
)
from metadata.ingestion.source.database.lineage_source import LineageSource


class BigqueryLineageSource(BigqueryQueryParserSource, LineageSource):
    """
    Implements the necessary methods to extract
    Database lineage from Bigquery Source
    """

    sql_stmt = BIGQUERY_STATEMENT

    filters = """
        AND (
            statement_type IN ("MERGE", "CREATE_TABLE_AS_SELECT", "UPDATE") 
            OR (statement_type = "INSERT" and UPPER(query) like '%%INSERT%%INTO%%SELECT%%')
        )
    """
