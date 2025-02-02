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
GraphQL queries used during ingestion
"""

TABLEAU_DATASOURCES_QUERY = """
{{
workbooks(filter:{{luid: "{workbook_id}"}}){{
  id
  luid
  name
  embeddedDatasourcesConnection(first: {first}, offset: {offset} ) {{
    nodes {{
      id
      name
      fields {{
        id
        name
        upstreamColumns{{
          id
          name
          remoteType
        }}
        description
      }}
      upstreamTables {{
        id
        luid
        name
        fullName
        schema
        referencedByQueries {{
          id
          name
          query
        }}
        columns {{
          id
          name
        }}
        database {{
          id
          name
        }}
      }}
    }}
    totalCount
  }}
  }}
}}
"""
