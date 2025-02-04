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
Converter logic to transform an Metadata Table Entity for Bigquery
to an SQLAlchemy ORM class.
"""


from metadata.generated.schema.entity.data.database import databaseService
from metadata.generated.schema.entity.data.table import Column, DataType
from metadata.profiler.orm.converter.common import CommonMapTypes
from metadata.profiler.source.bigquery.type_mapper import bigquery_type_mapper


class BigqueryMapTypes(CommonMapTypes):
    def return_custom_type(self, col: Column, table_service_type):
        if (
            table_service_type == databaseService.DatabaseServiceType.BigQuery
            and col.dataType == DataType.STRUCT
        ):
            return bigquery_type_mapper(self._TYPE_MAP, col)
        return super().return_custom_type(col, table_service_type)

    @staticmethod
    def map_sqa_to_om_types() -> dict:
        """returns an ORM type"""
        # pylint: disable=import-outside-toplevel
        from sqlalchemy_bigquery import STRUCT

        return {
            **CommonMapTypes.map_sqa_to_om_types(),
            STRUCT: DataType.STRUCT,
        }
