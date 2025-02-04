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

from unittest import TestCase

from metadata.ingestion.source.database.column_type_parser import ColumnTypeParser

SQLTYPES = {
    "ARRAY",
    "BIGINT",
    "BIGNUMERIC",
    "BIGSERIAL",
    "BINARY",
    "BIT",
    "BLOB",
    "BOOL",
    "BOOLEAN",
    "BPCHAR",
    "BYTEINT",
    "BYTES",
    "CHAR",
    "CHARACTER VARYING",
    "CHARACTER",
    "CURSOR",
    "DATE",
    "DATETIME",
    "DATETIME2",
    "DATETIMEOFFSET",
    "DECIMAL",
    "DOUBLE PRECISION",
    "DOUBLE",
    "ENUM",
    "FLOAT",
    "FLOAT4",
    "FLOAT64",
    "FLOAT8",
    "GEOGRAPHY",
    "HYPERLOGLOG",
    "IMAGE",
    "INT",
    "INT2",
    "INT4",
    "INT64",
    "INT8",
    "INTEGER",
    "INTERVAL DAY TO SECOND",
    "INTERVAL YEAR TO MONTH",
    "INTERVAL",
    "JSON",
    "LONG RAW",
    "LONG VARCHAR",
    "LONG",
    "LONGBLOB",
    "MAP",
    "MEDIUMBLOB",
    "MEDIUMINT",
    "MEDIUMTEXT",
    "MONEY",
    "NCHAR",
    "NTEXT",
    "NUMBER",
    "NUMERIC",
    "NVARCHAR",
    "OBJECT",
    "RAW",
    "REAL",
    "ROWID",
    "ROWVERSION",
    "SET",
    "SMALLDATETIME",
    "SMALLINT",
    "SMALLMONEY",
    "SMALLSERIAL",
    "SQL_VARIANT",
    "STRING",
    "STRUCT",
    "TABLE",
    "TEXT",
    "TIME",
    "TIMESTAMP WITHOUT TIME ZONE",
    "TIMESTAMP",
    "TIMESTAMPTZ",
    "TIMETZ",
    "TINYINT",
    "UNION",
    "UROWID",
    "VARBINARY",
    "VARCHAR",
    "VARIANT",
    "XML",
    "XMLTYPE",
    "YEAR",
    "TIMESTAMP_NTZ",
    "TIMESTAMP_LTZ",
    "TIMESTAMP_TZ",
}


class DataTypeTest(TestCase):
    def test_check_datatype_support(self):
        for types in SQLTYPES:
            with self.subTest(line=types):
                col_type = ColumnTypeParser.get_column_type(types)
                col_type = True if col_type != "NULL" else False
                self.assertTrue(col_type, msg=types)
