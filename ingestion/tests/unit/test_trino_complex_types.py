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

from metadata.ingestion.source.database.trino.metadata import (
    parse_array_data_type,
    parse_row_data_type,
)

RAW_ARRAY_DATA_TYPES = [
    "array(string)",
    "array(row(check_datatype array(string)))",
]

EXPECTED_ARRAY_DATA_TYPES = [
    "array<string>",
    "array<struct<check_datatype:array<string>>>",
]

RAW_ROW_DATA_TYPES = [
    "row(a int, b string)",
    "row(a row(b array(string),c bigint))",
    "row(a array(string))",
    "row(bigquerytestdatatype51 array(row(bigquery_test_datatype_511 array(string))))",
    "row(record_1 row(record_2 row(record_3 row(record_4 string))))",
]

EXPECTED_ROW_DATA_TYPES = [
    "struct<a:int,b:string>",
    "struct<a:struct<b:array<string>,c:bigint>>",
    "struct<a:array<string>>",
    "struct<bigquerytestdatatype51:array<struct<bigquery_test_datatype_511:array<string>>>>",
    "struct<record_1:struct<record_2:struct<record_3:struct<record_4:string>>>>",
]


class SouceConnectionTest(TestCase):
    def test_array_datatype(self):
        for i in range(len(RAW_ARRAY_DATA_TYPES)):
            parsed_type = parse_array_data_type(RAW_ARRAY_DATA_TYPES[i])
            assert parsed_type == EXPECTED_ARRAY_DATA_TYPES[i]

    def test_row_datatype(self):
        for i in range(len(RAW_ROW_DATA_TYPES)):
            parsed_type = parse_row_data_type(RAW_ROW_DATA_TYPES[i])
            assert parsed_type == EXPECTED_ROW_DATA_TYPES[i]
