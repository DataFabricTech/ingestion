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
Handle column logic when reading data from DataLake
"""
from metadata.utils.constants import COMPLEX_COLUMN_SEPARATOR


def _get_root_col(col_name: str) -> str:
    return col_name.split(COMPLEX_COLUMN_SEPARATOR)[1]


def clean_dataframe(df):
    all_complex_root_columns = set(
        _get_root_col(col) for col in df if COMPLEX_COLUMN_SEPARATOR in col
    )
    for complex_col in all_complex_root_columns:
        if complex_col in df.columns:
            df = df.drop(complex_col, axis=1)
    return df
