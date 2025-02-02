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
Helpers functions to handle columns when we extract
their raw information from the source
"""


def truncate_column_name(col_name: str):
    """
    Metadata table column specification limits column name to 128 characters.
    To allow ingestion of tables we set name to truncate to 128 characters if its longer
    and use displayName to have the raw column name
    """
    return col_name[:256]
