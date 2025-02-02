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
Oracle models
"""
from typing import List, Optional

from pydantic import BaseModel, Field


class OracleStoredProcedure(BaseModel):
    """Oracle Stored Procedure list query results"""

    name: str
    definition: str
    language: Optional[str] = Field(
        None, description="Will only be informed for non-SQL routines."
    )
    owner: str


class FetchProcedure(BaseModel):
    """Oracle Fetch Stored Procedure Raw Model"""

    owner: Optional[str]
    name: str
    line: int
    text: str


class FetchProcedureList(BaseModel):
    __name__: List[FetchProcedure]
