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
Models required for dbt 
"""

from typing import Any, List, Optional

from pydantic import BaseModel


class DbtFiles(BaseModel):
    dbt_catalog: Optional[dict]
    dbt_manifest: dict
    dbt_run_results: Optional[dict]


class DbtObjects(BaseModel):
    dbt_catalog: Optional[Any]
    dbt_manifest: Any
    dbt_run_results: Optional[Any]


class DbtFilteredModel(BaseModel):
    is_filtered: Optional[bool] = False
    message: Optional[str]
    model_fqn: Optional[str]


class DbtMetaGlossaryTier(BaseModel):
    tier: Optional[str]
    glossary: Optional[List[str]]


class DbtMeta(BaseModel):
    openmetadata: Optional[DbtMetaGlossaryTier]
