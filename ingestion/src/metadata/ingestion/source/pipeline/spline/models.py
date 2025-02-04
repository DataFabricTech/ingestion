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
Spline connector API response models
"""
from typing import List, Optional

from pydantic import BaseModel, Field


class ExecutionEvent(BaseModel):
    executionEventId: Optional[str]
    executionPlanId: Optional[str]
    applicationName: Optional[str]


class ExecutionEvents(BaseModel):
    items: Optional[List[ExecutionEvent]] = []
    totalCount: Optional[int] = 0
    pageNum: Optional[int] = 0
    pageSize: Optional[int] = 0


class Inputs(BaseModel):
    source: Optional[str]


class Output(BaseModel):
    source: Optional[str]


class AttributesNames(BaseModel):
    id: Optional[str]


class Extra(BaseModel):
    attributes: Optional[List[AttributesNames]] = []


class ExecutionPlan(BaseModel):
    id: Optional[str] = Field(..., alias="_id")
    name: Optional[str]
    inputs: Optional[List[Inputs]] = []
    output: Optional[Output] = None
    extra: Optional[Extra] = None


class ExecutionDetail(BaseModel):
    executionPlan: Optional[ExecutionPlan] = None


class ColNodes(BaseModel):
    id: Optional[str] = Field(..., alias="_id")
    name: Optional[str]


class ColLineage(BaseModel):
    source: Optional[str]
    target: Optional[str]


class Lineage(BaseModel):
    edges: Optional[List[ColLineage]] = []
    nodes: Optional[List[ColNodes]] = []


class AttributeDetail(BaseModel):
    lineage: Optional[Lineage] = None
