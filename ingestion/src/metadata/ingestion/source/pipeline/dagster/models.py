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
Tableau Source Model module
"""

from typing import List, Optional

from pydantic import BaseModel

# Models for get_task_runs


class RunStepStats(BaseModel):
    runId: str
    startTime: Optional[float]
    endTime: Optional[float]
    status: Optional[str]


class SolidStepStatsConnection(BaseModel):
    nodes: Optional[List[RunStepStats]]


class TaskSolidHandle(BaseModel):
    stepStats: Optional[SolidStepStatsConnection]


class DagsterPipeline(BaseModel):
    id: str
    name: str
    description: Optional[str]
    solidHandle: Optional[TaskSolidHandle]


class PipelineOrErrorModel(BaseModel):
    pipelineOrError: DagsterPipeline


# Models for get_run_list
class DagsterLocation(BaseModel):
    id: str
    name: str


class Node(BaseModel):
    id: str
    name: str
    location: Optional[DagsterLocation]
    pipelines: List[DagsterPipeline]


class RepositoryConnection(BaseModel):
    nodes: List[Node]


class RepositoriesOrErrorModel(BaseModel):
    repositoriesOrError: RepositoryConnection


# Models for get_jobs
class SolidName(BaseModel):
    name: str


class DependsOnSolid(BaseModel):
    solid: Optional[SolidName]


class SolidInput(BaseModel):
    dependsOn: Optional[List[DependsOnSolid]]


class Solid(BaseModel):
    name: str
    inputs: Optional[List[SolidInput]]


class SolidHandle(BaseModel):
    handleID: str
    solid: Optional[Solid]


class GraphOrError(BaseModel):
    id: str
    name: str
    description: Optional[str]
    solidHandles: Optional[List[SolidHandle]]


class GraphOrErrorModel(BaseModel):
    graphOrError: GraphOrError
