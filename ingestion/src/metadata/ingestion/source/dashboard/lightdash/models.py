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

"""Lightdash models"""

from typing import List, Optional

from pydantic import BaseModel


class LightdashChart(BaseModel):
    """
    Lightdash chart model
    """

    name: str
    organizationUuid: str
    uuid: str
    description: Optional[str]
    projectUuid: str
    spaceUuid: str
    pinnedListUuid: Optional[str]
    spaceName: str
    chartType: Optional[str]
    dashboardUuid: Optional[str]
    dashboardName: Optional[str]


class LightdashDashboard(BaseModel):
    organizationUuid: str
    name: str
    description: Optional[str]
    uuid: str
    projectUuid: str
    updatedAt: str
    spaceUuid: str
    views: float
    firstViewedAt: str
    pinnedListUuid: Optional[str]
    pinnedListOrder: Optional[float]
    charts: Optional[List[LightdashChart]]


class LightdashChartList(BaseModel):
    charts: Optional[List[LightdashChart]]


class LightdashDashboardList(BaseModel):
    dashboards: Optional[List[LightdashDashboard]]
