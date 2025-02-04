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
Abstract definition of each step
"""
from abc import ABC, abstractmethod
from typing import Any, Iterable, Optional

from metadata.ingestion.api.models import Entity
from metadata.ingestion.api.step import BulkStep, IterStep, ReturnStep, StageStep
from metadata.ingestion.server.server_api import ServerInterface
from metadata.utils.execution_time_tracker import (
    calculate_execution_time,
    calculate_execution_time_generator,
)
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


class InvalidSourceException(Exception):
    """
    The source config is not getting the expected
    service connection
    """


class Source(IterStep, ABC):
    """
    Abstract source implementation. The workflow will run
    its next_record and pass them to the next step.
    """

    metadata: ServerInterface
    connection_obj: Any
    service_connection: Any

    @abstractmethod
    def prepare(self):
        pass

    @abstractmethod
    def test_connection(self) -> None:
        pass

    @property
    def name(self) -> str:
        return "Source"

    @calculate_execution_time_generator(context="Source")
    def run(self) -> Iterable[Optional[Entity]]:
        yield from super().run()


class Sink(ReturnStep, ABC):
    """All Sinks must inherit this base class."""

    @property
    def name(self) -> str:
        return "Sink"

    @calculate_execution_time(context="Sink")
    def run(self, record: Entity) -> Optional[Entity]:
        return super().run(record)


class Processor(ReturnStep, ABC):
    """All Processor must inherit this base class"""

    @property
    def name(self) -> str:
        return "Processor"


class Stage(StageStep, ABC):
    """All Stages must inherit this base class."""

    @property
    def name(self) -> str:
        return "Stage"


class BulkSink(BulkStep, ABC):
    """All Stages must inherit this base class."""

    @property
    def name(self) -> str:
        return "BulkSink"
