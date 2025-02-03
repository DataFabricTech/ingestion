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

import sys

import pytest

from .integration_base import int_admin_ometa

if not sys.version_info >= (3, 9):
    collect_ignore = ["trino"]


@pytest.fixture(scope="module")
def metadata():
    return int_admin_ometa()


def pytest_pycollect_makeitem(collector, name, obj):
    try:
        if obj.__base__.__name__ in ("BaseModel", "Enum"):
            return []
    except AttributeError:
        pass


@pytest.fixture(scope="session", autouse=sys.version_info >= (3, 9))
def config_testcontatiners():
    from testcontainers.core.config import testcontainers_config

    testcontainers_config.max_tries = 10
