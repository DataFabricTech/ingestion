# Copyright 2024 Mobigen;
# Licensed under the Apache License, Version 2.0 (the "License");
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

import json
import os
from pathlib import Path


class ConfigurationError(Exception):
    """A configuration error has happened"""


def load_config_file(config_file: Path) -> dict:
    if not config_file.is_file():
        raise ConfigurationError(f"Cannot open config file {config_file}")

    with config_file.open() as raw_config_file:
        raw_config = raw_config_file.read()

    expanded_config_file = os.path.expandvars(raw_config)
    config = json.loads(expanded_config_file)

    return config
