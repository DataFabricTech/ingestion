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

from os import walk
from pathlib import Path
from unittest import TestCase

import yaml

from metadata.ingestion.api.parser import parse_workflow_config_gracefully


class TestWorkflowParse(TestCase):
    """
    Test parsing scenarios of JSON Schemas
    """

    def test_parse_workflow_config(self):
        package_path = (
            f"{Path(__file__).parent.parent.parent}/src/metadata/examples/workflows"
        )
        workflow_files = [files for _, _, files in walk(package_path)]
        for yaml_file in workflow_files[0]:
            with self.subTest(file_name=yaml_file):
                with open(f"{package_path}/{yaml_file}", "r") as file:
                    file_content = file.read()
                    self.assertTrue(
                        parse_workflow_config_gracefully(yaml.safe_load(file_content))
                    )
                    file.close()
