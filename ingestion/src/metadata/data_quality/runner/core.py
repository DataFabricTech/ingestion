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
Main class to run data tests
"""


from metadata.data_quality.api.models import TestCaseResultResponse
from metadata.data_quality.interface.test_suite_interface import TestSuiteInterface
from metadata.generated.schema.tests.testCase import TestCase
from metadata.utils.logger import test_suite_logger

logger = test_suite_logger()


class DataTestsRunner:
    """class to execute the test validation"""

    def __init__(self, test_runner_interface: TestSuiteInterface):
        self.test_runner_interface = test_runner_interface

    def run_and_handle(self, test_case: TestCase):
        """run and handle test case validation"""
        logger.info(
            f"Executing test case {test_case.name.__root__} "
            f"for entity {self.test_runner_interface.table_entity.fullyQualifiedName.__root__}"
        )
        test_result = self.test_runner_interface.run_test_case(
            test_case,
        )

        if test_result:
            return TestCaseResultResponse(
                testCaseResult=test_result, testCase=test_case
            )
        return None
