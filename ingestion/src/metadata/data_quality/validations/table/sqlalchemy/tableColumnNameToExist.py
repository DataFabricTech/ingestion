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
Validator for table column nanme to exist test case
"""

from sqlalchemy import inspect

from metadata.data_quality.validations.mixins.sqa_validator_mixin import (
    SQAValidatorMixin,
)
from metadata.data_quality.validations.table.base.tableColumnNameToExist import (
    BaseTableColumnNameToExistValidator,
)
from metadata.utils.logger import test_suite_logger

logger = test_suite_logger()


class TableColumnNameToExistValidator(
    BaseTableColumnNameToExistValidator, SQAValidatorMixin
):
    """Validator for table column nanme to exist test case"""

    def _run_results(self):
        """compute result of the test case"""
        names = inspect(self.runner.table).c
        if not names:
            raise ValueError(
                f"Column names for test case {self.test_case.name} returned None"
            )

        return names
