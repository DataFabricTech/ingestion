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

"""Base database (supporting SQA) test container for integration tests"""

from ...sqa import SQATestUtils
from ..abstract_test_container import AbstractTestContainer


class DataBaseTestContainer(AbstractTestContainer):
    def __init__(self):
        self.sqa_test_utils = SQATestUtils(self.get_connection_url())
        self.sesssion = self.sqa_test_utils.session
        self.engine = self.sqa_test_utils.engine
        self.sqa_test_utils.create_user_table()
        self.sqa_test_utils.load_user_data()
