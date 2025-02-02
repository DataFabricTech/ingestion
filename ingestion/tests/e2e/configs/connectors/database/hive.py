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

"""MySQL connector for e2e tests"""

import os

from playwright.sync_api import Page, expect

from .interface import DataBaseConnectorInterface


class HiveConnector(DataBaseConnectorInterface):
    def get_service(self, page: Page):
        """get service from the service page"""
        page.get_by_test_id("Hive").click()

    def set_connection(self, page):
        """Set connection for redshift service"""
        page.locator('[id="root\\/hostPort"]').fill(os.environ["E2E_HIVE_HOST_PORT"])
        expect(page.locator('[id="root\\/hostPort"]')).to_have_value(
            os.environ["E2E_HIVE_HOST_PORT"]
        )

        page.locator('[id="root\\/metastoreConnection__oneof_select"]').select_option(
            "2"
        )
