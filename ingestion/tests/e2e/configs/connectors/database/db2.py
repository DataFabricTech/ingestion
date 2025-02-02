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

"""Redshift connector for e2e tests"""

import os

from playwright.sync_api import Page, expect

from .interface import DataBaseConnectorInterface


class Db2Connector(DataBaseConnectorInterface):
    """db2 connector"""

    def get_service(self, page: Page):
        """get service from the service page"""
        page.get_by_test_id("Db2").click()

    def set_connection(self, page):
        """Set connection for redshift service"""
        page.get_by_label("Username*").fill(os.environ["E2E_DB2_USERNAME"])
        expect(page.get_by_label("Username*")).to_have_value(
            os.environ["E2E_DB2_USERNAME"]
        )

        page.get_by_label("Password").fill(os.environ["E2E_DB2_PASSWORD"])
        expect(page.get_by_label("Password")).to_have_value(
            os.environ["E2E_DB2_PASSWORD"]
        )

        page.get_by_label("Host and Port*").fill(os.environ["E2E_DB2_HOST_PORT"])
        expect(page.get_by_label("Host and Port*")).to_have_value(
            os.environ["E2E_DB2_HOST_PORT"]
        )

        page.get_by_label("database*").fill(os.environ["E2E_DB2_DATABASE"])
        expect(page.get_by_label("database*")).to_have_value(
            os.environ["E2E_DB2_DATABASE"]
        )
