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

"""Admin user configuration for e2e tests."""

import time
from typing import Optional

from playwright.sync_api import Page, expect


class User:
    def __init__(
        self, username: str, password: str, display_name: Optional[str] = None
    ):
        """Initialize the admin user."""
        self.username = username
        self.password = password
        self.display_name = display_name

    def login(self, page: Page):
        """Login as the user."""
        page.get_by_label("Username or Email").fill(self.username)
        page.get_by_label("Password").fill(self.password)
        page.get_by_role("button", name="Login").click()
        time.sleep(0.5)
        page.reload()
        expect(page.get_by_test_id("app-bar-item-explore")).to_be_visible()

    def delete(self, page: Page):
        """Delete the user."""
        page.get_by_test_id("app-bar-item-settings").click()
        page.get_by_test_id("global-setting-left-panel").get_by_text("Users").click()
        page.get_by_test_id("searchbar").fill(self.display_name)  # type: ignore
        page.get_by_test_id("searchbar").press("Enter")
        page.get_by_role("row", name=self.display_name).get_by_role("button").click()
        page.get_by_test_id("hard-delete").check()
        page.get_by_test_id("confirmation-text-input").fill("DELETE")
        page.get_by_test_id("confirm-button").click()
