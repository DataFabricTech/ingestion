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

"""common database assertions"""

import time

from playwright.sync_api import Page, expect

from ...configs.common import go_to_service


def assert_change_database_owner(page_context: Page, service_name: str):
    """assert database owner can be changed as expected"""
    go_to_service("Databases", page_context, service_name)
    page_context.get_by_test_id("edit-owner").click()
    page_context.get_by_test_id("owner-select-users-search-bar").click()
    page_context.get_by_test_id("owner-select-users-search-bar").fill("Aaron Johnson")
    page_context.get_by_text("Aaron Johnson").click()
    expect(
        page_context.get_by_test_id("owner-label").get_by_test_id("owner-link")
    ).to_have_text("Aaron Johnson")


def assert_profile_data(
    page_context: Page,
    service_name: str,
    database: str,
    schema: str,
    table: str,
    connector_obj,
):
    """Assert profile data have been computed correctly"""
    go_to_service("Databases", page_context, service_name)
    page_context.get_by_role("link", name=database).click()
    page_context.get_by_role("link", name=schema).click()
    page_context.get_by_role("link", name=table, exact=True).click()
    page_context.get_by_text("Profiler & Data Quality").click()
    time.sleep(0.05)
    for card in range(connector_obj.profiler_summary_card_count):
        summary_card = page_context.get_by_test_id("summary-card-container").nth(card)
        description = summary_card.get_by_test_id(
            "summary-card-description"
        ).inner_text()
        assert description not in {"0"}


def assert_sample_data_ingestion(
    page_context: Page,
    service_name: str,
    database: str,
    schema: str,
    table: str,
):
    """assert sample data are ingested as expected"""
    go_to_service("Databases", page_context, service_name)
    page_context.get_by_role("link", name=database).click()
    page_context.get_by_role("link", name=schema).click()
    page_context.get_by_role("link", name=table, exact=True).click()
    page_context.get_by_text("Sample Data").click()

    expect(page_context.get_by_test_id("sample-data")).to_be_visible()


def assert_pii_column_auto_tagging(
    page_context: Page,
    service_name: str,
    database: str,
    schema: str,
    table: str,
    column: str,
):
    """assert pii column auto tagging tagged as expected"""
    go_to_service("Databases", page_context, service_name)
    page_context.get_by_role("link", name=database).click()
    page_context.get_by_role("link", name=schema).click()
    page_context.get_by_role("link", name=table, exact=True).click()

    time.sleep(0.05)
    table_row = page_context.locator(f'tr:has-text("{column}")')
    tag = table_row.locator("td:nth-child(4)")
    expect(tag).to_be_visible()
    assert tag.text_content() in {"Sensitive", "NonSensitive"}
