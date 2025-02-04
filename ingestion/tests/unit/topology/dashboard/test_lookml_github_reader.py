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
Test GitHub Reader
"""
from unittest import TestCase

from metadata.generated.schema.security.credentials.githubCredentials import (
    GitHubCredentials,
)
from metadata.ingestion.source.dashboard.looker.models import Includes, ViewName
from metadata.ingestion.source.dashboard.looker.parser import LkmlParser
from metadata.readers.file.github import GitHubReader


class TestLookMLGitHubReader(TestCase):
    """
    Validate the github reader against the OM repo
    """

    creds = GitHubCredentials(
        repositoryName="lookml-sample",
        repositoryOwner="open-metadata",
    )

    reader = GitHubReader(creds)
    parser = LkmlParser(reader)

    def x_test_lookml_read_and_parse(self):
        """
        We can parse the explore file.

        We'll expand and find views from https://github.com/open-metadata/lookml-sample/blob/main/cats.explore.lkml

        disabling this test as it is flakey and fails with error rate limit exceeded
        """

        explore_file = "cats.explore.lkml"
        self.parser.parse_file(Includes(explore_file))

        contents = self.parser.parsed_files.get(Includes(explore_file))

        # Check file contents
        self.assertIn("explore: cats", contents)

        view = self.parser.find_view(
            view_name=ViewName("cats"), path=Includes(explore_file)
        )

        # We can get views that are resolved even if the include does not contain `.lkml`
        self.assertIsNotNone(view)
        self.assertEqual(view.name, "cats")
