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
from metadata.readers.file.github import GitHubReader


class TestGitHubReader(TestCase):
    """
    Validate the github reader against the OM repo
    """

    def test_headers(self):
        """
        We build the headers correctly
        """
        creds = GitHubCredentials(
            repositoryName="name", repositoryOwner="owner", token="token"
        )

        reader = GitHubReader(creds)

        self.assertEqual(reader.auth_headers, {"Authorization": "Bearer token"})

    def x_test_read(self):
        """
        We can read the OM README

        disabling this test as it is flakey and fails with error rate limit exceeded
        """
        creds = GitHubCredentials(
            repositoryName="Metadata",
            repositoryOwner="metadata",
        )

        reader = GitHubReader(creds)
        self.assertIsNotNone(reader.read("README.md"))
