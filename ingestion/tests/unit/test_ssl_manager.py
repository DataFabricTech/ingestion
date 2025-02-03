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
Manage SSL test cases
"""
import os
import unittest

from pydantic import SecretStr

from metadata.utils.ssl_manager import SSLManager


class SSLManagerTest(unittest.TestCase):
    """
    Tests to verify the functionality of SSLManager
    """

    def setUp(self):
        self.ca = SecretStr("CA certificate content")
        self.key = SecretStr("Private key content")
        self.cert = SecretStr("Certificate content")
        self.ssl_manager = SSLManager(self.ca, self.key, self.cert)

    def tearDown(self):
        self.ssl_manager.cleanup_temp_files()

    def test_create_temp_file(self):
        content = SecretStr("Test content")
        temp_file = self.ssl_manager.create_temp_file(content)
        self.assertTrue(os.path.exists(temp_file))
        with open(temp_file, "r", encoding="UTF-8") as file:
            file_content = file.read()
        self.assertEqual(file_content, content.get_secret_value())
        content = SecretStr("")
        temp_file = self.ssl_manager.create_temp_file(content)
        self.assertTrue(os.path.exists(temp_file))
        with open(temp_file, "r", encoding="UTF-8") as file:
            file_content = file.read()
        self.assertEqual(file_content, content.get_secret_value())
        with self.assertRaises(AttributeError):
            content = None
            self.ssl_manager.create_temp_file(content)

    def test_cleanup_temp_files(self):
        temp_file = self.ssl_manager.create_temp_file(SecretStr("Test content"))
        self.ssl_manager.cleanup_temp_files()
        self.assertFalse(os.path.exists(temp_file))
