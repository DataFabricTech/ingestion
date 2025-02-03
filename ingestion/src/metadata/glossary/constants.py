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
Glossary constants
"""
from enum import Enum


class GlossaryConstants(Enum):
    """
    Constants for Glossary
    """

    # 공공데이터표준용어
    GLOSSARY_NAME_PUBLIC_TERMINOLOGY = "public_terminology"
    # 공공데이터표준단어
    GLOSSARY_NAME_PUBLIC_WORD = "public_word"