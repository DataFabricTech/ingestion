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
Regex scanner for column names
"""
import re
from typing import Optional

from metadata.generated.schema.entity.classification.tag import Tag
from metadata.pii.constants import PII
from metadata.pii.models import TagAndConfidence, TagType
from metadata.utils import fqn


class ColumnNameScanner:
    """
    Column Name Scanner to scan column name
    """

    sensitive_regex = {
        "PASSWORD": re.compile("^.*password.*$", re.IGNORECASE),
        "SSN": re.compile("^.*(ssn|social).*$", re.IGNORECASE),
        "CREDIT_CARD": re.compile("^.*(credit).*(card).*$", re.IGNORECASE),
        "BANK_ACCOUNT": re.compile("^.*bank.*(acc|num).*$", re.IGNORECASE),
        "EMAIL_ADDRESS": re.compile("^.*(email|e-mail|mail).*$", re.IGNORECASE),
        "USER_NAME": re.compile("^.*(user|client|person).*(name).*$", re.IGNORECASE),
        "PERSON": re.compile(
            "^.*(firstname|lastname|fullname|maidenname|nickname|name_suffix).*$",
            re.IGNORECASE,
        ),
    }
    non_sensitive_regex = {
        "BIRTH_DATE": re.compile(
            "^.*(date_of_birth|dateofbirth|dob|"
            "birthday|date_of_death|dateofdeath).*$",
            re.IGNORECASE,
        ),
        "GENDER": re.compile("^.*(gender).*$", re.IGNORECASE),
        "NATIONALITY": re.compile("^.*(nationality).*$", re.IGNORECASE),
        "ADDRESS": re.compile(
            "^.*(address|city|state|county|country|"
            "zipcode|zip|postal|zone|borough).*$",
            re.IGNORECASE,
        ),
        "PHONE_NUMBER": re.compile("^.*(phone).*$", re.IGNORECASE),
    }

    @classmethod
    def scan(cls, column_name: str) -> Optional[TagAndConfidence]:
        """
        Check the column name against the regex patterns and prepare the
        sensitive or non-sensitive tag
        """
        for pii_type_pattern in cls.sensitive_regex.values():
            if pii_type_pattern.match(column_name) is not None:
                return TagAndConfidence(
                    tag_fqn=fqn.build(
                        metadata=None,
                        entity_type=Tag,
                        classification_name=PII,
                        tag_name=TagType.SENSITIVE.value,
                    ),
                    confidence=1,
                )

        for pii_type_pattern in cls.non_sensitive_regex.values():
            if pii_type_pattern.match(column_name) is not None:
                return TagAndConfidence(
                    tag_fqn=fqn.build(
                        metadata=None,
                        entity_type=Tag,
                        classification_name=PII,
                        tag_name=TagType.NONSENSITIVE.value,
                    ),
                    confidence=1,
                )

        return None
