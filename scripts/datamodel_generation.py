#  Copyright 2021 Collate
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""
This script generates the Python models from the JSON Schemas definition. Additionally, it replaces the `SecretStr`
pydantic class used for the password fields with the `CustomSecretStr` pydantic class which retrieves the secrets
from a configured secrets' manager.
"""

import datamodel_code_generator.model.pydantic
from datamodel_code_generator.imports import Import
import os
import re
import json



datamodel_code_generator.model.pydantic.types.IMPORT_SECRET_STR = Import.from_full_path(
    "metadata.ingestion.models.custom_pydantic.CustomSecretStr"
)

# Monkey patch to disable remote reference resolution
# datamodel-code-generator tries to download URLs from $id fields, which causes YAML parsing errors
from datamodel_code_generator.parser.jsonschema import JsonSchemaParser

_original_get_ref_body_from_url = JsonSchemaParser._get_ref_body_from_url

def _patched_get_ref_body_from_url(self, resolved_ref):
    """Patch to prevent remote URL resolution - convert to local file path"""
    if resolved_ref.startswith('http://') or resolved_ref.startswith('https://'):
        # Convert URL to local file path
        # e.g., https://open-metadata.org/schema/type/basic.json -> ./spec/src/main/resources/json/schema/type/basic.json
        url_match = re.search(r'https?://[^/]+/(.+)', resolved_ref)
        if url_match:
            local_path = f"./spec/src/main/resources/json/schema/{url_match.group(1)}"
            if os.path.exists(local_path):
                # Use local file instead of remote URL
                with open(local_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
        # If local file doesn't exist, raise an error instead of trying to download
        raise ValueError(f"Remote URL references are disabled. Local file not found for: {resolved_ref}")
    return _original_get_ref_body_from_url(self, resolved_ref)

JsonSchemaParser._get_ref_body_from_url = _patched_get_ref_body_from_url

from datamodel_code_generator.__main__ import main

current_directory = os.getcwd()
ingestion_path = "./"

UTF_8 = "UTF-8"
UNICODE_REGEX_REPLACEMENT_FILE_PATHS = [
    f"{ingestion_path}src/metadata/generated/schema/entity/classification/tag.py",
    f"{ingestion_path}src/metadata/generated/schema/entity/events/webhook.py",
    f"{ingestion_path}src/metadata/generated/schema/entity/teams/user.py",
    f"{ingestion_path}src/metadata/generated/schema/entity/type.py",
    f"{ingestion_path}src/metadata/generated/schema/type/basic.py",
]

args = f"--input ./spec/src/main/resources/json/schema --input-file-type jsonschema --output {ingestion_path}src/metadata/generated/schema --set-default-enum-member".split(" ")

print("datamodel_code_generator: %s" % args)

main(args)

for file_path in UNICODE_REGEX_REPLACEMENT_FILE_PATHS:
    print("Processing file:", file_path)
    with open(file_path, "r", encoding=UTF_8) as file_:
        content = file_.read()
        # Python now requires to move the global flags at the very start of the expression
        content = content.replace("(?U)", "(?u)")
    with open(file_path, "w", encoding=UTF_8) as file_:
        file_.write(content)


# Until https://github.com/koxudaxi/datamodel-code-generator/issues/1895
MISSING_IMPORTS = [f"{ingestion_path}src/metadata/generated/schema/entity/applications/app.py",]
WRITE_AFTER = "from __future__ import annotations"

for file_path in MISSING_IMPORTS:
    with open(file_path, "r", encoding=UTF_8) as file_:
        lines = file_.readlines()
    with open(file_path, "w", encoding=UTF_8) as file_:
        for line in lines:
            file_.write(line)
            if line.strip() == WRITE_AFTER:
                file_.write("from typing import Union  # custom generate import\n\n")
