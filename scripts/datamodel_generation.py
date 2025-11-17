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
        # Extract path after 'schema/' to avoid duplication
        # e.g., https://open-metadata.org/schema/type/basic.json -> ./spec/src/main/resources/json/schema/type/basic.json
        # Handle cases where path might be duplicated: entity/services/entity/services/... -> entity/services/...
        schema_match = re.search(r'/schema/(.+)', resolved_ref)
        if schema_match:
            path_after_schema = schema_match.group(1)
            
            # Handle duplicate path segments (e.g., entity/services/entity/services/...)
            # Simple approach: find the last occurrence of common patterns and use everything after it
            clean_path = path_after_schema
            
            # Remove duplicate patterns by finding the last occurrence
            # Pattern: entity/services/entity/services/... -> entity/services/...
            if 'entity/services/' in clean_path:
                # Count occurrences
                occurrences = [m.start() for m in re.finditer(r'entity/services/', clean_path)]
                if len(occurrences) > 1:
                    # Use path starting from the last occurrence
                    last_start = occurrences[-1]
                    clean_path = clean_path[last_start:]
            
            # Try multiple possible base paths (for both local and Docker environments)
            base_paths = [
                "./spec/src/main/resources/json/schema",
                "/metadata_collector/spec/src/main/resources/json/schema",
                os.path.join(os.getcwd(), "spec/src/main/resources/json/schema"),
            ]
            
            tried_paths = []
            for base_path in base_paths:
                local_path = os.path.join(base_path, clean_path)
                tried_paths.append(local_path)
                if os.path.exists(local_path):
                    with open(local_path, 'r', encoding='utf-8') as f:
                        return json.load(f)
        
        # If local file doesn't exist, raise an error instead of trying to download
        if 'tried_paths' not in locals():
            tried_paths = [f"./spec/src/main/resources/json/schema/{clean_path}" if 'clean_path' in locals() else 'N/A']
        raise ValueError(f"Remote URL references are disabled. Local file not found for: {resolved_ref}. Tried paths: {tried_paths}")
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

try:
    main(args)
    print("✓ datamodel_code_generator completed successfully")
except Exception as e:
    print(f"✗ datamodel_code_generator failed: {e}")
    raise

print("Processing unicode regex replacements...")
for file_path in UNICODE_REGEX_REPLACEMENT_FILE_PATHS:
    if os.path.exists(file_path):
        print(f"  Processing file: {file_path}")
        with open(file_path, "r", encoding=UTF_8) as file_:
            content = file_.read()
            # Python now requires to move the global flags at the very start of the expression
            content = content.replace("(?U)", "(?u)")
        with open(file_path, "w", encoding=UTF_8) as file_:
            file_.write(content)
    else:
        print(f"  Skipping missing file: {file_path}")
print("✓ Unicode regex replacements completed")


# Until https://github.com/koxudaxi/datamodel-code-generator/issues/1895
print("Processing missing imports...")
MISSING_IMPORTS = [f"{ingestion_path}src/metadata/generated/schema/entity/applications/app.py",]
WRITE_AFTER = "from __future__ import annotations"

for file_path in MISSING_IMPORTS:
    if os.path.exists(file_path):
        print(f"  Processing file: {file_path}")
        with open(file_path, "r", encoding=UTF_8) as file_:
            lines = file_.readlines()
        with open(file_path, "w", encoding=UTF_8) as file_:
            for line in lines:
                file_.write(line)
                if line.strip() == WRITE_AFTER:
                    file_.write("from typing import Union  # custom generate import\n\n")
    else:
        print(f"  Skipping missing file: {file_path}")
print("✓ Missing imports processing completed")

print("=" * 60)
print("✓ datamodel_generation.py completed successfully!")
print("=" * 60)
