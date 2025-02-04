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
Mixin class containing Suggestions specific methods

To be used by Metadata class
"""
from metadata.generated.schema.entity.feed.suggestion import Suggestion
from metadata.ingestion.server.client import REST
from metadata.utils.logger import ometa_logger

logger = ometa_logger()


class OMetaSuggestionsMixin:
    """
    Metadata API methods related to the Suggestion Entity

    To be inherited by Metadata
    """

    client: REST

    def update_suggestion(self, suggestion: Suggestion) -> Suggestion:
        """
        Update an existing Suggestion with new fields
        """
        resp = self.client.put(
            f"{self.get_suffix(Suggestion)}/{str(suggestion.id.__root__)}",
            data=suggestion.json(),
        )

        return Suggestion(**resp)
