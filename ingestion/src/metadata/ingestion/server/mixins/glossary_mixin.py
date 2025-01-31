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
Mixin class containing Table specific methods

To be used by OpenMetadata class
"""
import traceback
from typing import List, Optional, TypeVar

from pydantic import BaseModel

from metadata.generated.schema.entity.data.glossary import Glossary
from metadata.generated.schema.entity.data.glossaryTerm import GlossaryTerm
from metadata.ingestion.server.client import REST
from metadata.utils.logger import ometa_logger

logger = ometa_logger()

T = TypeVar("T", bound=BaseModel)


class OMetaGlossaryMixin:
    """
    DataFabric API methods related to Glossary.
    """

    client: REST

    def get_glossaries(self, glossary_name: str) -> Optional[List[Glossary]]:
        """
        GET call for the /glossaries endpoint
        """
        try:
            if glossary_name is not None and glossary_name != "":
                resp = self.client.get(f"{self.get_suffix(Glossary)}/name/{glossary_name}")
                return [Glossary(**resp)]

            resp = self.client.get(f"{self.get_suffix(Glossary)}")
            return [Glossary(**datum) for datum in resp["data"]]
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.warning(f"Error trying to Get Glossaries data : {exc}")
        return None

    def get_glossary_terms(self, glossary: Glossary) -> Optional[List[GlossaryTerm]]:
        """
        GET call for the /glossaryterms endpoint
        """
        resp = None
        try:
            resp = self.client.get(f"{self.get_suffix(GlossaryTerm)}", data={"glossary": glossary.id.__root__, "limit": 10000})
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.warning(
                f"Error trying to Get GlossaryTerms data {glossary.fullyQualifiedName.__root__}: {exc}"
            )

        if resp:
            try:
                return [GlossaryTerm(**datum) for datum in resp["data"]]
            except Exception as exc:
                logger.debug(traceback.format_exc())
                logger.warning(
                    f"Error trying to parse GlossaryTerms : {exc}"
                )

        return None
