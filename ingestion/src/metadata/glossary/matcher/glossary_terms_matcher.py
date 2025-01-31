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
import traceback
from typing import List, Optional, Dict

from metadata.generated.schema.entity.data.glossaryTerm import GlossaryTerm
from metadata.generated.schema.type import basic
from metadata.generated.schema.type.basic import FullyQualifiedEntityName
from metadata.ingestion.server.server_api import OpenMetadata
from metadata.utils.logger import profiler_logger
from metadata.glossary.constants import GlossaryConstants

logger = profiler_logger()


class GlossaryTermsMatcher:
    glossary_terms: Dict[str, List[GlossaryTerm]] = {}

    def __init__(self, metadata: OpenMetadata):
        self.metadata = metadata
        # GET Glossary
        glossary_list = self.metadata.get_glossaries("")

        # GET Glossary Terms
        for glossary in glossary_list:
            if glossary.name.__root__ == GlossaryConstants.GLOSSARY_NAME_PUBLIC_TERMINOLOGY.value:
                glossary_terms = self.metadata.get_glossary_terms(glossary)
                if glossary_terms is not None:
                    self.glossary_terms[GlossaryConstants.GLOSSARY_NAME_PUBLIC_TERMINOLOGY.name] = glossary_terms
                    continue
            if glossary.name.__root__ == GlossaryConstants.GLOSSARY_NAME_PUBLIC_WORD.value:
                glossary_terms = self.metadata.get_glossary_terms(glossary)
                if glossary_terms is not None:
                    self.glossary_terms[GlossaryConstants.GLOSSARY_NAME_PUBLIC_WORD.name] = glossary_terms
                    continue

    def __match_terminology_terms(self, name: str) -> Optional[List[basic.FullyQualifiedEntityName]]:
        terms_fqn_list = []
        for term in self.glossary_terms[GlossaryConstants.GLOSSARY_NAME_PUBLIC_TERMINOLOGY.name]:
            try:
                if name == term.name.__root__:
                    terms_fqn_list.append(term.fullyQualifiedName)
                    continue
                if term.synonyms is not None:
                    for synonym in term.synonyms:
                        if name == synonym.__root__:
                            terms_fqn_list.append(term.fullyQualifiedName)
                            break

            except Exception as exc:
                logger.warning(f"Unknown error while glossary processing for {name} - {exc}")
                logger.debug(traceback.format_exc())
        if len(terms_fqn_list) > 0:
            return terms_fqn_list
        return None

    def __match_word_terms(self, name: str) -> Optional[List[FullyQualifiedEntityName]]:
        terms_fqn_list = []
        for term in self.glossary_terms[GlossaryConstants.GLOSSARY_NAME_PUBLIC_WORD.name]:
            try:
                if name == term.name.__root__:
                    terms_fqn_list.append(term.fullyQualifiedName)
                    continue
                if term.synonyms is not None:
                    for synonym in term.synonyms:
                        if name in synonym.__root__:
                            terms_fqn_list.append(term.fullyQualifiedName)
                            break

            except Exception as exc:
                logger.warning(f"Unknown error while glossary processing for {name} - {exc}")
                logger.debug(traceback.format_exc())
        if len(terms_fqn_list) > 0:
            return terms_fqn_list
        return None

    def match(self, name: str) -> Optional[List[basic.FullyQualifiedEntityName]]:
        logger.debug("Processing Glossary Match For '%s'", name)

        terms_fqn_list = []
        if self.glossary_terms is None:
            return None

        for glossary_name, glossary_terms in self.glossary_terms.items():
            logger.debug("Processing With Glossary[%s] Terms", glossary_name)
            if glossary_name == GlossaryConstants.GLOSSARY_NAME_PUBLIC_TERMINOLOGY.name:
                result = self.__match_terminology_terms(name)
                if result is not None:
                    terms_fqn_list.extend(result)
            if glossary_name == GlossaryConstants.GLOSSARY_NAME_PUBLIC_WORD.name:
                result = self.__match_word_terms(name)
                if result is not None:
                    terms_fqn_list.extend(result)

        if len(terms_fqn_list) > 0:
            return terms_fqn_list

        return None
