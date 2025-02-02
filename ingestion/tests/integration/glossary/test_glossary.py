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
Test the SQL profiler using a Postgres and a MySQL container.
We load a simple user table in each service and run the profiler on it.
To run this we need Metadata server up and running.
No sample data is required beforehand
"""
from metadata.generated.schema.entity.data.glossary import Glossary
from tests.integration.integration_base import (
    int_admin_ometa,
)

from metadata.glossary.matcher.glossary_terms_matcher import GlossaryTermsMatcher


class TestGlossary:
    server_url = "http://192.168.105.2:8585/api"

    # init
    def __init__(self):
        self.setup()

    def setup(self):
        self.metadata = int_admin_ometa(url=self.server_url)

    def tear_down(self):
        self.metadata.close()

    def test_get_glossary(self):
        """test a simple profiler workflow on a table in each service and validate the profile is created"""
        try:
            if self.metadata.get_glossaries("") is None:
                print("No glossaries found")

            glossary = self.metadata.get_glossaries("public_terminology")
            if glossary is None:
                print("public_terminology No glossaries found")

            # if glossary is not None:
            #     print(f"glossary :{glossary}")
            #     terms = self.metadata.get_glossary_terms(glossary[0])
            #     if terms is not None:
            #         print(f"terms count :{len(terms)}")
            #     else:
            #         print("No terms found")

            glossary_matcher = GlossaryTermsMatcher(self.metadata)
            result = glossary_matcher.match("테스")
            if result is not None:
                print(f"result :{result[0].__root__}")
            result = glossary_matcher.match("1회섭취참고량명")
            if result is not None:
                print(f"result :{result[0].__root__}")
            result = glossary_matcher.match("RAFOS_NM")
            if result is not None:
                print(f"result :{result[0].__root__}")
            result = glossary_matcher.match("BBS_EXPLN")
            if result is not None:
                print(f"result :{result[0].__root__}")


        except Exception as e:
            print(f"error :{e}")


if __name__ == "__main__":
    profiler = TestGlossary()

    profiler.test_get_glossary()

    profiler.tear_down()
