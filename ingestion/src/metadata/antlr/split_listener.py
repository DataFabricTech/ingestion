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
Helper class to handle FQN splitting logic
"""
from metadata.generated.antlr.EntityLinkListener import EntityLinkListener
from metadata.generated.antlr.EntityLinkParser import EntityLinkParser
from metadata.generated.antlr.FqnListener import FqnListener
from metadata.generated.antlr.FqnParser import FqnParser


class FqnSplitListener(FqnListener):
    def __init__(self):
        self._list = []

    def enterQuotedName(self, ctx: FqnParser.QuotedNameContext):
        self._list.append(ctx.getText())

    def enterUnquotedName(self, ctx: FqnParser.UnquotedNameContext):
        self._list.append(ctx.getText())

    def split(self):
        return self._list


class EntityLinkSplitListener(EntityLinkListener):
    def __init__(self):
        self._list = []

    def enterNameOrFQN(self, ctx: EntityLinkParser.NameOrFQNContext):
        self._list.append(ctx.getText())

    def enterEntityType(self, ctx: EntityLinkParser.EntityTypeContext):
        self._list.append(ctx.getText())

    def enterEntityField(self, ctx: EntityLinkParser.EntityFieldContext):
        self._list.append(ctx.getText())

    def split(self):
        return self._list
