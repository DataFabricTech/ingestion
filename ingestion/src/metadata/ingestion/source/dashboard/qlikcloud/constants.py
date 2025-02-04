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
QlikCloud Constants
"""

OPEN_DOC_REQ = {
    "method": "OpenDoc",
    "handle": -1,
    "outKey": -1,
    "id": 1,
}
CREATE_SHEET_SESSION = {
    "method": "CreateSessionObject",
    "handle": 1,
    "params": [
        {
            "qInfo": {"qType": "SheetList"},
            "qAppObjectListDef": {
                "qType": "sheet",
                "qData": {
                    "title": "/qMetaDef/title",
                    "description": "/qMetaDef/description",
                    "thumbnail": "/thumbnail",
                    "cells": "/cells",
                    "rank": "/rank",
                    "columns": "/columns",
                    "rows": "/rows",
                },
            },
        }
    ],
    "outKey": -1,
    "id": 2,
}
GET_SHEET_LAYOUT = {
    "method": "GetLayout",
    "handle": 2,
    "params": [],
    "outKey": -1,
    "id": 3,
}
APP_LOADMODEL_REQ = {
    "delta": True,
    "handle": 1,
    "method": "GetObject",
    "params": ["LoadModel"],
    "id": 4,
    "jsonrpc": "2.0",
}
GET_LOADMODEL_LAYOUT = {
    "delta": True,
    "handle": 3,
    "method": "GetLayout",
    "params": [],
    "id": 5,
    "jsonrpc": "2.0",
}
