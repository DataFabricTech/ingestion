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
s3 utils module
"""
from botocore.exceptions import ClientError
from itertools import product

import traceback
import unicodedata
from typing import Iterable

from metadata.utils.logger import utils_logger
from metadata.ingestion.source.storage.storage_service import KEY_SEPARATOR

logger = utils_logger()


def list_s3_objects(client, **kwargs) -> Iterable:
    """
    Method to get list of s3 objects using pagination
    """
    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(**kwargs):
            yield from page.get("Contents", [])
    except Exception as exc:
        logger.debug(traceback.format_exc())
        logger.warning(f"Unexpected exception to yield s3 object: {exc}")


def get_normalized_key(client, bucket_name, key):

    # key에 폴더가 있을 수 있기 때문에 모든 경우를 다 확인한다.
    # 1. 경로 나누기 (폴더 ..n , 파일)
    parts = key.split(KEY_SEPARATOR)

    # 2. 모든 경로를 forms 로 변환해서 리스트 만들기
    forms = ['NFC', 'NFD']
    normalized_parts_list = [
        [unicodedata.normalize(form, part) for form in forms]
        for part in parts
    ]

    # 3. 모든 조합으로 존재 확인
    for combo in product(*normalized_parts_list):
        normalized_key = '/'.join(combo)

        try:
            client.head_object(Bucket=bucket_name, Key=normalized_key)
            logger.debug(f"Found: {normalized_key}")
            return normalized_key
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                continue
            else:
                raise

    logger.debug(f"Not found: {key}")
    raise FileNotFoundError(f"Not found from minio: {key}")

