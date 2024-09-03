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
Module containing Minio Client
"""
from typing import Any, Optional

import boto3
from boto3 import Session
# from pydantic import BaseModel

from metadata.generated.schema.security.credentials.minioCredentials import MinioCredentials
from metadata.ingestion.models.custom_pydantic import CustomSecretStr
from metadata.utils.logger import utils_logger

logger = utils_logger()


class MinioClient:
    """
    MinioClient creates based on MinioCredentials.
    """

    def __init__(self, config: "MinioCredentials"):
        self.config = (
            config
            if isinstance(config, MinioCredentials)
            else (MinioCredentials.parse_obj(config) if config else config)
        )

    @staticmethod
    def _get_session(
        access_key_id: Optional[str],
        secret_access_key: Optional[CustomSecretStr],
        session_token: Optional[str],
        region: str,
    ) -> Session:
        """
        The only required param for boto3 is the region.
        The rest of credentials will have fallback strategies based on
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials
        """
        return Session(
            aws_access_key_id=access_key_id,
            aws_secret_access_key=(secret_access_key.get_secret_value() if secret_access_key else None),
            aws_session_token=session_token,
            region_name=region,
        )

    def create_session(self) -> Session:
        return MinioClient._get_session(
            self.config.accessKeyId,
            self.config.secretKey,
            self.config.sessionToken,
            self.config.region,
        )

    def get_client(self) -> Any:
        # initialize the client depending on the AWSCredentials passed
        if self.config is not None:
            logger.info(f"Getting Minio client")
            session = self.create_session()
            if self.config.endPointURL is not None:
                return session.client(
                    service_name="s3", endpoint_url=self.config.endPointURL
                )
            return session.client(service_name="s3")

        logger.info(f"Getting Minio default client")
        # initialized with the credentials loaded from running machine
        return boto3.client("s3")