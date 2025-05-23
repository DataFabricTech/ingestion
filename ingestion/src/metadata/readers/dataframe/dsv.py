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
Generic Delimiter-Separated-Values implementation
"""
import functools
import io
from functools import singledispatchmethod
from typing import Any, Dict, Optional

from metadata.generated.schema.entity.services.connections.database.datalake.azureConfig import (
    AzureConfig,
)
from metadata.generated.schema.entity.services.connections.database.datalake.gcsConfig import (
    GCSConfig,
)
from metadata.generated.schema.entity.services.connections.database.datalake.s3Config import (
    S3Config,
)
from metadata.generated.schema.entity.services.connections.database.datalakeConnection import (
    LocalConfig,
)
from metadata.generated.schema.security.credentials.minioCredentials import (
    MinioCredentials
)
from metadata.readers.dataframe.base import DataFrameReader, FileFormatException
from metadata.readers.dataframe.common import PANDAS_ENCODINGS
from metadata.readers.dataframe.models import DatalakeColumnWrapper
from metadata.readers.file.adls import AZURE_PATH, return_azure_storage_options
from metadata.readers.models import ConfigSource
from metadata.utils.constants import CHUNKSIZE

TSV_SEPARATOR = "\t"
CSV_SEPARATOR = ","


class DSVDataFrameReader(DataFrameReader):
    """
    Manage the implementation to read DSV dataframes
    from any source based on its init client.
    """

    def __init__(
            self,
            config_source: ConfigSource,
            client: Optional[Any],
            separator: str = CSV_SEPARATOR,
    ):
        self.separator = separator
        super().__init__(config_source, client)

    def read_from_pandas(
            self, path: str, storage_options: Optional[Dict[str, Any]] = None, **kwargs
    ) -> DatalakeColumnWrapper:
        import pandas as pd  # pylint: disable=import-outside-toplevel

        chunk_list = []
        with pd.read_csv(
                path,
                sep=self.separator,
                chunksize=CHUNKSIZE,
                storage_options=storage_options,
                **kwargs
        ) as reader:
            for chunks in reader:
                chunk_list.append(chunks)

        # columns = [Column(name=col) for col in chunk_list[0].columns]
        return DatalakeColumnWrapper(dataframes=chunk_list)

    def read_from_pandas_with_raw(self, body) -> DatalakeColumnWrapper:
        import pandas as pd  # pylint: disable=import-outside-toplevel

        chunk_list = []
        with pd.read_csv(body, sep=self.separator, chunksize=CHUNKSIZE) as reader:
            for chunks in reader:
                chunk_list.append(chunks)

        # columns = [Column(name=col) for col in chunk_list[0].columns]
        return DatalakeColumnWrapper(dataframes=chunk_list)

    @singledispatchmethod
    def _read_dsv_dispatch(
            self, config_source: ConfigSource, key: str, bucket_name: str
    ) -> DatalakeColumnWrapper:
        raise FileFormatException(config_source=config_source, file_name=key)

    @_read_dsv_dispatch.register
    def _(self, _: GCSConfig, key: str, bucket_name: str) -> DatalakeColumnWrapper:
        """
        Read the CSV file from the gcs bucket and return a dataframe
        """
        path = f"gs://{bucket_name}/{key}"
        return self.read_from_pandas(path=path)

    @_read_dsv_dispatch.register
    def _(self, _: S3Config, key: str, bucket_name: str) -> DatalakeColumnWrapper:
        path = self.client.get_object(Bucket=bucket_name, Key=key)["Body"]
        return self.read_from_pandas(path=path)

    @_read_dsv_dispatch.register
    def _(self, _: MinioCredentials, key: str, bucket_name: str) -> DatalakeColumnWrapper:
        import urllib.parse  # pylint: disable=import-outside-toplevel
        key = urllib.parse.unquote_plus(key)
        storage_options = {
            "key": f"{self.config_source.accessKeyId}",
            "secret": f"{self.config_source.secretKey}",
            "client_kwargs": {"endpoint_url": f"{self.config_source.endPointURL}", "region_name": "ap-northeast-2"},}

        for encoding in PANDAS_ENCODINGS:
            try:
                return self.read_from_pandas(path=f"s3://{bucket_name}/{key}",
                                             storage_options=storage_options, encoding=encoding)
            except UnicodeDecodeError as err:
                continue
            except PermissionError as err:
                try:
                    response = self.client.get_object(Bucket=bucket_name, Key=key)
                    data = response['Body'].read()
                    return self.read_from_pandas_with_raw(io.BytesIO(data))
                except Exception as err:
                    raise err
            except Exception as err:
                raise err

        # res = self.client.head_object(Bucket=bucket_name, Key=key)
        # data.raw_data = res

    @_read_dsv_dispatch.register
    def _(self, _: AzureConfig, key: str, bucket_name: str) -> DatalakeColumnWrapper:
        storage_options = return_azure_storage_options(self.config_source)
        path = AZURE_PATH.format(
            bucket_name=bucket_name,
            account_name=self.config_source.securityConfig.accountName,
            key=key,
        )
        return self.read_from_pandas(
            path=path,
            storage_options=storage_options,
        )

    @_read_dsv_dispatch.register
    def _(  # pylint: disable=unused-argument
            self, _: LocalConfig, key: str, bucket_name: str
    ) -> DatalakeColumnWrapper:
        return self.read_from_pandas(path=key)

    def _read(self, *, key: str, bucket_name: str, **__) -> DatalakeColumnWrapper:
        return self._read_dsv_dispatch(
            self.config_source, key=key, bucket_name=bucket_name)


def get_dsv_reader_by_separator(separator: str) -> functools.partial:
    return functools.partial(DSVDataFrameReader, separator=separator)


CSVDataFrameReader = get_dsv_reader_by_separator(separator=CSV_SEPARATOR)
TSVDataFrameReader = get_dsv_reader_by_separator(separator=TSV_SEPARATOR)
