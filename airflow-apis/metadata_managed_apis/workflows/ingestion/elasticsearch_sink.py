# Copyright 2024 Mobigen;
# Licensed under the Apache License, Version 2.0 (the "License");
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
Build the elasticsearch sink
"""
from metadata.generated.schema.entity.services.connections.metadata.metadataConnection import (
    MetadataConnection,
)
from metadata.generated.schema.entity.services.ingestionPipelines.ingestionPipeline import (
    IngestionPipeline,
)
from metadata.generated.schema.metadataIngestion.workflow import Sink
from metadata.generated.schema.type.basic import ComponentConfig
from metadata.utils.constants import ES_SOURCE_IGNORE_KEYS, ES_SOURCE_TO_ES_OBJ_ARGS


def build_elasticsearch_sink(
    metadata_service_connection: MetadataConnection,
    ingestion_pipeline: IngestionPipeline,
) -> Sink:
    """
    Build the elasticsearch sink given the OM service and
    the ingestion pipeline.

    Note that we need to map the JSON Schema properties names
    to the arguments required by the elasticsearch sink in the
    Python side.
    """

    elasticsearch_service_config_dict = (
        metadata_service_connection.elasticsSearch.config.dict()
    )

    elasticsearch_source_config_dict = {
        ES_SOURCE_TO_ES_OBJ_ARGS[key]: value
        for key, value in ingestion_pipeline.sourceConfig.config.dict().items()
        if value and key not in ES_SOURCE_IGNORE_KEYS
    }

    return Sink(
        type="elasticsearch",
        config=ComponentConfig(
            **elasticsearch_service_config_dict,
            **elasticsearch_source_config_dict,
        ),
    )
