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

import logging
from enum import Enum
from logging.handlers import RotatingFileHandler
from typing import Union

from airflow.configuration import conf

from metadata.generated.schema.metadataIngestion.application import (
    MetadataApplicationConfig,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    MetadataWorkflowConfig,
)
from metadata.utils.logger import set_loggers_level

BASE_LOGGING_FORMAT = (
    "[%(asctime)s] %(levelname)-8s {%(name)s:%(module)s:%(lineno)d} - %(message)s"
)


class Loggers(Enum):
    API_ROUTES = "AirflowAPIRoutes"
    API = "AirflowAPI"
    OPERATIONS = "AirflowOperations"
    WORKFLOW = "AirflowWorkflow"
    UTILS = "AirflowUtils"


def build_logger(logger_name: str) -> logging.Logger:
    logger = logging.getLogger(logger_name)
    log_format = logging.Formatter(BASE_LOGGING_FORMAT)
    rotating_log_handler = RotatingFileHandler(
        f"{conf.get('logging', 'base_log_folder', fallback='')}/metadata_airflow_api.log",
        maxBytes=1000000,
        backupCount=10,
    )
    rotating_log_handler.setFormatter(log_format)
    logger.addHandler(rotating_log_handler)
    # We keep the log level as DEBUG to have all the traces in case anything fails
    # during a deployment of a DAG
    logger.setLevel(logging.DEBUG)
    return logger


def routes_logger() -> logging.Logger:
    return build_logger(Loggers.API_ROUTES.value)


def api_logger():
    return build_logger(Loggers.API.value)


def operations_logger():
    return build_logger(Loggers.OPERATIONS.value)


def workflow_logger():
    return build_logger(Loggers.WORKFLOW.value)


def utils_logger():
    return build_logger(Loggers.UTILS.value)


def set_operator_logger(
    workflow_config: Union[MetadataWorkflowConfig, MetadataApplicationConfig]
) -> None:
    """
    Handle logging for the Python Operator that
    will execute the ingestion
    """
    logging.getLogger().setLevel(logging.WARNING)
    set_loggers_level(workflow_config.workflowConfig.loggerLevel.value)
