"""
Airflow config
"""
import os
import socket

import airflow
from airflow.configuration import conf
from metadata_managed_apis import __version__

PLUGIN_NAME = "metadata_managed_apis"
REST_API_ENDPOINT = "/api/v1/metadata/"

# Getting Versions and Global variables
HOSTNAME = socket.gethostname()
AIRFLOW_VERSION = airflow.__version__
REST_API_PLUGIN_VERSION = __version__

# Getting configurations from airflow.cfg file
AIRFLOW_WEBSERVER_BASE_URL = conf.get("webserver", "BASE_URL")
AIRFLOW_DAGS_FOLDER = conf.get("core", "DAGS_FOLDER")
# Path to store the JSON configurations we receive via REST
DAG_GENERATED_CONFIGS = conf.get(
    "metadata_airflow_apis",
    "DAG_GENERATED_CONFIGS",
    fallback=f"{os.environ['AIRFLOW_HOME']}/dag_generated_configs",
)
