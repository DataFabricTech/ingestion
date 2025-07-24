#!/bin/bash

cd /metadata_collector

export PYTHONPATH=$PYTHONPATH:/metadata_collector/src:/metadata_collector/libs
export METADATA_COLLECTOR_CONFIG_FILE=/metadata_collector/conf/config_docker.ini

python /metadata_collector/metadata_collector_main.py "$@"