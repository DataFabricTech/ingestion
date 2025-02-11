#!/bin/bash

source /opt/metadata_collector/venv/bin/activate

python3 /opt/metadata_collector/metadata_collector_main.py $1

deactivate