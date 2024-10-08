#!/bin/bash

source /opt/metadata_collector/venv/bin/activate

python3 /opt/metadata_collector/src/metadata_ingestion.py $1

deactivate