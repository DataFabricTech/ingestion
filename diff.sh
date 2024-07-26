#!/bin/bash
datetime=$(date '+%Y%m%d%H%M%S')

OPEN_METADATA_PATH=/Users/jblim/Workspace/open-metadata

# get args and switch case
while getopts "aisp" opt; do
  case $opt in
    i)
      echo "ingestion diff"
      diff -aNur --exclude=.idea --exclude=.git --exclude=build --exclude=__pycache__ --exclude=openmetadata_ingestion.egg-info --exclude=generated ./ingestion ${OPEN_METADATA_PATH}/ingestion > ingestion-$datetime.patch
      ;;
    a)
      echo "api diff"
      diff -aNur --exclude=.idea --exclude=.git --exclude=build --exclude=__pycache__ --exclude=openmetadata_managed_apis.egg-info ./airflow-apis ${OPEN_METADATA_PATH}/openmetadata-airflow-apis > api-$datetime.patch
      ;;
    s)
      echo "spec diff"
      diff -aNur --exclude='openmetadata-spec.iml' --exclude="target" \
          ./spec ${OPEN_METADATA_PATH}/openmetadata-spec > spec-$datetime.patch
      ;;
    p)
      echo "scripts diff"
      diff -aNur ./scripts ${OPEN_METADATA_PATH}/scripts > scripts-$datetime.patch
      ;;
    \?)
      echo "사용법 $0 [-i] [-a] [-s] [-p]"
      echo "-i = ingestion"
      echo "-a = airflow-apis"
      echo "-s = spec"
      echo "-p = scripts"
      exit 1
      ;;
  esac
done

