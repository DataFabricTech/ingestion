#!/bin/bash
datetime=$(date '+%Y%m%d%H%M%S')

# get args and switch case
while getopts "aisp" opt; do
  case $opt in
    i)
      echo "ingestion diff"
      diff -aNur --exclude=.idea --exclude=.git --exclude=build --exclude=__pycache__ --exclude=openmetadata_ingestion.egg-info --exclude=generated ./ingestion /DATA/workspace/open-metadata/ingestion > ingestion-$datetime.patch
      ;;
    a)
      echo "api diff"
      diff -aNur --exclude=.idea --exclude=.git --exclude=build --exclude=__pycache__ --exclude=openmetadata_managed_apis.egg-info ./airflow-apis /DATA/workspace/open-metadata/openmetadata-airflow-apis > api-$datetime.patch
      ;;
    s)
      echo "spec diff"
      diff -aNur ./spec /DATA/workspace/open-metadata/openmetadata-spec > spec-$datetime.patch
      ;;
    p)
      echo "scripts diff"
      diff -aNur ./scripts /DATA/workspace/open-metadata/scripts > scripts-$datetime.patch
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

