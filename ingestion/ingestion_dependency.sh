#!/usr/bin/env bash

DB_HOST=${DB_HOST:-mysql}
DB_PORT=${DB_PORT:-3306}

AIRFLOW_DB=${AIRFLOW_DB:-airflow_db}
DB_USER=${DB_USER:-airflow_user}
DB_SCHEME=${DB_SCHEME:-mysql+pymysql}
DB_PASSWORD=${DB_PASSWORD:-airflow_pass}
DB_PROPERTIES=${DB_PROPERTIES:-""}

AIRFLOW_ADMIN_USER=${AIRFLOW_ADMIN_USER:-admin}
AIRFLOW_ADMIN_PASSWORD=${AIRFLOW_ADMIN_PASSWORD:-admin}

DB_USER_VAR=`echo "${DB_USER}" | python3 -c "import urllib.parse; encoded_user = urllib.parse.quote(input()); print(encoded_user)"`
DB_PASSWORD_VAR=`echo "${DB_PASSWORD}" | python3 -c "import urllib.parse; encoded_user = urllib.parse.quote(input()); print(encoded_user)"`

DB_CONN=`echo -n "${DB_SCHEME}://${DB_USER_VAR}:${DB_PASSWORD_VAR}@${DB_HOST}:${DB_PORT}/${AIRFLOW_DB}${DB_PROPERTIES}"`

# Set the default necessary auth_backend information
export AIRFLOW__API__AUTH_BACKEND=${AIRFLOW__API__AUTH_BACKENDS:-"airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session"}

# Use the default airflow env var or the one we set from OM properties
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN:-$DB_CONN}

airflow db init

airflow users create \
    --username ${AIRFLOW_ADMIN_USER} \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org \
    --password ${AIRFLOW_ADMIN_PASSWORD}

(sleep 5; airflow db migrate)
(sleep 5; airflow db migrate)

# we need to this in case the container is restarted and the scheduler exited without tidying up its lock file
rm -f /opt/airflow/airflow-webserver-monitor.pid
airflow webserver --port 8080 -D &
airflow scheduler
