FROM mysql:8.3 as mysql

FROM apache/airflow:2.7.3-python3.10
USER root
RUN curl -sS https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl -sS https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list
# Install Dependencies (listed in alphabetical order)
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get -qq update \
    && apt-get -qq install -y \
    alien \
    build-essential \
    default-libmysqlclient-dev \
    freetds-bin \
    freetds-dev \
    gcc \
    gnupg \
    libaio1 \
    libevent-dev \
    libffi-dev \
    libpq-dev \
    librdkafka-dev \
    libsasl2-dev \
    libsasl2-2 \
    libsasl2-modules \
    libsasl2-modules-gssapi-mit \
    libssl-dev \
    libxml2 \
    libkrb5-dev \
    openjdk-11-jre \
    openssl \
    postgresql \
    postgresql-contrib \
    tdsodbc \
    unixodbc \
    unixodbc-dev \
    unzip \
    git \
    wget --no-install-recommends \
    # Accept MSSQL ODBC License
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=mysql /usr/bin/mysqldump /usr/bin/mysqldump

RUN if [[ $(uname -m) == "arm64" || $(uname -m) == "aarch64" ]]; \
 then \
 wget -q https://download.oracle.com/otn_software/linux/instantclient/191000/instantclient-basic-linux.arm64-19.10.0.0.0dbru.zip -O /oracle-instantclient.zip && \
 unzip -qq -d /instantclient -j /oracle-instantclient.zip && rm -f /oracle-instantclient.zip; \
 else \
 wget -q https://download.oracle.com/otn_software/linux/instantclient/1917000/instantclient-basic-linux.x64-19.17.0.0.0dbru.zip -O /oracle-instantclient.zip && \
 unzip -qq -d /instantclient -j /oracle-instantclient.zip && rm -f /oracle-instantclient.zip; \
 fi

ENV LD_LIBRARY_PATH=/instantclient

# Security patches for base image
# monitor no fixed version for
#    https://security.snyk.io/vuln/SNYK-DEBIAN11-LIBTASN16-3061097
#    https://security.snyk.io/vuln/SNYK-DEBIAN11-MARIADB105-2940589
#    https://security.snyk.io/vuln/SNYK-DEBIAN11-BIND9-3027852
#    https://security.snyk.io/vuln/SNYK-DEBIAN11-EXPAT-3023031 we are already installed the latest
RUN echo "deb http://deb.debian.org/debian bullseye-backports main" > /etc/apt/sources.list.d/backports.list
RUN apt-get -qq update \
    && apt-get -qq install -t bullseye-backports -y \
    curl \
    libpcre2-8-0 \
    postgresql-common \
    expat \
    bind9 \
    make \
    procps \
    vim \
    && rm -rf /var/lib/apt/lists/*

# Required for Starting Ingestion Container in Docker Compose
COPY --chown=airflow:0 --chmod=775 ingestion/ingestion_dependency.sh /opt/airflow
# Required for Ingesting Sample Data
COPY --chown=airflow:0 ingestion/examples/sample_data /home/airflow/ingestion/examples/sample_data
# Required for Airflow DAGs of Sample Data
COPY --chown=airflow:0 ingestion/examples/airflow/dags /opt/airflow/dags

RUN mkdir -p /home/airflow/workspace
COPY --chown=airflow:0 . /home/airflow/workspace/datafabric

USER airflow
# Argument to provide for Ingestion Dependencies to install. Defaults to all
ARG INGESTION_DEPENDENCY="all"

# Disable pip cache dir
# https://pip.pypa.io/en/stable/topics/caching/#avoiding-caching
ENV PIP_NO_CACHE_DIR=1
# Make pip silent
ENV PIP_QUIET=1
ARG RI_VERSION="1.4.0.1"
RUN pip install --upgrade pip

RUN echo "Image built for $(uname -m)"
RUN if [[ $(uname -m) != "aarch64" ]]; \
 then \
 pip install "ibm-db-sa~=0.4"; \
 fi

RUN pip install -v /home/airflow/workspace/datafabric/airflow-apis/
RUN pip install -v /home/airflow/workspace/datafabric/ingestion[all]/
RUN pip install -v /home/airflow/workspace/datafabric/ingestion/

# bump python-daemon for https://github.com/apache/airflow/pull/29916
RUN pip install "python-daemon>=3.0.0"

# remove all airflow providers except for docker and cncf kubernetes
#RUN pip freeze | grep "apache-airflow-providers" | grep --invert-match -E "docker|http|cncf" | xargs pip uninstall -y
# Uninstalling psycopg2-binary and installing psycopg2 instead 
# because the psycopg2-binary generates a architecture specific error 
# while authenticating connection with the airflow, psycopg2 solves this error
RUN pip uninstall psycopg2-binary -y
RUN pip install psycopg2 mysqlclient==2.1.1
# Make required folders for airflow-apis
RUN mkdir -p /opt/airflow/dag_generated_configs
# This is required as it's responsible to create airflow.cfg file
RUN airflow db init && rm -f /opt/airflow/airflow.db

USER root

RUN apt-get update && apt-get install -t bullseye-backports -y \
    openssh-server default-jdk \
    && rm -rf /var/lib/apt/lists/*

RUN echo "root:Ahqlwps12#$" | chpasswd && \
    echo "airflow:Ahqlwps12#$" | chpasswd && \
    mkdir -p /var/run/sshd /airflow/.ssh /root/.ssh && \
    echo "airflow   ALL=(ALL)   ALL" >> /etc/sudoers && \
    echo "alias vi=vim" >> /home/airflow/.bashrc

RUN sed -ri 's/^#?PermitRootLogin\s+.*/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN sed -ri 's/UsePAM yes/#UsePAM yes/g' /etc/ssh/sshd_config
RUN sed -ri 's/#UseDNS no/UseDNS no/g' /etc/ssh/sshd_config
RUN sed -ri 's/#ClientAliveInterval 0/ClientAliveInterval 100/g' /etc/ssh/sshd_config
RUN sed -ri 's/#ClientAliveCountMax 3/ClientAliveCountMax 3/g' /etc/ssh/sshd_config
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd

USER airflow
