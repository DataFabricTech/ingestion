#  Copyright 2021 Collate
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""
Python Dependencies
"""

from typing import Dict, List, Set

from setuptools import setup

# Add here versions required for multiple plugins
VERSIONS = {
    "adlfs": "adlfs==2023.12.0",
    "airflow": "apache-airflow==2.7.3",
    "avro": "avro>=1.11.3,<1.12",
    "azure-identity": "azure-identity~=1.12",
    "azure-storage-blob": "azure-storage-blob~=12.14",
    "boto3": "boto3==1.29.4",  # No need to add botocore separately. It's a dep from boto3
    "databricks-sdk": "databricks-sdk>=0.18.0,<0.20.0",
    "elasticsearch8": "elasticsearch8~=8.9.0",
    "fsspec": "fsspec==2023.12.1",
    "gcsfs": "gcsfs==2023.12.1",
    "geoalchemy2": "GeoAlchemy2~=0.12",
    "giturlparse": "giturlparse",
    "google-cloud-storage": "google-cloud-storage==1.43.0",
    "great-expectations": "great-expectations>=0.18.0,<0.18.14",
    "grpc-tools": "grpcio-tools>=1.47.2",
    "lkml": "lkml~=1.3",
    "looker-sdk": "looker-sdk>=22.20.0",
    "lxml": "lxml==5.3.0",
    "mongo": "pymongo~=4.3",
    "msal": "msal~=1.2",
    "neo4j": "neo4j~=5.3.0",
    "nltk": "nltk==3.9.1",
    "transformers": "transformers==4.46.3",
    "openpyxl": "openpyxl~=3.1.3",
    "packaging": "packaging==21.3",
    "pandas": "pandas~=2.0.0",
    "pyarrow": "pyarrow~=14.0",
    "pydantic": "pydantic~=1.10",
    "pydomo": "pydomo~=0.3",
    "pyhive": "pyhive[hive_pure_sasl]~=0.7",
    "pymysql": "pymysql>=1.0.2",
    "pyodbc": "pyodbc>=4.0.35,<5",
    "python-docx": "python-docx==1.1.2",
    "pyhwp": "pyhwp==0.1b15",
    "redshift": "sqlalchemy-redshift==0.8.12",
    "s3fs": "s3fs==2023.12.1",
    "scikit-learn": "scikit-learn~=1.0",  # Python 3.7 only goes up to 1.0.2
    "snowflake": "snowflake-sqlalchemy~=1.4",
    "spacy": "spacy==3.5.0",
    "sqlalchemy-databricks": "sqlalchemy-databricks~=0.1",
    "tableau": "tableau-api-lib~=0.1",
    "tika": "tika==2.6.0",
    "trino": "trino[sqlalchemy]",
    "torch": "torch==2.5.1",
    "validators": "validators~=0.22.0",
}

COMMONS = {
    "datalake": {
        VERSIONS["avro"],
        VERSIONS["boto3"],
        VERSIONS["pandas"],
        VERSIONS["pyarrow"],
        # python-snappy does not work well on 3.11 https://github.com/aio-libs/aiokafka/discussions/931
        # Using this as an alternative
        "cramjam~=2.7",
    },
    "hive": {
        "presto-types-parser>=0.0.2",
        VERSIONS["pyhive"],
    },
    "kafka": {
        VERSIONS["avro"],
        "confluent_kafka==2.1.1",
        "fastavro>=1.2.0",
        # Due to https://github.com/grpc/grpc/issues/30843#issuecomment-1303816925
        # use >= v1.47.2 https://github.com/grpc/grpc/blob/v1.47.2/tools/distrib/python/grpcio_tools/grpc_version.py#L17
        VERSIONS[
            "grpc-tools"
        ],  # grpcio-tools already depends on grpcio. No need to add separately
        "protobuf",
    },
    "postgres": {
        VERSIONS["pymysql"],
        "psycopg2-binary",
        VERSIONS["geoalchemy2"],
        VERSIONS["packaging"],
    },  # Adding as Postgres SQL & GreenPlum are using common packages.
}


base_requirements = {
    "antlr4-python3-runtime==4.9.2",
    VERSIONS["azure-identity"],
    "azure-keyvault-secrets",  # Azure Key Vault SM
    VERSIONS["boto3"],  # Required in base for the secrets manager
    "cached-property==1.5.2",  # LineageParser
    "chardet==4.0.0",  # Used in the profiler
    "cryptography>=42.0.0",
    "email-validator>=1.0.3",  # For the pydantic generated models for Email
    "importlib-metadata>=4.13.0",  # From airflow constraints
    "Jinja2>=2.11.3",
    "jsonpatch<2.0, >=1.24",
    "memory-profiler",
    "mypy_extensions>=0.4.3",
    VERSIONS["pydantic"],
    VERSIONS["pymysql"],
    "python-dateutil>=2.8.1",
    "PyYAML~=6.0",
    "requests>=2.23",
    "requests-aws4auth~=1.1",  # Only depends on requests as external package. Leaving as base.
    "sqlalchemy>=1.4.0,<2",
    "collate-sqllineage~=1.4.0",
    "tabulate==0.9.0",
    "typing-inspect",
    "packaging",  # For version parsing
    "google-auth",
    "nltk==3.9.1",
    "transformers==4.46.3",
    "torch==2.5.1",
    "python-docx==1.1.2",
    "lxml==5.3.0",
    "tika==2.6.0",
    "pyhwp==0.1b15",
}


plugins: Dict[str, Set[str]] = {
    "airflow": {
        VERSIONS["airflow"],
        "attrs",
    },  # Same as ingestion container. For development.
    "amundsen": {VERSIONS["neo4j"]},
    "athena": {"pyathena==3.0.8"},
    "atlas": {},
    "azuresql": {VERSIONS["pyodbc"]},
    "azure-sso": {VERSIONS["msal"]},
    "backup": {VERSIONS["boto3"], VERSIONS["azure-identity"], "azure-storage-blob"},
    "bigquery": {
        "cachetools",
        "google-cloud-datacatalog>=3.6.2",
        "google-cloud-logging",
        VERSIONS["pyarrow"],
        "sqlalchemy-bigquery>=1.2.2",
    },
    "bigtable": {"google-cloud-bigtable>=2.0.0", VERSIONS["pandas"]},
    "clickhouse": {"clickhouse-driver~=0.2", "clickhouse-sqlalchemy~=0.2"},
    "dagster": {
        VERSIONS["pymysql"],
        "psycopg2-binary",
        VERSIONS["geoalchemy2"],
        "dagster_graphql~=1.1",
    },
    "dbt": {
        "google-cloud",
        VERSIONS["boto3"],
        VERSIONS["google-cloud-storage"],
        "dbt-artifacts-parser",
        VERSIONS["azure-storage-blob"],
        VERSIONS["azure-identity"],
    },
    "db2": {"ibm-db-sa~=0.3"},
    "db2-ibmi": {"sqlalchemy-ibmi~=0.9.3"},
    "databricks": {
        VERSIONS["sqlalchemy-databricks"],
        VERSIONS["databricks-sdk"],
        "ndg-httpsclient~=0.5.1",
        "pyOpenSSL~=24.1.0",
        "pyasn1~=0.6.0",
    },
    "datalake-azure": {
        VERSIONS["azure-storage-blob"],
        VERSIONS["azure-identity"],
        VERSIONS["adlfs"],  # Python 3.7 does only support up to 2022.2.0
        *COMMONS["datalake"],
    },
    "datalake-gcs": {
        VERSIONS["google-cloud-storage"],
        VERSIONS["gcsfs"],
        *COMMONS["datalake"],
    },
    "datalake-s3": {
        # requires aiobotocore
        # https://github.com/fsspec/s3fs/blob/9bf99f763edaf7026318e150c4bd3a8d18bb3a00/requirements.txt#L1
        # however, the latest version of `s3fs` conflicts its `aiobotocore` dep with `boto3`'s dep on `botocore`.
        # Leaving this marked to the automatic resolution to speed up installation.
        "s3fs==2023.12.1",
        *COMMONS["datalake"],
    },
    "deltalake": {"delta-spark<=2.3.0"},
    "domo": {VERSIONS["pydomo"]},
    "doris": {"pydoris==1.0.2"},
    "druid": {"pydruid>=0.6.5"},
    "dynamodb": {VERSIONS["boto3"]},
    "elasticsearch": {
        VERSIONS["elasticsearch8"],
    },  # also requires requests-aws4auth which is in base
    "glue": {VERSIONS["boto3"]},
    "great-expectations": {VERSIONS["great-expectations"]},
    "greenplum": {*COMMONS["postgres"]},
    "hive": {
        *COMMONS["hive"],
        "thrift>=0.13,<1",
        # Replacing sasl with pure-sasl based on https://github.com/cloudera/python-sasl/issues/30 for py 3.11
        "pure-sasl",
        "thrift-sasl~=0.4",
        "impyla~=0.18.0",
    },
    "iceberg": {
        "pyiceberg<1",
        # Forcing the version of a few packages so it plays nicely with other requirements.
        VERSIONS["pydantic"],
        VERSIONS["adlfs"],
        VERSIONS["gcsfs"],
        VERSIONS["pyarrow"],
    },
    "impala": {
        "presto-types-parser>=0.0.2",
        "impyla[kerberos]~=0.18.0",
        "thrift>=0.13,<1",
        "pure-sasl",
        "thrift-sasl~=0.4",
    },
    "kafka": {*COMMONS["kafka"]},
    "kafkaconnect": {"kafka-connect-py==0.10.11"},
    "kinesis": {VERSIONS["boto3"]},
    "looker": {
        VERSIONS["looker-sdk"],
        VERSIONS["lkml"],
        "gitpython~=3.1.34",
        VERSIONS["giturlparse"],
    },
    "mlflow": {"mlflow-skinny==2.14.3", "alembic~=1.10.2"},
    "mongo": {VERSIONS["mongo"], VERSIONS["pandas"]},
    "couchbase": {"couchbase~=4.1"},
    "mssql": {"sqlalchemy-pytds~=0.3"},
    "mssql-odbc": {VERSIONS["pyodbc"]},
    "mysql": {VERSIONS["pymysql"]},
    "nifi": {},  # uses requests
    "openlineage": {*COMMONS["kafka"]},
    "oracle": {"cx_Oracle>=8.3.0,<9", "oracledb~=1.2"},
    "pgspider": {"psycopg2-binary", "sqlalchemy-pgspider"},
    "pinotdb": {"pinotdb~=0.3"},
    "postgres": {*COMMONS["postgres"]},
    "powerbi": {
        VERSIONS["msal"],
        VERSIONS["boto3"],
        VERSIONS["google-cloud-storage"],
        VERSIONS["azure-storage-blob"],
        VERSIONS["azure-identity"],
    },
    "qliksense": {"websocket-client~=1.6.1"},
    "presto": {*COMMONS["hive"]},
    "pymssql": {"pymssql~=2.2.0"},
    "quicksight": {VERSIONS["boto3"]},
    "redash": {VERSIONS["packaging"]},
    "redpanda": {*COMMONS["kafka"]},
    "redshift": {
        # Going higher has memory and performance issues
        VERSIONS["redshift"],
        "psycopg2-binary",
        VERSIONS["geoalchemy2"],
    },
    "sagemaker": {VERSIONS["boto3"]},
    "salesforce": {"simple_salesforce==1.11.4"},
    "sample-data": {VERSIONS["avro"], VERSIONS["grpc-tools"]},
    "sap-hana": {"hdbcli", "sqlalchemy-hana"},
    "sas": {},
    "singlestore": {VERSIONS["pymysql"]},
    "sklearn": {VERSIONS["scikit-learn"]},
    "snowflake": {VERSIONS["snowflake"]},
    "superset": {},  # uses requests
    "tableau": {VERSIONS["tableau"], VERSIONS["validators"], VERSIONS["packaging"]},
    "trino": {VERSIONS["trino"]},
    "vertica": {"sqlalchemy-vertica[vertica-python]>=0.0.5"},
    "pii-processor": {
        VERSIONS["spacy"],
        VERSIONS["pandas"],
        "presidio-analyzer==2.2.32",
    },
    # jblim : MinIO 저장소 데이터 가상화를 위해 추가
    "fsspec": {VERSIONS["fsspec"]},
    "s3fs": {VERSIONS["s3fs"]},
    "openpyxl": {VERSIONS["openpyxl"]},
}

dev = {
    "black==22.3.0",
    "datamodel-code-generator==0.24.2",
    "boto3-stubs[essential]==1.35.1",
    "isort",
    "pre-commit",
    "pycln",
    "pylint~=3.0.0",
    # For publishing
    "twine",
    "build",
    *plugins["sample-data"],
}


test = {
    # Install Airflow as it's not part of `all` plugin
    VERSIONS["airflow"],
    "boto3-stubs[boto3]",
    "coverage",
    # Install GE because it's not in the `all` plugin
    VERSIONS["great-expectations"],
    "moto==4.0.8",
    "pytest==7.0.0",
    "pytest-cov",
    "pytest-order",
    # install dbt dependency
    "dbt-artifacts-parser",
    VERSIONS["sqlalchemy-databricks"],
    VERSIONS["databricks-sdk"],
    VERSIONS["scikit-learn"],
    VERSIONS["pyarrow"],
    VERSIONS["trino"],
    VERSIONS["spacy"],
    VERSIONS["pydomo"],
    VERSIONS["looker-sdk"],
    VERSIONS["lkml"],
    VERSIONS["tableau"],
    VERSIONS["pyhive"],
    VERSIONS["mongo"],
    VERSIONS["redshift"],
    VERSIONS["snowflake"],
    VERSIONS["elasticsearch8"],
    VERSIONS["giturlparse"],
    VERSIONS["avro"],  # Sample Data
    VERSIONS["grpc-tools"],
    "testcontainers==3.7.1;python_version<'3.9'",
    "testcontainers==4.4.0;python_version>='3.9'",
    "minio==7.2.5",
    *plugins["mlflow"],
    *plugins["datalake-s3"],
    "requests==2.31.0",
}

e2e_test = {
    # playwright dependencies
    "pytest-playwright",
    "pytest-base-url",
}

extended_testing = {
    "Faker",  # For Sample Data Generation
}


def filter_requirements(filtered: Set[str]) -> List[str]:
    """Filter out requirements from base_requirements"""
    return list(
        base_requirements.union(
            *[
                requirements
                for plugin, requirements in plugins.items()
                if plugin not in filtered
            ]
        )
    )


setup(
    install_requires=list(base_requirements),
    extras_require={
        "base": list(base_requirements),
        "dev": list(dev),
        "test": list(test),
        "e2e_test": list(e2e_test),
        "extended_testing": list(extended_testing),
        "data-insight": list(plugins["elasticsearch"]),
        **{plugin: list(dependencies) for (plugin, dependencies) in plugins.items()},
        "all": filter_requirements({"airflow", "db2", "great-expectations"}),
        "slim": filter_requirements(
            {"airflow", "db2", "great-expectations", "deltalake", "sklearn"}
        ),
    },
)
