"""
Test helper functions
"""
from metadata_managed_apis.api.utils import clean_dag_id
from metadata_managed_apis.workflows.ingestion.common import clean_name_tag


def test_clean_dag_id():
    """
    To make sure airflow can parse it
    """
    assert clean_dag_id("hello") == "hello"
    assert clean_dag_id("hello(world)") == "hello_world_"
    assert clean_dag_id("hello-world") == "hello-world"
    assert clean_dag_id("%%&^++hello__") == "_hello__"


def test_clean_tag():
    """We can properly tag airflow DAGs"""

    assert clean_name_tag("hello") == "hello"
    assert clean_name_tag("hello(world)") == "hello(world)"
    assert clean_name_tag("service.pipeline") == "pipeline"
    assert clean_name_tag(f"service.{'a' * 200}") == "a" * 90
