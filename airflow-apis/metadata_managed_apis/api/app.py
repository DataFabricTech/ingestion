from os.path import dirname
from pathlib import Path

from flask import Blueprint
from metadata_managed_apis.api.config import REST_API_ENDPOINT
from metadata_managed_apis.api.utils import import_path


def get_blueprint() -> Blueprint:
    """
    Return the blueprint after forcing the import of all routes
    :return: REST API blueprint
    """

    blueprint = Blueprint("airflow_api", __name__, url_prefix=REST_API_ENDPOINT)

    routes = Path(dirname(__file__)) / "routes"
    modules = [
        str(elem.absolute())
        for elem in routes.glob("*.py")
        if elem.is_file() and elem.stem != "__init__"
    ]

    # Force import routes to load endpoints
    for file in modules:
        module = import_path(file)
        module.get_fn(blueprint)

    return blueprint
