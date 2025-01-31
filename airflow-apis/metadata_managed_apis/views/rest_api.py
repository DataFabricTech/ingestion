"""
Airflow REST API definition
"""
from flask_appbuilder import BaseView as AppBuilderBaseView
from flask_appbuilder import expose as app_builder_expose
from metadata_managed_apis.api.apis_metadata import APIS_METADATA
from metadata_managed_apis.api.config import (
    AIRFLOW_VERSION,
    AIRFLOW_WEBSERVER_BASE_URL,
    REST_API_ENDPOINT,
    REST_API_PLUGIN_VERSION,
)


class RestApiView(AppBuilderBaseView):
    """
    API View which extends either flask AppBuilderBaseView or flask AdminBaseView
    """

    # '/' Endpoint where the Admin page is which allows you to view the APIs available and trigger them
    @app_builder_expose("/")
    def list(self):
        return self.render_template(
            "/rest_api/index.html",
            airflow_webserver_base_url=AIRFLOW_WEBSERVER_BASE_URL,
            rest_api_endpoint=REST_API_ENDPOINT,
            apis_metadata=APIS_METADATA,
            airflow_version=AIRFLOW_VERSION,
            rest_api_plugin_version=REST_API_PLUGIN_VERSION,
            rbac_authentication_enabled=True,
        )
