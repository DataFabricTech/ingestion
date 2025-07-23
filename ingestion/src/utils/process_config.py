from pydantic import BaseSettings
import os, sys
from dotenv import load_dotenv
import configparser


class Config(BaseSettings):

    system_name = "METADATA COLLECTOR"

    crypt_key: str

    sink_host: str
    sink_port: int

    minio_url: str

    catalog_api_base_url: str
    get_meta_system_info_api: str
    set_meta_ingestion_status_api: str

    @classmethod
    def get_yaml(cls):
        # .env 파일 로드
        load_dotenv()

        config_file = os.getenv("METADATA_COLLECTOR_CONFIG_FILE")

        if config_file is None:
            print("env 에 프로세스 환경설정 파일을 설정하세요 [예: METADATA_COLLECTOR_CONFIG_FILE = config.ini]")
            sys.exit()

        config_data = configparser.ConfigParser()
        config_data.read(config_file)

        catalog_api_config = config_data["catalog_api"]
        sink_host = catalog_api_config["host"]
        sink_port = catalog_api_config["port"]

        data_catalog_minio_config = config_data["minio"]
        minio_url = data_catalog_minio_config["url"]

        catalog_api_base_url = f"http://{catalog_api_config['host']}:{catalog_api_config['port']}{catalog_api_config['prefix']}"
        get_meta_system_info_api = catalog_api_config['get_meta_system_info_api']
        set_meta_ingestion_status_api = catalog_api_config['set_meta_ingestion_status_api']


        return cls(
            crypt_key=config_data["security"]["cryptkey"],
            catalog_api_base_url=catalog_api_base_url,
            get_meta_system_info_api=get_meta_system_info_api,
            set_meta_ingestion_status_api = set_meta_ingestion_status_api,
            sink_host = sink_host,
            sink_port = sink_port,
            minio_url=minio_url
        )


config = Config.get_yaml()

