from pydantic import BaseSettings
import yaml
import os, sys
from dotenv import load_dotenv


class Config(BaseSettings):

    system_name = "METADATA COLLECTOR"

    crypt_key: str

    @classmethod
    def get_yaml(cls):
        # .env 파일 로드
        load_dotenv()

        config_file = os.getenv("METADATA_COLLECTOR_CONFIG_FILE")

        if config_file is None:
            print("env 에 프로세스 환경설정 파일을 설정하세요 [예: METADATA_COLLECTOR_CONFIG_FILE = config.yaml]")
            sys.exit()

        with open(config_file, "r") as f:
            config_data = yaml.safe_load(f)

        metadata_collector = config_data.get("metadata_collector")

        crypt = metadata_collector["crypt"]

        return cls(
            crypt_key=crypt["cryptKey"],
        )


config = Config.get_yaml()

