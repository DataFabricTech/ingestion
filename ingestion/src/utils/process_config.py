from pydantic import BaseSettings
import os, sys
from dotenv import load_dotenv
import configparser


class Config(BaseSettings):

    system_name = "METADATA COLLECTOR"

    crypt_key: str

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

        return cls(
            crypt_key=config_data["security"]["cryptkey"],
        )


config = Config.get_yaml()

