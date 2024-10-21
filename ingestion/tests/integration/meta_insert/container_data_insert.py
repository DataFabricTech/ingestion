import base64
import json
import os
from collections import Counter
from pathlib import Path

import requests
from pydantic import BaseModel, Field


# Base64로 비밀번호 인코딩 함수
def encode_password(password):
    # 비밀번호를 UTF-8로 인코딩한 후 Base64로 변환
    encoded_bytes = base64.b64encode(password.encode('utf-8'))
    # Base64 인코딩된 결과를 문자열로 변환하여 반환
    encoded_str = encoded_bytes.decode('utf-8')
    return encoded_str


class ServerInfo(BaseModel):
    host: str = Field(
        '192.168.109.254',
        description="The host address for the server",
    )
    port: int = Field(
        8080,
        description="The port number for the server",
    ),
    id: str = Field(
        "admin",
        description="The user ID for login",
    )
    password: str = Field(
        "admin",
        description="The password for login",
    )
    login_path: str = Field(
        '/api/v1/login',
        description='The path to the login endpoint',
    )


class DataInsert:
    def __init__(self, server_info: ServerInfo):
        self.parent_container_id = None
        self.conf = server_info
        self.conf.password = encode_password(server_info.password)
        self.session = None
        self.token = None

    def login(self):
        # 세션 생성 (쿠키 관리)
        self.session = requests.Session()

        login_url = f'http://{self.conf.host}:{self.conf.port}{self.conf.login_path}'
        login_data = {
            'email': self.conf.id,
            'password': self.conf.password,
        }
        headers = {
            'Content-Type': 'application/json',
        }
        # 로그인 요청 (POST 방식)
        response = self.session.post(login_url, headers=headers, json=login_data)

        # 로그인 성공 여부 확인
        if response.status_code == 200:
            token = response.json().get("accessToken")
            print("Login successful! : Token:", token)
            self.token = token

    def insert_parent(self):
        # REST API 통신 (로그인 후의 인증이 필요한 요청)
        api_url = f'http://{self.conf.host}:{self.conf.port}/api/v1/containers'
        create_data = {
            "name": "AITrainingData",
            "displayName": "AITrainingData",
            "description": "TTA AITrainingData",
            "service": "MinIO",
            "parent": {
                "id": "7d05752e-dcde-4d64-ab9e-fcda02165f90",
                "type": "container"
            },
            "prefix": "/AITrainingData",
            "fullPath": "s3://datafabric/AITrainingData",
            "tags": [
                {
                    "tagFQN": "ovp_category.미분류"
                }
            ]
        }
        headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }
        response = self.session.put(api_url, headers=headers, json=create_data)

        self.parent_container_id = response.json().get("id")
        print("Parent Container ID:", self.parent_container_id)

    def insert_child(self, jsonData):
        api_url = f'http://{self.conf.host}:{self.conf.port}/api/v1/containers'
        insert_count = 0
        for path, counts in jsonData.items():
            description = ""
            for name, count in counts.items():
                description += f"Class: {name}, Count: {count}"

            # 파일 이름만 추출
            file_name = Path(path).name
            prefix = f'/AITrainingData/{file_name.split(".")[0]}'
            fullPath = f's3://datafabric{prefix}'

            insert_count = insert_count + 1
            if insert_count >= 100:
                break

            create_data = {
                "name": file_name.split(".")[0],
                "displayName": file_name.split(".")[0],
                "description": description,
                "service": "MinIO",
                "parent": {
                    "id": self.parent_container_id,
                    "type": "container"
                },
                "prefix": prefix,
                "fullPath": fullPath,
                "tags": [
                    {
                        "tagFQN": "ovp_category.미분류"
                    }
                ]
            }
            headers = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json'
            }
            response = self.session.put(api_url, headers=headers, json=create_data)
            if response.status_code == 200 or response.status_code == 201:
                print("Success Child Container ID:", response.json().get("id"))


class JsonParse:
    def __init__(self):
        self.insert_data = {}

    def parse(self, dir_path):
        # 디렉토리 내 모든 JSON 파일들을 순차적으로 읽고 파싱
        for filename in os.listdir(dir_path):
            if filename.endswith('.json'):  # 확장자가 .json인 파일만 처리
                file_path = os.path.join(dir_path, filename)

                # 파일을 열고 JSON 데이터를 파싱
                with open(file_path, 'r', encoding='utf-8') as file:
                    try:
                        data = json.load(file)

                        # name 값들을 리스트로 추출 (비어있거나 name이 없는 항목은 제외)
                        names = [image_data["classification"]["class"]
                                 for image_data in data["annotations"]["image"]
                                 if "class" in image_data["classification"] and image_data["classification"]["class"]]

                        # 각 name 값의 빈도를 카운트
                        name_counts = Counter(names)
                        self.insert_data[file_path] = dict(name_counts)

                        # 출력
                        print(f"{file_path} - Class Counts : ")
                        for name, count in name_counts.items():
                            print(f"  {name}: {count}")

                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON in file {filename}: {e}")


if __name__ == "__main__":
    conf = ServerInfo(
        host="192.168.109.254",
        port=31751,
        id="admin",
        password="admin",
        login_path="/api/v1/users/login",
    )
    data_insert = DataInsert(conf)
    data_insert.login()
    data_insert.insert_parent()

    jsonParser = JsonParse()
    jsonParser.parse("json")

    data_insert.insert_child(jsonParser.insert_data)
