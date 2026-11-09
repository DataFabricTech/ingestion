# Open VDAP Ingestion

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![Airflow](https://img.shields.io/badge/Airflow-plugin-017CEE)

## 개요

데이터패브릭 기술을 적용한 서비스 명칭은 **Open VDAP**이며, 본 저장소는 Open VDAP을 구성하는 요소 중 **ingestion**입니다.

연결된 저장소에서 **메타데이터를 수집**합니다. Airflow 위에서 동작하며 Open VDAP 서버의 수집 요청에 따라 실행됩니다. 관계형 데이터베이스와 객체 저장소를 대상으로 하고, 비정형 파일에서는 파일 속성과 내용 요약까지 추출합니다.

## 기능

- **관계형 데이터베이스 수집** — 테이블·뷰·컬럼 구조와 설명, 분류 정보를 수집합니다.
- **상용 DB 커넥터** — Altibase, Tibero 커넥터를 제공합니다.
- **객체 저장소 수집** — 버킷·객체 계층을 컨테이너 구조로 수집합니다.
- **비정형 파일 추출** — 문서(Word·HWP·PDF·Excel·텍스트·JSON·XML)와 이미지(PSD·EXR·일반 이미지)에서 속성과 샘플을 추출합니다.
- **프로파일링** — 최소·최대·평균·결측 등 통계와 샘플 데이터를 산출합니다.
- **수집 범위 지정** — 데이터베이스·스키마·테이블, 버킷·객체 단위로 포함·제외를 지정합니다.
- **검색 서비스 수집** — 검색 인덱스 메타데이터를 수집합니다.

## 스펙

| 항목 | 버전 |
|---|---|
| Python | 3.8 이상 |

의존 패키지는 `ingestion/setup.py`로 관리합니다.

## 구성

```
.
├── airflow-apis/           Airflow 플러그인 (71파일)
│                             REST API 서버로 수집 요청을 받아
│                             DAG 생성·시작·중지·삭제를 수행합니다
├── ingestion/              DAG가 실행하는 수집 코드
│   ├── src/metadata/
│   │   ├── workflow/         수집 프로세스 진입점
│   │   ├── ingestion/        메타데이터 수집과 커넥터
│   │   ├── profiler/         프로파일링
│   │   └── utils/            비정형 파일 추출기
│   ├── src/services/         수집 서비스 계층 (40파일)
│   ├── src/utils/            설정 로딩, 카탈로그 API 연동
│   ├── config_templates/     설정 템플릿
│   └── setup.py
├── scripts/                spec의 JSON을 Python 클래스로 변환
└── spec/                   메타데이터 구조 선언 (JSON, 509파일)
```

## 설정

`ingestion/config_templates/config.ini`를 복사해 사용합니다.

```bash
cp ingestion/config_templates/config.ini ingestion/config.ini
cp ingestion/config_templates/.env.example ingestion/.env
```

| 섹션 | 키 | 설명 |
|---|---|---|
| `security` | `cryptkey` | 저장된 접속 비밀번호를 복호할 키 |
| `catalog_api` | `host`·`port`·`prefix` | 수집 대상 조회와 상태 보고에 사용하는 카탈로그 API |
| | `get_meta_system_info_api`·`set_meta_ingestion_status_api` | 조회·보고 경로 |
| `minio` | `url`·`dataextract_bucket`·`sample_data_bucket` | 객체 저장소와 수집 제외 버킷 |
| `metadata_collector` | `document_tmp_dir` | 문서 변환용 임시 디렉토리 |
| `ingestion` | `system_config_path` | 수집 작업 정의 경로 |

설정 파일 경로는 `.env`의 `METADATA_COLLECTOR_CONFIG_FILE`로 지정합니다.

접속 정보는 설정 파일로 전달합니다. **저장소에 기록하지 않습니다.**

## 선택 설치

다음 기능은 **AGPL-3.0 라이선스 패키지**에 의존하므로 선택 설치입니다. 설치하면 해당 기능 사용에 AGPL-3.0 조건이 적용됩니다.

| 기능 | 설치 | 패키지 |
|---|---|---|
| PDF 추출 | `pip install '.[pdf]'` | PyMuPDF |
| HWP 추출 | `pip install '.[hwp]'` | pyhwp |
| 이미지 객체 탐지 | `pip install '.[yolo]'` | Ultralytics |

설치하지 않으면 해당 추출만 비활성화되고 나머지 수집은 그대로 동작합니다.

## 상용 DB 드라이버

Altibase·Tibero·Oracle 연동에는 각 벤더의 드라이버가 필요합니다. 드라이버는 각 벤더의 라이선스 조건에 따라 배포되므로 본 저장소에 포함하지 않습니다. 아래를 참고해 직접 설치해 주십시오.

- Altibase ODBC: 벤더 제공 경로
- Tibero ODBC: 벤더 제공 경로
- Oracle Instant Client: https://www.oracle.com/database/technologies/instant-client/

SQLAlchemy dialect는 PyPI에서 설치합니다.

```bash
pip install sqlalchemy-altibase sqlalchemy-tibero
```

## 실행

로컬에서 수집을 실행하는 경우입니다.

```bash
cd ingestion
pip install -e .
python metadata_collector_main.py
```

이미지로 빌드하는 경우 최상위의 `build.sh`를 사용합니다. 코드에서 새 라이브러리를 사용했다면 `ingestion/setup.py`를 먼저 수정합니다.

```bash
./build.sh
```

## 원 소스 대비 주요 변경 사항

- **객체 저장소(MinIO)** — CSV·Excel·Word·HWP 등의 메타데이터 수집과 프로파일링
- **상용 DB** — Altibase·Tibero 커넥터와 스키마 추가
- **비정형 파일** — 문서·이미지 속성 추출, 내용 요약
- **프로파일링** — 데이터 사전 연동
- **서버 연동** — 카탈로그 API를 통한 수집 대상 조회와 상태 보고

## 라이선스

[Apache-2.0](LICENSE)

접속 정보와 개인 환경 설정은 저장소에 등록하지 않습니다.

---

본 저장소는 아래 과제의 산출물입니다.

- 과제명: 분산된 데이터에 대한 논리적 데이터 통합과 복합분석을 지원하는 데이터 패브릭 기술개발
- 과제번호: RS-2023-00222191

관련 저장소: [DataFabricPlatform](https://github.com/DataFabricTech/DataFabricPlatform) · [DataFabricVisual](https://github.com/DataFabricTech/DataFabricVisual) · [Recommender](https://github.com/DataFabricTech/Recommender)
