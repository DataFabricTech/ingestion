# MinIO 메타데이터 수집 흐름

## 개요

MinIO 수집은 **Ingestion(메타데이터 수집)**과 **Profile(프로파일링)** 두 가지 타입으로 실행된다.

```
metadata_collector_main.py
  --collector_type ingestion  →  metadata_collector_execute()
  --collector_type profile    →  metadata_profiler_execute()
```

---

## 1. Ingestion 흐름 (메타데이터 수집)

```
metadata_collector_main.py
  └─ metadata_collector_execute(system_id, sink, filter_include_dict, filter_exclude_dict)
       └─ MinioSource (src/services/storage/minio/source.py)
            ├─ getServiceConnection()  → MinioConnection (accessKey, secretKey, endPointURL)
            └─ getSourceConfig()       → StorageServiceMetadataPipeline
                 └─ MinioSource (src/metadata/ingestion/source/storage/minio/metadata.py)
                      ├─ fetch_buckets()           → bucket 목록 조회 (list_buckets or bucketNames)
                      ├─ get_buckets()             → MinioContainerDetails (버킷 컨테이너 생성)
                      │    └─ _set_bucket_obj_info()  → 전체 오브젝트 스캔 → _metadata_cache 구성
                      ├─ get_directories()         → 디렉토리 계층 구조 생성
                      │    └─ _generate_directory_container()
                      └─ get_containers()          → 파일별 컨테이너 생성
                           ├─ _generate_container_details()            (CSV → ContainerDataModel 포함)
                           ├─ _generate_unstructured_container_details() (문서: doc/docx/hwp/txt/json/xml/xls/xlsx)
                           ├─ _generate_image_container_details()      (이미지: jpg/png/bmp/gif/pdf 등)
                           ├─ _generate_media_container_details()      (미디어: wav/mp3/mp4/mov)
                           └─ _generate_unsupported_container_details() (기타)
```

### 컨테이너 계층 구조

MinIO 오브젝트 경로를 분석하여 아래와 같은 계층 구조로 Container 엔티티를 생성한다.

```
StorageService (MinIO 서비스)
  └─ Bucket (버킷)
       └─ Directory (디렉토리, 중첩 가능)
            └─ File Container (개별 파일)
```

예시: `my-bucket/reports/2024/data.csv`
```
MinIO Service
  └─ my-bucket
       └─ reports
            └─ 2024
                 └─ data.csv  (ContainerDataModel + 컬럼 정보 포함)
```

### 파일 포맷별 처리

| 포맷 | 처리 방식 |
|------|-----------|
| `csv` | ContainerDataModel (컬럼 스키마 추출) |
| `doc`, `docx`, `hwp`, `hwpx`, `txt`, `xml`, `json`, `xls`, `xlsx` | 비정형 컨테이너 (rdfs 메타 추출) |
| `jpg`, `jpeg`, `png`, `bmp`, `gif`, `webp`, `cr2`, `nef`, `tiff`, `tif`, `pdf` | 이미지 컨테이너 (rdfs 메타 추출) |
| `wav`, `mp3`, `mp4`, `mov`, `m4v` | 미디어 컨테이너 (rdfs 메타 추출) |
| 그 외 | 미지원 컨테이너 (fileFormats=none) |

---

## 2. Profile 흐름 (데이터 프로파일링)

```
metadata_collector_main.py
  └─ metadata_profiler_execute(system_id, sink, filter_include_dict, filter_exclude_dict)
       └─ MinioSource (src/services/storage/minio/source.py)
            └─ ProfilerExecutor.execute(source)
                 └─ MetadataSourceForMinio (src/metadata/profiler/source/metadata_minio.py)
                      ├─ YOLO 모델 로딩 (yolov8s.pt)
                      └─ _iter()  → 카탈로그 API에서 Container 엔티티 목록 조회
                           └─ (지원 포맷 필터링 후 각 Container에 대해)
                                └─ MinIOProfilerSource (src/metadata/profiler/source/storage/minio/profiler_source.py)
                                     └─ get_profiler_runner(entity, profiler_config)
                                          ├─ 문서 포맷  → DocProfiler  + DocumentProfilerInterface
                                          ├─ 이미지/PDF → ImageProfiler + ImageProfilerInterface
                                          └─ 구조적 데이터 → DefaultProfiler + StorageProfilerInterface
```

### Profiler 처리 단계

```
MinioProfilerProcessor._run(MinIoProfilerSourceAndEntity)
  └─ profiler_runner.process()
       ├─ DocProfiler.process()    → 문서 텍스트/메타 분석
       ├─ ImageProfiler.process()  → YOLOv8 객체 탐지 + 이미지 메타 분석
       └─ DefaultProfiler.process() → 컬럼 통계 지표 계산
```

### Profile 지원 파일 포맷

| 분류 | 포맷 |
|------|------|
| 문서 | `csv`, `xls`, `xlsx`, `hwp`, `hwpx`, `doc`, `docx`, `txt`, `json`, `xml` |
| 이미지/PDF | `jpeg`, `jpg`, `png`, `bmp`, `gif`, `webp`, `cr2`, `nef`, `tiff`, `tif`, `pdf` |

---

## 3. 핵심 컴포넌트

| 컴포넌트 | 경로 | 역할 |
|----------|------|------|
| `MinioClient` | `src/metadata/clients/minio_client.py` | boto3 기반 S3 호환 클라이언트 생성 |
| `MinioSource` (서비스) | `src/services/storage/minio/source.py` | 연결 설정 조립 (accessKey/endPointURL) |
| `MinioSource` (ingestion) | `src/metadata/ingestion/source/storage/minio/metadata.py` | 버킷/디렉토리/파일 Container 엔티티 생성 |
| `MetadataSourceForMinio` | `src/metadata/profiler/source/metadata_minio.py` | 프로파일링 대상 Container 목록 제공 |
| `MinIOProfilerSource` | `src/metadata/profiler/source/storage/minio/profiler_source.py` | 파일 포맷에 따라 적절한 Profiler 생성 |
| `MinioProfilerProcessor` | `src/metadata/profiler/processor/minio_processor.py` | Profiler 실행 및 결과 반환 |

---

## 4. 실행 예시

```bash
# Ingestion (메타데이터 수집)
python metadata_collector_main.py \
  --collector_type ingestion \
  --system_id <system_id> \
  --include bucket=my-bucket

# Profile (데이터 프로파일링)
python metadata_collector_main.py \
  --collector_type profile \
  --system_id <system_id> \
  --include bucket=my-bucket object=data.csv
```

---

## 5. 프로파일링 샘플링 결과

### 전제: `generateSampleData` 플래그

`StorageServiceProfilerPipeline.generateSampleData`가 `True`일 때만 샘플링을 실행한다.
`False`이면 각 Profiler는 샘플 수집 없이 `ProfilerResponse`를 반환한다.

### 파일 타입별 샘플링 결과

**문서 (DocProfiler)**
([src/metadata/profiler/processor/document_core.py](src/metadata/profiler/processor/document_core.py))

```
파일 다운로드 (tmp dir)
  → 텍스트 추출 (hwp/docx/txt/xml/json, chunk_size=-1 → 전체)
  → Summarization.summarize() → AI 요약문 생성
  → PDF 변환 → 썸네일 이미지 생성

ProfilerResponse {
    unstructured_sample_data  : 추출된 텍스트
    unstructured_summary      : AI 요약문
    image_sample_data         : PDF 변환 썸네일
}
```

**이미지 (ImageProfiler)**
([src/metadata/profiler/processor/image_core.py](src/metadata/profiler/processor/image_core.py))

```
파일 다운로드 (tmp dir)
  → PIL 메타 추출 (EXIF 등) + 리사이즈 썸네일
  → YOLOv8 객체 탐지 → 탐지 객체 목록 + 바운딩박스 이미지

ProfilerResponse {
    image_sample_data              : 리사이즈 썸네일 (PDF는 첫 페이지 이미지)
    image_text                     : EXIF/PIL 메타 텍스트
    image_detected_objects         : 탐지된 객체 목록
    image_detected_objects_image   : 바운딩박스 그려진 이미지
}
```

**구조적 데이터 CSV (DefaultProfiler)**
([src/metadata/profiler/processor/core.py](src/metadata/profiler/processor/core.py))

```
샘플 rows 조회 (SAMPLE_DATA_DEFAULT_COUNT 제한)
  → storage_config 있으면 S3에 parquet 파일로 업로드
    (경로: {prefix}/{service}/{bucket}/{dir}/{file}/sample_data.parquet)
  → rows를 SAMPLE_DATA_DEFAULT_COUNT로 잘라서 반환

ProfilerResponse {
    profile      : TableProfile + ColumnProfile (컬럼 통계 지표)
    sample_data  : TableData (rows + columns)
}
```

### 공통 처리

- 모든 결과는 `ProfilerResponse`로 반환 → `MinioProfilerProcessor`가 `Either(right=profile)`로 래핑 → 카탈로그 API 서버로 전송
- 임시 다운로드 파일은 `finally` 블록에서 처리 후 즉시 삭제

---

## 6. MetaData 서버 전송 API

모든 요청은 `{hostPort}/api/v1` 을 base URL로 사용한다.

### Ingestion — 엔티티 등록/수정

| 대상 | 메서드 | 경로 | 설명 |
|------|--------|------|------|
| StorageService | `PUT` | `/services/storageServices` | MinIO 서비스 등록 |
| Container (버킷/디렉토리/파일) | `PUT` | `/containers` | Container 엔티티 생성 또는 업데이트 |
| File | `PUT` | `/files` | File 엔티티 생성 또는 업데이트 |
| 수집 상태 체크 | `PUT` | `/ingestion/check` | 수집 상태 확인 |
| 수집 상태 업데이트 | `PUT` | `/ingestion/collector/status` | 수집 진행 상태 업데이트 |

### Profile — 프로파일 데이터 전송

| 대상 | 메서드 | 경로 | 설명 |
|------|--------|------|------|
| 구조적 데이터 프로파일 (CSV) | `PUT` | `/containers/{id}/tableProfile` | TableProfile + ColumnProfile 저장 |
| 비정형/이미지 rdfs 저장 | `PUT` | `/containers/{id}/unstructured_tableProfile` | summary·image_text·detected_objects 저장 |
| 샘플 데이터 (CSV rows) | `PUT` | `/containers/sampleData?data_type=STRUCTURE&system_id=…&meta_id=…` | 구조적 샘플 rows 저장 |
| 샘플 이미지 (문서 썸네일) | `PUT` | `/containers/sampleData/image?data_type=UNSTRUCTURED&system_id=…&meta_id=…` | 문서 PDF 변환 이미지 저장 |
| 샘플 이미지 (이미지 파일) | `PUT` | `/containers/sampleData/image?data_type=IMAGE&system_id=…&meta_id=…` | 리사이즈 썸네일 저장 |
| 객체 탐지 이미지 | `PUT` | `/containers/sampleData/image?data_type=DETECTED_OBJECTS_IMAGE&system_id=…&meta_id=…` | YOLOv8 바운딩박스 이미지 저장 |
| 프로파일 실행 요청 | `POST` | `/profile/execute/storage` | 프로파일링 실행 트리거 |

### 전송 흐름 요약

```
ProfilerResponse
  ├─ 비정형/이미지 파일
  │    ├─ PUT /containers/{id}/unstructured_tableProfile  (rdfs: summary·image_text·detected_objects)
  │    └─ PUT /containers/sampleData/image               (data_type별 이미지)
  │
  └─ 구조적 데이터 (CSV)
       ├─ PUT /containers/{id}/tableProfile              (TableProfile + ColumnProfile)
       └─ PUT /containers/sampleData                    (sample rows)
```

> `data_type` 쿼리 파라미터 값: `STRUCTURE` | `UNSTRUCTURED` | `IMAGE` | `DETECTED_OBJECTS_IMAGE`

---

## 7. MinIO 접속 방식

`MinioClient`([src/metadata/clients/minio_client.py](src/metadata/clients/minio_client.py))는 **boto3 S3 호환 방식**으로 접속한다.
MinIO가 S3 API를 그대로 구현하기 때문에 `endpoint_url`만 변경하여 boto3를 그대로 사용한다.

```python
session.client(service_name="s3", endpoint_url=self.config.endPointURL)
```

### 인증 파라미터 (MinioCredentials)

| 파라미터 | 설명 | 필수 여부 |
|----------|------|-----------|
| `accessKeyId` | MinIO Access Key | 선택 |
| `secretKey` | MinIO Secret Key (암호화 저장) | 선택 |
| `sessionToken` | 임시 세션 토큰 | 선택 |
| `region` | 리전 (MinIO는 무관하지만 boto3 내부 사용) | 선택 |
| `endPointURL` | MinIO 서버 주소 (예: `http://host:9000`) | 핵심 |

`accessKeyId` / `secretKey`가 없으면 boto3의 credential fallback chain(환경변수 → `~/.aws/credentials` 등)을 따른다.

### MinioConnection 구조

```
MinioConnection
  ├─ minioConfig: MinioCredentials  (accessKeyId, secretKey, endPointURL 등)
  ├─ bucketNames: List[str]         (지정 시 해당 버킷만 수집)
  ├─ connectionOptions
  └─ connectionArguments
```

---

## 8. 내부 캐시 구조

`MinioSource`는 버킷 스캔 시 두 개의 내부 캐시를 사용한다.

- **`_metadata_cache`** (`Dict[str, MetadataEntry]`): 오브젝트 경로 → 파일 포맷/크기/소유자 정보
- **`_dir_cache`** (`Dict[str, List[str]]`): 서비스.버킷 키 → 현재 처리 중인 디렉토리 경로 스택

버킷 전환 시 두 캐시 모두 초기화(`clear()`)된다.
