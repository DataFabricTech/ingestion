# filter_config 구조 명세

`system_info` API 응답의 `filter_config` 필드 구조를 정의합니다.

---

## 개요

`filter_config`는 수집 시스템별로 고정된 필터 설정을 관리합니다.  
CLI `--include` / `--exclude` 보다 우선 적용되며, 충돌 시 `filter_config` 설정이 최종 결과를 결정합니다.

---

## 구조

```json
{
  "filter_config": {
    "bucket": {
      "includes": ["string", ...],
      "excludes": ["string", ...]
    },
    "container": {
      "includes": ["string", ...],
      "excludes": ["string", ...]
    },
    "schema": {
      "includes": ["string", ...],
      "excludes": ["string", ...]
    },
    "table": {
      "includes": ["string", ...],
      "excludes": ["string", ...]
    },
    "classifications_rules": { ... }
  }
}
```

---

## 필드 설명

### Storage 서비스 (MinIO 등)

| 필드 | 타입 | 적용 대상 | 설명 |
|------|------|----------|------|
| `bucket.includes` | `string[]` | `bucketFilterPattern.includes` | 수집할 버킷 목록 |
| `bucket.excludes` | `string[]` | `bucketFilterPattern.excludes` | 수집 제외할 버킷 목록 |
| `container.includes` | `string[]` | `containerFilterPattern.includes` | 수집할 경로/파일 패턴 |
| `container.excludes` | `string[]` | `containerFilterPattern.excludes` | 수집 제외할 경로/파일 패턴 |

### Database 서비스 (PostgreSQL, Oracle 등)

| 필드 | 타입 | 적용 대상 | 설명 |
|------|------|----------|------|
| `schema.includes` | `string[]` | `schemaFilterPattern.includes` | 수집할 스키마 목록 |
| `schema.excludes` | `string[]` | `schemaFilterPattern.excludes` | 수집 제외할 스키마 목록 |
| `table.includes` | `string[]` | `tableFilterPattern.includes` | 수집할 테이블 목록 |
| `table.excludes` | `string[]` | `tableFilterPattern.excludes` | 수집 제외할 테이블 목록 |

### 공통

| 필드 | 타입 | 설명 |
|------|------|------|
| `classifications_rules` | `object` | 분류 규칙. 상세 구조는 [classifications_rules.md](classifications_rules.md) 참고 |

---

## 우선순위

```
filter_config  >  CLI (--include / --exclude)  >  config 기본값
```

### includes — whitelist 동작

`includes`가 설정되면 해당 항목만 수집합니다. 기존 `excludes`(config 기본값 포함)는 모두 무시됩니다.

| filter_config | CLI | 최종 결과 |
|--------------|-----|----------|
| `bucket.includes: ["A"]` | `--exclude bucket=A` | **A만 수집** (excludes clear) |
| `bucket.includes: ["A"]` | (없음) | **A만 수집** (excludes clear) |
| `bucket.includes: ["A"]` | `--exclude bucket=B` | **A만 수집** (B exclude 무시) |

> `includes`와 `excludes`가 filter_config에 동시 설정된 경우 `includes`가 우선 적용되며 `excludes`는 무시됩니다.

### excludes — blacklist 동작

`includes`가 없고 `excludes`만 설정된 경우, 해당 항목을 제외하고 나머지를 수집합니다.

| filter_config | CLI | 최종 결과 |
|--------------|-----|----------|
| `bucket.excludes: ["A"]` | `--include bucket=A` | **A 제외** (filter_config 우선) |
| `bucket.excludes: ["A"]` | (없음) | A 제외 |
| (없음) | `--exclude bucket=A` | A 제외 |

### filter_config 미설정

CLI와 config 기본값이 그대로 적용됩니다.

> **기본 제외 버킷** (`dataextract`, `meta-data-sample`)도 `filter_config.bucket.includes`에 포함 시 수집 대상으로 전환됩니다.

---

## 예시

### Storage — 특정 버킷만 수집

```json
{
  "filter_config": {
    "bucket": {
      "includes": ["my-data", "public-data"],
      "excludes": []
    },
    "container": {
      "includes": [],
      "excludes": ["tmp/", "staging/"]
    }
  }
}
```

### Database — 특정 스키마/테이블 제외

```json
{
  "filter_config": {
    "schema": {
      "includes": [],
      "excludes": ["pg_catalog", "information_schema"]
    },
    "table": {
      "includes": [],
      "excludes": ["temp_%", "bak_%"]
    }
  }
}
```

### 기본 제외 버킷 수집 허용

```json
{
  "filter_config": {
    "bucket": {
      "includes": ["dataextract"],
      "excludes": []
    }
  }
}
```

---

## 적용 위치

`src/utils/metadata_process.py` — `MetadataProcess.apply_filter_config()`
