---
본 문서는 이 서비스에 대해서 설명한다.
---

## 개요

이 서비스(github.com/datafabrictech/ingestion)는 메타데이터 수집을 담당한다.  
Airflow 위에서 동작하며, Open VDAP 서버의 메타데이터 수집 요청에의해 동작한다.

## 코드 설명

다음은 각 디렉토리 별 설명이다.

1. airflow-apis  
airflow 의 plugin 형태로 동작하는 RestAPI Server로 Open VDAP 서버의 요청을 수신, 
airflow DAG 생성, 시작, 중지, 삭제를 수행한다.  
2. ingestion   
    DAG(Python Operator)에 의해 실제 실행되는 코드로 크게 다음과 같이 분류할 수 있다.  
    - workflow  
    DAG에 의해 실행되는 메타데이터 수집 프로세스
    - ingestion  
    메타데이터 수집
    - profiler  
    프로파일링 정보 수집(min, max, avg, sample)
    - mixins  
    수집된 정보를 서버로 전송하는 API
3. scripts  
spec 에 선언된 json 파일을 python 클래스로 변경한다.
4. spec  
메타데이터 수집을 위한 데이터 구조체 선언부(JSON)

## 이미지 빌드

이미지 빌드는 코드 수정 후 최상위 디렉토리에 build.sh를 이용해 수행할 수 있다.

> 코드 내 추가적인 라이브러리를 사용한 경우 `setup.py`를 수정한다.

```shell
./build.sh
```