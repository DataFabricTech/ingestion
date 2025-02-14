FROM rockylinux:8.9

# python, pip 설치
RUN dnf -y groupinstall "Development Tools" && \
    dnf -y install python3.8 python3-pip && \
    python3.8 -m pip install --upgrade pip && \
    dnf clean all && \
    # python3.8을 python 명령으로 링크
    ln -s /usr/bin/python3.8 /usr/bin/python

WORKDIR /metadata_collector

# 프로젝트 파일들을 컨테이너로 복사
COPY ../setup.py /metadata_collector/setup.py

COPY ../.env /metadata_collector/.env
COPY ../conf /metadata_collector/conf
COPY ../src /metadata_collector/src
COPY ../metadata_collector_main.py /metadata_collector/metadata_collector_main.py

# SASL 관련 라이브러리 설치
#RUN apt-get update && \
#    apt-get install -y \
#    libsasl2-dev \
#    build-essential && \
#    rm -rf /var/lib/apt/lists/*

RUN dnf -y update && \
    dnf -y install \
    cyrus-sasl-devel \
    gcc-c++ && \
    dnf clean all

# pip와 setuptools를 최신 버전으로 업그레이드
RUN pip install --upgrade pip setuptools
# metadata_collector 설치
RUN python /metadata_collector/setup.py install
RUN pip install ingest-server[all]

# PYTHONPATH 환경 변수 설정
ENV PYTHONPATH="/metadata_collector/src:$PYTHONPATH"

# 5. 프로젝트 실행 (필요에 따라 수정)
CMD ["tail", "-f", "/dev/null"]