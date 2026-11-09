# ------------------------------
# 1단계: 빌드용 이미지
# ------------------------------
FROM rockylinux:8.9 AS builder

# EPEL, RPM Fusion 리포 설치 및 powertools 활성화 후 mediainfo 설치
RUN dnf install -y epel-release dnf-utils && \
    dnf install -y https://mirrors.rpmfusion.org/free/el/rpmfusion-free-release-8.noarch.rpm && \
    dnf config-manager --set-enabled powertools && \
    dnf install -y mediainfo && \
    dnf clean all && rm -rf /var/cache/dnf /tmp/*

# 개발 도구 설치
# mesa-libGL, libzstd-devel : cr2 관련 OpenGL 패키지
RUN dnf -y groupinstall "Development Tools" && \
    dnf -y install \
        python38 \
        python38-devel \
        python3-pip \
        gcc \
        make \
        glibc-devel \
        cyrus-sasl-devel \
        cyrus-sasl-plain \
        python3-devel \
        libffi-devel  \
        openssh-server \
        sudo \
        zlib-devel \
        unixODBC \
        unixODBC-devel \
        mesa-libGL \
        libzstd-devel \
        unzip && \
    dnf clean all && rm -rf /var/cache/dnf /tmp/*


# 파이선 설치 (캐시 없이 설치)
RUN python3.8 -m pip install --no-cache-dir --upgrade pip setuptools packaging wheel cython
# Python 링크 설정
RUN ln -s /usr/bin/python3.8 /usr/bin/python && \
    ln -s /usr/bin/pip3.8 /usr/bin/pip

WORKDIR /metadata_collector

# 프로젝트 파일들을 컨테이너로 복사
COPY ../setup.py /metadata_collector/setup.py
COPY ../src /metadata_collector/src
COPY ../metadata_collector_main.py /metadata_collector/metadata_collector_main.py
# generate
COPY ../scripts /metadata_collector/scripts
COPY ../spec /metadata_collector/spec
COPY ../Makefile /metadata_collector/Makefile
# odbc
COPY ../odbc/odbcinst.ini /etc/odbcinst.ini
COPY ../odbc/odbc.ini /etc/odbc.ini

# oracle client (zip -> extract to /instantclient and create /instantclient/current)
# 빌드 컨텍스트가 repo root(docker-compose context: ../)인 전제에서 `instantclient/*.zip`를 사용합니다.
ARG ORACLE_IC_DIR=""
COPY instantclient/*.zip /tmp/instantclient/
RUN mkdir -p /instantclient && \
    for f in /tmp/instantclient/*.zip; do unzip -q "$f" -d /instantclient; done && \
    rm -rf /tmp/instantclient && \
    if [ -n "$ORACLE_IC_DIR" ] && [ -d "/instantclient/$ORACLE_IC_DIR" ]; then \
        ln -sfn "/instantclient/$ORACLE_IC_DIR" /instantclient/current; \
    else \
        CANDIDATE="$(find /instantclient -maxdepth 1 -type d -name 'instantclient_*' | sort | tail -n 1)"; \
        if [ -n "$CANDIDATE" ]; then \
            ln -sfn "$CANDIDATE" /instantclient/current; \
        else \
            echo "ERROR: Could not find extracted instantclient directory under /instantclient (expected instantclient_*)"; \
            exit 1; \
        fi; \
    fi

# metadata_collector 설치 (캐시 없이 설치)
RUN pip install --no-cache-dir ".[all]"

# generate
RUN make install_antlr_cli && make generate

# pip 캐시 및 빌드 아티팩트 정리
RUN rm -rf /root/.cache/pip && \
    rm -rf /tmp/* && \
    rm -rf /var/tmp/*


# ------------------------------
# 2단계: 런타임용 슬림 이미지
# ------------------------------
FROM rockylinux:8.9

# 런타임에 필요한 최소 패키지 설치
RUN dnf install -y epel-release dnf-utils && \
    dnf install -y https://mirrors.rpmfusion.org/free/el/rpmfusion-free-release-8.noarch.rpm && \
    dnf config-manager --set-enabled powertools && \
    dnf -y install \
        mediainfo \
        python38 \
        python3-pip \
        cyrus-sasl-devel \
        cyrus-sasl-plain \
        mesa-libGL \
        libzstd-devel \
        unixODBC \
        openssh-server \
        sudo \
        libreoffice \
        google-noto-sans-cjk-ttc-fonts \
        fontconfig \
        libnsl \
        libaio && \
        fc-cache -fv && \
    dnf clean all && rm -rf /var/cache/dnf /tmp/*

# ------------
# 패키지 설명
# libreoffice : pdf 변환
# google-noto-sans-cjk-ttc-fonts : 한글 폰트 설치
# fontconfig : 글꼴 캐시 유틸
# fc-cache -fv : 글꼴 캐시 갱신
# ------------


WORKDIR /metadata_collector

# 빌드 스테이지에서 필요한 파일만 복사
# python
COPY --from=builder /usr/local/lib/python3.8 /usr/local/lib/python3.8
COPY --from=builder /usr/local/lib64/python3.8 /usr/local/lib64/python3.8
COPY --from=builder /usr/bin/pip3.8 /usr/bin/pip3.8
# psd-tools 같은 CLI 도구 포함됨
COPY --from=builder /usr/local/bin /usr/local/bin
# odbc 정보
COPY --from=builder /etc/odbcinst.ini /etc/odbcinst.ini
COPY --from=builder /etc/odbc.ini /etc/odbc.ini

COPY --from=builder /metadata_collector /metadata_collector
COPY --from=builder /instantclient /instantclient

# Python 링크 설정
RUN ln -s /usr/bin/python3.8 /usr/bin/python && \
    ln -s /usr/bin/pip3.8 /usr/bin/pip

# SSH 설정
RUN ssh-keygen -A && \
    mkdir /var/run/sshd && \
    echo 'root:root' | chpasswd && \
    sed -i 's/^#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

# alias 설정
RUN echo 'alias ll="ls -alF --color=auto"' >> /root/.bashrc

# ENV 설정
ENV PYTHONPATH="/metadata_collector/src:/metadata_collector/site-package:$PYTHONPATH"
ENV LD_LIBRARY_PATH="/instantclient/current:${LD_LIBRARY_PATH}"

# 포트 노출
EXPOSE 22

# 컨테이너 유지 및 odbc 설정
CMD ["sh", "-c", "cp /metadata_collector/conf/odbc/odbcinst.ini /etc/odbcinst.ini && cp /metadata_collector/conf/odbc/odbc.ini /etc/odbc.ini && tail -f /dev/null"]
