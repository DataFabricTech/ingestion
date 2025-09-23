# ------------------------------
# 1단계: 빌드용 이미지
# ------------------------------
FROM rockylinux:8.9 AS builder

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
        libzstd-devel && \
    dnf clean all && rm -rf /var/cache/dnf /tmp/*


# 파이선 설치
RUN python3.8 -m pip install --upgrade pip setuptools packaging wheel cython
# Python 링크 설정
RUN ln -s /usr/bin/python3.8 /usr/bin/python && \
    ln -s /usr/bin/pip3.8 /usr/bin/pip

RUN python3.8 -m site && python3.8 -m pip list

WORKDIR /metadata_collector

RUN python3.8 -m site && python3.8 -m pip list
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

# metadata_collector 설치
RUN pip install ".[all]"

## psd-tool 별도 설치 (옵션 설정을 하는데 setup.py에서는 설정을 못함)
# 빌드 도구 재설치, pip install ".[all]" 이후에 갱신? 되는것같음
RUN pip install --upgrade setuptools packaging wheel cython
#--no-build-isolation --no-deps 옵션을 추가해야해서 따로 설치
RUN pip install psd-tools==1.10.7 --no-build-isolation --no-deps

# generate
RUN make install_antlr_cli && make generate


# ------------------------------
# 2단계: 런타임용 슬림 이미지
# ------------------------------
FROM rockylinux:8.9

# 런타임에 필요한 최소 패키지 설치
RUN dnf -y install \
        python38 \
        python3-pip \
        cyrus-sasl-devel \
        cyrus-sasl-plain \
        mesa-libGL \
        libzstd-devel \
        unixODBC \
        openssh-server \
        sudo \
        libreoffice \                               # pdf 변환
        google-noto-sans-cjk-ttc-fonts \            # 한글 폰트 설치
        fontconfig && \                             # 글꼴 캐시 유틸
        fc-cache -fv && \                           # 글꼴 캐시 갱신
    dnf clean all && rm -rf /var/cache/dnf /tmp/*

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

# 포트 노출
EXPOSE 22

# 컨테이너 유지 및 odbc 설정
CMD ["sh", "-c", "cp /metadata_collector/conf/odbc/odbcinst.ini /etc/odbcinst.ini && cp /metadata_collector/conf/odbc/odbc.ini /etc/odbc.ini && tail -f /dev/null"]
