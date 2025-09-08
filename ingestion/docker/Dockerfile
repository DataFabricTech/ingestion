FROM rockylinux:8.9

# python, pip 설치
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
    unixODBC-devel && \
    python3.8 -m pip install --upgrade pip && \
    dnf clean all

# cr2 관련 OpenGL 패키지
RUN dnf install mesa-libGL

RUN dnf -y install libzstd-devel

WORKDIR /metadata_collector

# 프로젝트 파일들을 컨테이너로 복사
COPY ../setup.py /metadata_collector/setup.py

#COPY ../.env /metadata_collector/.env
COPY ../src /metadata_collector/src
COPY ../metadata_collector_main.py /metadata_collector/metadata_collector_main.py

COPY ../odbc/odbcinst.ini /etc/odbcinst.ini
COPY ../odbc/odbc.ini /etc/odbc.ini

# SASL 관련 라이브러리 설치
#RUN dnf -y update && \
#    dnf -y install \
#    cyrus-sasl-devel \
#    python3-devel \
#    libffi-devel  \
#    openssh-server \
#    sudo && \
#    dnf clean all

# tmp 디렉토리
RUN mkdir /metadata_collector/tmp

# ssh 관련
RUN ssh-keygen -A
RUN mkdir /var/run/sshd
RUN echo 'root:root' | chpasswd

# root 계정의 비밀번호 인증 허용
RUN sed -i 's/^#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

EXPOSE 22

# python 링크
RUN ln -s /usr/bin/python3.8 /usr/bin/python
RUN ln -s /usr/bin/pip3.8 /usr/bin/pip

# pip와 setuptools를 최신 버전으로 업그레이드
RUN pip install --upgrade pip setuptools packaging

# RUN dnf -y module enable python38

# metadata_collector 설치
# RUN python /metadata_collector/setup.py install
RUN pip install ".[all]"
# RUN pip install ingest-server

#--no-build-isolation --no-deps 옵션을 추가해야해서 따로 설치
RUN pip install psd-tools==1.10.7 --no-build-isolation --no-deps

# generate
COPY ../scripts /metadata_collector/scripts
COPY ../spec /metadata_collector/spec
COPY ../Makefile /metadata_collector/Makefile
RUN make install_antlr_cli
RUN make generate

# bashrc 설정 추가
RUN echo 'alias ll="ls -alF --color=auto"' >> /root/.bashrc

# PYTHONPATH 환경 변수 설정
ENV PYTHONPATH="/metadata_collector/src:/metadata_collector/site-package:$PYTHONPATH"

#CMD ["tail", "-f", "/dev/null"]
CMD ["sh", "-c", "cp /metadata_collector/conf/odbc/odbcinst.ini /etc/odbcinst.ini && tail -f /dev/null"]