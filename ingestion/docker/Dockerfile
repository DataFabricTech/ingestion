FROM rockylinux:8.9

# python, pip 설치
RUN dnf -y groupinstall "Development Tools" && \
    dnf -y install \
    python38 \
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
#sudo dnf install cyrus-sasl cyrus-sasl-devel cyrus-sasl-lib cyrus-sasl-plain
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

# ssh 관련
RUN ssh-keygen -A
RUN mkdir /var/run/sshd
RUN echo 'root:root' | chpasswd

# root 계정의 비밀번호 인증 허용
RUN sed -i 's/^#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

EXPOSE 22

# python 링크
RUN ln -s /usr/bin/python3.8 /usr/bin/python

# pip와 setuptools를 최신 버전으로 업그레이드
RUN pip install --upgrade pip setuptools
# metadata_collector 설치
# RUN python /metadata_collector/setup.py install
RUN pip install .
RUN pip install ingest-server[all]

# PYTHONPATH 환경 변수 설정
ENV PYTHONPATH="/metadata_collector/src:/metadata_collector/site-package:$PYTHONPATH"

#CMD ["tail", "-f", "/dev/null"]
CMD ["sh", "-c", "cp /metadata_collector/conf/odbc/odbcinst.ini /etc/odbcinst.ini && tail -f /dev/null"]