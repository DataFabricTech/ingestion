FROM python:3.8

WORKDIR /opt/metadata_collector

# PYTHONPATH 환경 변수 설정
ENV PYTHONPATH="/opt/metadata_collector/src:$PYTHONPATH"

COPY ./requirements.txt /opt/metadata_collector/requirements.txt
RUN pip install --no-cache-dir -r /opt/metadata_collector/requirements.txt

COPY .env /opt/metadata_collector/.env
COPY ./conf /opt/metadata_collector/conf
COPY ./src /opt/metadata_collector/src
COPY ./metadata_collector_main.py /opt/metadata_collector/metadata_collector_main.py

CMD ["tail", "-f", "/dev/null"]