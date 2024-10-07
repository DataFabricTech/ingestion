# python setup

- python 3.8로 ingest_server/venv 구성
```
ingest_server/venv/activate
```
# module root
```
./lib
./src
./tests
```

# build
```
python setup.py install
pip install ./whl/sasl-0.3.1-cp38-cp38-win_amd64.whl   (Window의 경우)
pip install ingest-server[all]
```

# test run
```
python tests/test_metadata.py
```
