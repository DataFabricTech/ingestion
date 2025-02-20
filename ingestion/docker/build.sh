# 아래 명령 실행 위치는 docker 폴더 상위
# 예 :
# project/
# |- src/
# |- dpcker/
# |  |- Dockerfile
# |  |- docker-compose.yml
# |- ** docker-compose 명령어 실행 위치 **

export DATA_CATALOG_SOLUTION_DATA=/DATA_CATALOG/data
export DATA_CATALOG_SOLUTION_IMG_VERSION=0.4

docker-compose -f docker/docker-compose.yml build

