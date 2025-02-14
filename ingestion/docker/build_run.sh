# 아래 명령 실행 위치는 docker 폴더 상위
# 예 :
# project/
# |- src/
# |- dpcker/
# |  |- Dockerfile
# |  |- docker-compose.yml
# |- ** docker-compose 명령어 실행 위치 **

docker-compose -f docker/docker-compose.yml build

docker-compose -f docker/docker-compose.yml up -d

