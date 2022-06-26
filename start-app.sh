docker network create ranger-env

cd docker-composes/ranger
docker-compose up -d --build

cd ../hadoop
docker-compose up -d --build

