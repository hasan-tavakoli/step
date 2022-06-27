docker network create ranger-env

cd docker-composes/ranger
docker-compose up -d --build

cd ../hadoop
docker-compose up -d --build

cd ../mongo
docker-compose up -d --build

cd ../docker/docker-airflow
docker build --rm --force-rm -t docker-airflow-spark:1.10.7_3.1.2 .

cd ..
docker-compose up -d --build
