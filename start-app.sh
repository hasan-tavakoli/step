docker network create ranger-env
 
cd docker-composes/ranger
unzip db_data.zip  
rm db_data.zip
docker-compose up -d --build

cd ../hadoop
unzip hadoop_historyserver.zip
rm hadoop_historyserver.zip
unzip hadoop_datanode.zip
rm hadoop_datanode.zip
unzip hadoop_namenode.zip
rm hadoop_namenode.zip
docker-compose up -d --build

cd ../mongo
docker-compose up -d --build

cd ../docker/docker-airflow
docker build --rm --force-rm -t docker-airflow-spark:1.10.7_3.1.2 .

cd ..
unzip postgres-db-volume.zip
rm postgres-db-volume.zip
docker-compose up -d --build

cd ../metabase
docker-compose up -d --build

sleep 100 && docker exec -it hive-server hive -f sql.hql
