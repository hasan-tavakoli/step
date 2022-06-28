docker stop $(docker ps -aq)
docker rm $(docker ps -aq)
#docker rmi $(docker images -aq)
cd docker-composes/ranger/
zip -r  db_data.zip db_data/
sudo rm -r  db_data/

