docker stop $(docker ps -aq)
docker rm $(docker ps -aq)
#docker rmi $(docker images -aq)
cd docker-composes/ranger/
zip -r  db_data.zip db_data/
sudo rm -r  db_data/
cd ../hadoop
zip -r  hadoop_datanode.zip hadoop_datanode/
sudo rm -r  hadoop_datanode/

zip -r   hadoop_namenode.zip hadoop_namenode/
sudo rm -r hadoop_namenode/

zip -r  hadoop_historyserver.zip hadoop_historyserver/
sudo rm -r hadoop_historyserver/

