curl -X POST -H "Content-Type: application/json" --data '
{ "name":  "mongo-source-tutorial3-eventroundtrip",
  "config": {
    "connector.class":"com.mongodb.kafka.connect.MongoSourceConnector",
    "connection.uri":"mongodb://mongo1:27017,mongo2:27017,mongo3:27017",
    "database":"mydb","collection":"Source"}
}' http://connect:8083/connectors -w "\n" | jq .

