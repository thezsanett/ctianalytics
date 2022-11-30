docker network create kafka-network
docker network create cassandra-network

docker-compose -f kafka/docker-compose.yml up -d --build
docker-compose -f consumers/docker-compose.yml up -d --build
docker-compose -f flink/flink-compose.yml up -d --build
echo "Kafka-Manager front end will be avaliable in a few minutes at http://localhost:9000."

# TODO: there may be a bug, where Cassandra sinks don't start in Kafka. 
# You have to manually go to CLI of the "kafka-connect" container and run the below comment to start the Cassandra sinks. 
# docker exec -d kafka-connect ./start-and-wait.sh

docker-compose -f cassandra/docker-compose.yml up -d --build

echo "Done initilizing the containers."
docker ps -a

echo "Waiting 2 minutes for the containers to start..."
sleep 120

echo "Executing scripts in the containers..."
docker exec -d grafana grafana-cli plugins install hadesarchitect-cassandra-datasource

echo "Waiting 1 minute for grafana plugin to install..."
sleep 60
docker restart grafana

echo "Initializing the producer..."
docker-compose -f otxdata-producer/docker-compose.yml up -d --build 

echo "Process done."
