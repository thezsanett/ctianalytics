 docker network create kafka-network
 docker network create cassandra-network
 docker-compose -f cassandra/docker-compose.yml up -d
 docker-compose -f kafka/docker-compose.yml up -d
 docker-compose -f otxdata-producer/docker-compose.yml up -d 
 docker-compose -f consumers/docker-compose.yml up -d
 docker ps -a
 echo "Kafka-Manager front end will be avaliable in a few minutes at http://localhost:9000"
 
 #TODO: there may be a bug, where Cassandra sinks don't start in Kafka. You have to manually go to CLI of the "kafka-connect" container and run the below comment to start the Cassandra sinks.
