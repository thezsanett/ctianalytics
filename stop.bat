 docker-compose -f otxdata-producer/docker-compose.yml down
 docker-compose -f kafka/docker-compose.yml down
 docker-compose -f cassandra/docker-compose.yml down
 docker-compose -f cassandra_output/docker-compose.yml down
 docker-compose -f consumers/docker-compose.yml down
 docker-compose -f flink/docker-compose.yml down
 docker network rm kafka-network
 docker network rm cassandra-network
