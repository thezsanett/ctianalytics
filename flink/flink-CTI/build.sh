docker run -it --rm --name flink-sketches-project -v "$(pwd)":/usr/src/ -w /usr/src/ maven:3.6.0-jdk-11-slim mvn clean install
docker cp target/FlinkScketches.jar flink_jobmanager:/FlinkScketches.jar
docker exec -it flink_jobmanager flink run /FlinkScketches.jar
